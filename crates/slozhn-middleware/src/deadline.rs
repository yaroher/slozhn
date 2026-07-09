//! Server-side deadline enforcement, wasm-safe on the client side but only
//! meaningful (and only compiled) on the server: [`super::TimeoutLayer`] sets
//! the `grpc-timeout` header for outgoing calls, and native tonic transport
//! enforces it on the receiving end — our WS bridge does not, so a handler
//! keeps running after the client has already given up. `DeadlineLayer`
//! closes that gap: it parses `grpc-timeout` off the request, races the
//! inner call against a `futures_timer::Delay` (same primitive as
//! [`super::TimeoutLayer`] and the retry backoff timer), and, if headers
//! arrive in time, keeps enforcing the SAME deadline over the response body
//! so a slow/stalled stream is also cut off.
//!
//! Wrap it around `tonic::service::Routes` (or the traced/auth-wrapped
//! routes), same as [`super::AuthLayer`]:
//! ```ignore
//! let routes = tonic::service::Routes::new(EchoServer::new(MyEcho));
//! let deadlined = DeadlineLayer::new().max(Duration::from_secs(30)).layer(routes);
//! ```
//!
//! Header absent or malformed per the gRPC wire spec → passthrough,
//! untouched (native tonic behavior for the same case). `DeadlineLayer::max`
//! clamps whatever the client asked for, so a server can bound worst-case
//! handler runtime independent of what callers request.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures::future::Either;
use http_body::Frame;
use pin_project_lite::pin_project;
use tonic::body::Body as TonicBody;

const GRPC_TIMEOUT_HEADER: &str = "grpc-timeout";

/// Parse a `grpc-timeout` header value per the gRPC wire spec: 1-8 ASCII
/// digits followed by exactly one unit char (`H`/`M`/`S`/`m`/`u`/`n`).
/// Anything else (empty, too many digits, unknown unit, non-digit body,
/// overflow) is treated as absent — the caller passes the request through
/// untouched rather than guessing at an invalid deadline.
fn parse_grpc_timeout(value: &str) -> Option<Duration> {
    if value.is_empty() || value.len() > 9 {
        return None;
    }
    let (digits, unit) = value.split_at(value.len() - 1);
    if digits.is_empty() || digits.len() > 8 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let n: u64 = digits.parse().ok()?;
    match unit.as_bytes()[0] {
        b'H' => n.checked_mul(3_600).map(Duration::from_secs),
        b'M' => n.checked_mul(60).map(Duration::from_secs),
        b'S' => Some(Duration::from_secs(n)),
        b'm' => Some(Duration::from_millis(n)),
        b'u' => Some(Duration::from_micros(n)),
        b'n' => Some(Duration::from_nanos(n)),
        _ => None,
    }
}

/// See module docs.
#[derive(Clone, Copy, Debug, Default)]
pub struct DeadlineLayer {
    max: Option<Duration>,
}

impl DeadlineLayer {
    /// No server-side cap: the effective deadline is whatever the client
    /// sent in `grpc-timeout` (or none, if the header is absent/invalid).
    pub fn new() -> Self {
        Self { max: None }
    }

    /// Clamp the effective deadline to at most `max`, regardless of what the
    /// client requested.
    pub fn max(mut self, max: Duration) -> Self {
        self.max = Some(max);
        self
    }
}

impl<S> tower::Layer<S> for DeadlineLayer {
    type Service = DeadlineService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        DeadlineService { inner, max: self.max }
    }
}

#[derive(Clone)]
pub struct DeadlineService<S> {
    inner: S,
    max: Option<Duration>,
}

fn deadline_exceeded_response() -> http::Response<TonicBody> {
    let mut resp = http::Response::new(TonicBody::default());
    resp.headers_mut().insert(
        "content-type",
        http::header::HeaderValue::from_static("application/grpc"),
    );
    let status = tonic::Status::new(tonic::Code::DeadlineExceeded, "deadline exceeded");
    let _ = status.add_header(resp.headers_mut());
    resp
}

fn deadline_exceeded_trailers() -> http::HeaderMap {
    let mut trailers = http::HeaderMap::new();
    let status = tonic::Status::new(tonic::Code::DeadlineExceeded, "deadline exceeded");
    let _ = status.add_header(&mut trailers);
    trailers
}

impl<S> tower::Service<http::Request<TonicBody>> for DeadlineService<S>
where
    S: tower::Service<http::Request<TonicBody>, Response = http::Response<TonicBody>>,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = http::Response<TonicBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<TonicBody>) -> Self::Future {
        let header_timeout = req
            .headers()
            .get(GRPC_TIMEOUT_HEADER)
            .and_then(|v| v.to_str().ok())
            .and_then(parse_grpc_timeout);

        let Some(mut timeout) = header_timeout else {
            // No (valid) deadline requested — nothing for this layer to do.
            return Box::pin(self.inner.call(req));
        };
        if let Some(max) = self.max {
            timeout = timeout.min(max);
        }

        let fut = self.inner.call(req);

        Box::pin(async move {
            let deadline = Instant::now() + timeout;
            futures::pin_mut!(fut);
            let delay = futures_timer::Delay::new(timeout);
            futures::pin_mut!(delay);

            match futures::future::select(fut, delay).await {
                Either::Left((res, _delay)) => {
                    let resp = res?;
                    let (parts, body) = resp.into_parts();
                    if parts.headers.contains_key("grpc-status") {
                        // Trailers-only: the call already finished, nothing
                        // to enforce over a body that doesn't exist.
                        return Ok(http::Response::from_parts(parts, body));
                    }
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    let wrapped = TonicBody::new(DeadlineBody {
                        inner: body,
                        delay: futures_timer::Delay::new(remaining),
                        done: false,
                    });
                    Ok(http::Response::from_parts(parts, wrapped))
                }
                // Dropping the pending inner future here cancels the handler.
                Either::Right((_, _fut)) => {
                    tracing::debug!(?timeout, "deadline exceeded before headers");
                    metrics::counter!("slozhn_deadline_exceeded_total", "stage" => "headers")
                        .increment(1);
                    Ok(deadline_exceeded_response())
                }
            }
        })
    }
}

pin_project! {
    /// Wraps a response body so the SAME deadline that gated headers keeps
    /// being enforced over the stream: on expiry mid-stream, polling of the
    /// inner body stops and one final trailers frame with grpc-status 4 is
    /// emitted, then the body ends.
    struct DeadlineBody {
        #[pin]
        inner: TonicBody,
        #[pin]
        delay: futures_timer::Delay,
        done: bool,
    }
}

impl http_body::Body for DeadlineBody {
    type Data = bytes::Bytes;
    type Error = tonic::Status;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let mut this = self.project();
        if *this.done {
            return Poll::Ready(None);
        }

        if this.delay.as_mut().poll(cx).is_ready() {
            *this.done = true;
            tracing::debug!("deadline exceeded mid-stream");
            metrics::counter!("slozhn_deadline_exceeded_total", "stage" => "stream")
                .increment(1);
            return Poll::Ready(Some(Ok(Frame::trailers(deadline_exceeded_trailers()))));
        }

        let out = std::task::ready!(this.inner.as_mut().poll_frame(cx));
        if let Some(Ok(frame)) = &out
            && frame.trailers_ref().is_some()
        {
            *this.done = true;
        }
        if out.is_none() {
            *this.done = true;
        }
        Poll::Ready(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http_body_util::BodyExt as _;
    use tower::{Layer, Service, ServiceExt};

    fn req(timeout: Option<&str>) -> http::Request<TonicBody> {
        let mut builder = http::Request::builder().uri("/t.S/M");
        if let Some(timeout) = timeout {
            builder = builder.header(GRPC_TIMEOUT_HEADER, timeout);
        }
        builder.body(TonicBody::default()).unwrap()
    }

    fn ok_response() -> http::Response<TonicBody> {
        http::Response::new(TonicBody::default())
    }

    async fn grpc_status(resp: &http::Response<TonicBody>) -> Option<u32> {
        resp.headers().get("grpc-status")?.to_str().ok()?.parse().ok()
    }

    #[tokio::test]
    async fn slow_unary_handler_gets_deadline_exceeded_trailers_only() {
        let pending = tower::service_fn(|_req: http::Request<TonicBody>| async move {
            futures::future::pending::<Result<http::Response<TonicBody>, std::convert::Infallible>>()
                .await
        });
        let mut svc = DeadlineLayer::new().layer(pending);

        let resp = svc
            .ready()
            .await
            .unwrap()
            .call(req(Some("50m")))
            .await
            .unwrap();

        assert_eq!(grpc_status(&resp).await, Some(4));
        // Trailers-only: no separate trailers frame needed, body is empty.
        let collected = resp.into_body().collect().await.unwrap();
        assert!(collected.to_bytes().is_empty());
    }

    #[tokio::test]
    async fn stalled_stream_gets_data_then_deadline_trailers() {
        // Emits one data frame then hangs forever.
        struct StallAfterOne {
            sent: bool,
        }
        impl http_body::Body for StallAfterOne {
            type Data = Bytes;
            type Error = tonic::Status;
            fn poll_frame(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
                let this = self.get_mut();
                if !this.sent {
                    this.sent = true;
                    return Poll::Ready(Some(Ok(Frame::data(Bytes::from_static(b"hi")))));
                }
                // Register interest so we don't busy-loop; never resolves.
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }

        let svc = tower::service_fn(|_req: http::Request<TonicBody>| async move {
            Ok::<_, std::convert::Infallible>(http::Response::new(TonicBody::new(
                StallAfterOne { sent: false },
            )))
        });
        let mut svc = DeadlineLayer::new().layer(svc);

        let resp = svc
            .ready()
            .await
            .unwrap()
            .call(req(Some("50m")))
            .await
            .unwrap();
        assert!(!resp.headers().contains_key("grpc-status"), "headers arrived before deadline");

        let mut body = resp.into_body();
        let first = body.frame().await.unwrap().unwrap();
        assert_eq!(first.into_data().ok().unwrap(), Bytes::from_static(b"hi"));

        let second = body.frame().await.unwrap().unwrap();
        let trailers = second.into_trailers().ok().unwrap();
        assert_eq!(trailers.get("grpc-status").unwrap(), "4");

        assert!(body.frame().await.is_none(), "body ends after the trailers frame");
    }

    #[tokio::test]
    async fn no_header_passes_through() {
        let mut svc = DeadlineLayer::new().layer(tower::service_fn(
            |_req: http::Request<TonicBody>| async move {
                Ok::<_, std::convert::Infallible>(ok_response())
            },
        ));

        let resp = svc.ready().await.unwrap().call(req(None)).await.unwrap();
        assert!(!resp.headers().contains_key("grpc-status"));
    }

    #[test]
    fn header_parsing() {
        assert_eq!(parse_grpc_timeout("5S"), Some(Duration::from_secs(5)));
        assert_eq!(parse_grpc_timeout("100m"), Some(Duration::from_millis(100)));
        assert_eq!(parse_grpc_timeout("1M"), Some(Duration::from_secs(60)));
        assert_eq!(parse_grpc_timeout("50u"), Some(Duration::from_micros(50)));
        assert_eq!(parse_grpc_timeout("2H"), Some(Duration::from_secs(7_200)));
        assert_eq!(parse_grpc_timeout("7n"), Some(Duration::from_nanos(7)));
        assert_eq!(parse_grpc_timeout("abcm"), None, "non-digit body");
        assert_eq!(parse_grpc_timeout("999999999S"), None, "more than 8 digits");
        assert_eq!(parse_grpc_timeout(""), None, "empty");
        assert_eq!(parse_grpc_timeout("5X"), None, "unknown unit");
    }

    #[tokio::test]
    async fn max_clamps_effective_deadline() {
        let pending = tower::service_fn(|_req: http::Request<TonicBody>| async move {
            futures::future::pending::<Result<http::Response<TonicBody>, std::convert::Infallible>>()
                .await
        });
        // Client asks for 5s, server caps at 30ms — the cap should win.
        let mut svc = DeadlineLayer::new().max(Duration::from_millis(30)).layer(pending);

        let start = Instant::now();
        let resp = svc
            .ready()
            .await
            .unwrap()
            .call(req(Some("5S")))
            .await
            .unwrap();
        let elapsed = start.elapsed();

        assert_eq!(grpc_status(&resp).await, Some(4));
        assert!(elapsed < Duration::from_millis(500), "elapsed={elapsed:?}");
    }
}
