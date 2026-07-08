//! Client-side retries for unary calls.
//!
//! The request body is buffered up to `buffer_limit`; if it fits, the call is
//! retried on connection-level `UNAVAILABLE` (grpc-status 14 arriving as a
//! trailers-only response — the slozhn connection-lost path) or a transport
//! error, with jittered exponential backoff. Bodies over the limit (streaming
//! requests) pass through with a single attempt — a partially consumed stream
//! must never be silently replayed.
//!
//! NOTE: a retried RPC may execute twice on the server if the response (not
//! the request) was lost — enable this layer for idempotent methods.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::BodyExt;
use tonic::body::Body as TonicBody;

/// See module docs. `Default`: 3 attempts, 256 KiB buffer, 50 ms base backoff.
#[derive(Clone, Debug)]
pub struct RetryLayer {
    pub max_attempts: u32,
    pub buffer_limit: usize,
    pub base_backoff: Duration,
}

impl Default for RetryLayer {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            buffer_limit: 256 * 1024,
            base_backoff: Duration::from_millis(50),
        }
    }
}

impl<S> tower::Layer<S> for RetryLayer {
    type Service = RetryService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RetryService { inner, config: self.clone() }
    }
}

#[derive(Clone)]
pub struct RetryService<S> {
    inner: S,
    config: RetryLayer,
}

fn is_unavailable<RB>(resp: &http::Response<RB>) -> bool {
    resp.headers()
        .get("grpc-status")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|s| s == "14")
}

fn jittered(base: Duration) -> Duration {
    let half = base / 2;
    half + Duration::from_millis(fastrand::u64(0..=half.as_millis() as u64))
}

impl<S, RB> tower::Service<http::Request<TonicBody>> for RetryService<S>
where
    S: tower::Service<http::Request<TonicBody>, Response = http::Response<RB>>
        + Clone
        + Send
        + 'static,
    S::Future: Send,
    S::Error: Send,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<TonicBody>) -> Self::Future {
        let mut inner = self.inner.clone();
        let config = self.config.clone();
        Box::pin(async move {
            let (parts, body) = req.into_parts();

            // Буферизуем тело до лимита. Не влезло — одна попытка,
            // префикс + остаток склеиваются обратно без копий остатка.
            let mut collected: Vec<Bytes> = Vec::new();
            let mut total = 0usize;
            let mut body = body;
            let overflow = loop {
                match body.frame().await {
                    Some(Ok(frame)) => match frame.into_data() {
                        Ok(data) => {
                            total += data.len();
                            collected.push(data);
                            if total > config.buffer_limit {
                                break true;
                            }
                        }
                        Err(_trailer_frame) => {} // клиент trailers не шлёт
                    },
                    Some(Err(_)) => break true, // ошибку тела отдаём вниз как есть
                    None => break false,
                }
            };

            if overflow {
                // невоспроизводимое тело: prefix + недочитанный остаток
                let prefix = http_body_util::Full::new(Bytes::from(
                    collected.into_iter().fold(Vec::new(), |mut acc, b| {
                        acc.extend_from_slice(&b);
                        acc
                    }),
                ));
                let chained = TonicBody::new(
                    prefix
                        .map_err(|e: std::convert::Infallible| match e {})
                        .boxed_unsync()
                        .chain_compat(body),
                );
                let req = http::Request::from_parts(parts, chained);
                return inner.call(req).await;
            }

            let buffered = Bytes::from(collected.into_iter().fold(
                Vec::with_capacity(total),
                |mut acc, b| {
                    acc.extend_from_slice(&b);
                    acc
                },
            ));

            let mut attempt = 0u32;
            loop {
                attempt += 1;
                let mut req = http::Request::builder()
                    .method(parts.method.clone())
                    .uri(parts.uri.clone())
                    .version(parts.version)
                    .body(TonicBody::new(
                        http_body_util::Full::new(buffered.clone())
                            .map_err(|e: std::convert::Infallible| match e {}),
                    ))
                    .expect("rebuilt request");
                *req.headers_mut() = parts.headers.clone();

                match inner.call(req).await {
                    Ok(resp) if is_unavailable(&resp) && attempt < config.max_attempts => {
                        tracing::info!(attempt, "retrying after UNAVAILABLE");
                    }
                    Err(_e) if attempt < config.max_attempts => {
                        tracing::info!(attempt, "retrying after transport error");
                    }
                    other => return other,
                }
                futures_timer::Delay::new(jittered(
                    config.base_backoff * 2u32.pow(attempt - 1),
                ))
                .await;
            }
        })
    }
}

/// `Body::chain` does not exist in http-body-util — a minimal two-part chain.
trait ChainCompat: Sized {
    fn chain_compat(self, rest: TonicBody) -> ChainBody<Self>;
}

impl<B> ChainCompat for B
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
{
    fn chain_compat(self, rest: TonicBody) -> ChainBody<Self> {
        ChainBody { first: Some(self), rest }
    }
}

pin_project_lite::pin_project! {
    pub struct ChainBody<B> {
        #[pin]
        first: Option<B>,
        #[pin]
        rest: TonicBody,
    }
}

impl<B> http_body::Body for ChainBody<B>
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    type Data = Bytes;
    type Error = tonic::Status;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        let mut this = self.project();
        if let Some(first) = this.first.as_mut().as_pin_mut() {
            match std::task::ready!(first.poll_frame(cx)) {
                Some(Ok(f)) => return Poll::Ready(Some(Ok(f))),
                Some(Err(e)) => {
                    return Poll::Ready(Some(Err(tonic::Status::internal(
                        e.into().to_string(),
                    ))))
                }
                None => this.first.set(None),
            }
        }
        this.rest.poll_frame(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use tower::{Layer, Service, ServiceExt};

    fn unary_req(payload: &'static [u8]) -> http::Request<TonicBody> {
        http::Request::builder()
            .uri("/t.S/M")
            .body(TonicBody::new(
                http_body_util::Full::new(Bytes::from_static(payload))
                    .map_err(|e: std::convert::Infallible| match e {}),
            ))
            .unwrap()
    }

    fn unavailable_then_ok(
        calls: Arc<AtomicU32>,
    ) -> impl tower::Service<
        http::Request<TonicBody>,
        Response = http::Response<String>,
        Error = std::convert::Infallible,
        Future = impl Send,
    > + Clone
           + Send {
        tower::service_fn(move |_req: http::Request<TonicBody>| {
            let calls = calls.clone();
            async move {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                let mut resp = http::Response::new(String::new());
                if n == 0 {
                    resp.headers_mut().insert("grpc-status", "14".parse().unwrap());
                }
                Ok(resp)
            }
        })
    }

    #[tokio::test]
    async fn retries_unavailable_then_succeeds() {
        let calls = Arc::new(AtomicU32::new(0));
        let mut svc = RetryLayer::default().layer(unavailable_then_ok(calls.clone()));
        let resp = svc.ready().await.unwrap().call(unary_req(b"hi")).await.unwrap();
        assert!(!resp.headers().contains_key("grpc-status"));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn gives_up_after_max_attempts() {
        let calls = Arc::new(AtomicU32::new(0));
        let always_unavailable = tower::service_fn({
            let calls = calls.clone();
            move |_req: http::Request<TonicBody>| {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let mut resp = http::Response::new(String::new());
                    resp.headers_mut().insert("grpc-status", "14".parse().unwrap());
                    Ok::<_, std::convert::Infallible>(resp)
                }
            }
        });
        let mut svc = RetryLayer::default().layer(always_unavailable);
        let resp = svc.ready().await.unwrap().call(unary_req(b"hi")).await.unwrap();
        assert!(resp.headers().contains_key("grpc-status")); // последняя неудача отдана как есть
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn oversized_body_passes_through_without_retry() {
        let calls = Arc::new(AtomicU32::new(0));
        let mut svc = RetryLayer {
            buffer_limit: 4, // всё, что больше 4 байт — «стрим»
            ..Default::default()
        }
        .layer(unavailable_then_ok(calls.clone()));
        let resp = svc
            .ready()
            .await
            .unwrap()
            .call(unary_req(b"way-too-big-body"))
            .await
            .unwrap();
        // первый ответ (UNAVAILABLE) отдан как есть, ретрая не было
        assert!(resp.headers().contains_key("grpc-status"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
