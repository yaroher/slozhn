//! Client-side per-call deadline, wasm-safe (built on `futures_timer::Delay`,
//! same as the retry backoff timer).
//!
//! `TimeoutLayer::call` does two things:
//! 1. If the outgoing request has no `grpc-timeout` header yet, one is set
//!    in gRPC wire format (`"{millis}m"`) so the SERVER also enforces the
//!    deadline, not just this client. A caller-supplied `grpc-timeout` is
//!    never overridden.
//! 2. The inner call races against a `Delay`. If the delay wins, the inner
//!    future is dropped — canceling the pending RPC (in the slozhn stack
//!    this tears down the receive half) — and `Err(TimeoutError::Elapsed)`
//!    is returned instead of a fabricated response.
//!
//! Why an error and not a synthetic trailers-only response: the service's
//! `Response` type is a generic `RB` here (this layer isn't pinned to
//! `tonic::body::Body` the way the server-side dedup layer is), so there is
//! no generic way to construct one. Returning `Service::Error =
//! TimeoutError<S::Error>` is simpler and type-clean; tonic's client stubs
//! map service errors through `Status::from_error`, which lands as code
//! `Unknown` with the message preserved.
//!
//! Ordering: place `TimeoutLayer` ABOVE `RetryLayer` (i.e. apply it to the
//! already-retry-wrapped service) if you want a deadline to cover all
//! attempts. Timeouts themselves are never retried by this layer.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::future::Either;

/// See module docs.
#[derive(Clone, Copy, Debug)]
pub struct TimeoutLayer {
    pub timeout: Duration,
}

impl TimeoutLayer {
    pub fn new(timeout: Duration) -> Self {
        Self { timeout }
    }
}

impl<S> tower::Layer<S> for TimeoutLayer {
    type Service = TimeoutService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TimeoutService { inner, timeout: self.timeout }
    }
}

#[derive(Clone)]
pub struct TimeoutService<S> {
    inner: S,
    timeout: Duration,
}

/// Error produced by [`TimeoutService`]: either the deadline elapsed before
/// the inner service responded, or the inner service itself failed.
#[derive(Debug)]
pub enum TimeoutError<E> {
    Elapsed(Duration),
    Inner(E),
}

impl<E: fmt::Display> fmt::Display for TimeoutError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TimeoutError::Elapsed(d) => write!(f, "deadline exceeded after {d:?}"),
            TimeoutError::Inner(e) => write!(f, "{e}"),
        }
    }
}

impl<E: fmt::Debug + fmt::Display> std::error::Error for TimeoutError<E> {}

const GRPC_TIMEOUT_HEADER: &str = "grpc-timeout";

fn grpc_timeout_header(timeout: Duration) -> Option<http::HeaderValue> {
    // gRPC wire format: ASCII digits (<=8) + a unit char. `m` = milliseconds.
    let millis = timeout.as_millis().min(99_999_999);
    http::HeaderValue::from_str(&format!("{millis}m")).ok()
}

impl<S, B, RB> tower::Service<http::Request<B>> for TimeoutService<S>
where
    S: tower::Service<http::Request<B>, Response = http::Response<RB>>,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = S::Response;
    type Error = TimeoutError<S::Error>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(TimeoutError::Inner)
    }

    fn call(&mut self, mut req: http::Request<B>) -> Self::Future {
        if !req.headers().contains_key(GRPC_TIMEOUT_HEADER)
            && let Some(v) = grpc_timeout_header(self.timeout)
        {
            req.headers_mut().insert(GRPC_TIMEOUT_HEADER, v);
        }

        let timeout = self.timeout;
        let fut = self.inner.call(req);

        Box::pin(async move {
            futures::pin_mut!(fut);
            let delay = futures_timer::Delay::new(timeout);
            futures::pin_mut!(delay);

            match futures::future::select(fut, delay).await {
                Either::Left((res, _delay)) => res.map_err(TimeoutError::Inner),
                // Dropping the pending inner future here cancels the RPC.
                Either::Right((_, _fut)) => Err(TimeoutError::Elapsed(timeout)),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tower::{Layer, Service, ServiceExt};

    fn ok_probe(
        seen: Arc<Mutex<Option<http::HeaderMap>>>,
    ) -> impl tower::Service<
        http::Request<()>,
        Response = http::Response<()>,
        Error = std::convert::Infallible,
        Future = impl Send,
    > + Clone
    + Send {
        tower::service_fn(move |req: http::Request<()>| {
            let seen = seen.clone();
            async move {
                *seen.lock().unwrap() = Some(req.headers().clone());
                Ok(http::Response::new(()))
            }
        })
    }

    fn req() -> http::Request<()> {
        http::Request::builder().uri("/t.S/M").body(()).unwrap()
    }

    #[tokio::test]
    async fn fast_inner_passes_through_and_injects_grpc_timeout() {
        let seen = Arc::new(Mutex::new(None));
        let mut svc =
            TimeoutLayer::new(Duration::from_secs(5)).layer(ok_probe(seen.clone()));

        svc.ready().await.unwrap().call(req()).await.unwrap();

        let headers = seen.lock().unwrap().clone().expect("probe ran");
        let value = headers
            .get(GRPC_TIMEOUT_HEADER)
            .expect("grpc-timeout injected")
            .to_str()
            .unwrap()
            .to_owned();
        assert!(value.ends_with('m'));
        assert_eq!(value.trim_end_matches('m').parse::<u64>().unwrap(), 5000);
    }

    #[tokio::test]
    async fn explicit_grpc_timeout_is_not_overridden() {
        let seen = Arc::new(Mutex::new(None));
        let mut svc =
            TimeoutLayer::new(Duration::from_secs(5)).layer(ok_probe(seen.clone()));

        let mut r = req();
        r.headers_mut()
            .insert(GRPC_TIMEOUT_HEADER, "42m".parse().unwrap());
        svc.ready().await.unwrap().call(r).await.unwrap();

        let headers = seen.lock().unwrap().clone().expect("probe ran");
        assert_eq!(
            headers.get(GRPC_TIMEOUT_HEADER).unwrap().to_str().unwrap(),
            "42m"
        );
    }

    #[tokio::test]
    async fn slow_inner_times_out() {
        let pending = tower::service_fn(|_req: http::Request<()>| async move {
            futures::future::pending::<Result<http::Response<()>, std::convert::Infallible>>()
                .await
        });
        let mut svc = TimeoutLayer::new(Duration::from_millis(50)).layer(pending);

        let start = std::time::Instant::now();
        let err = svc.ready().await.unwrap().call(req()).await.unwrap_err();
        let elapsed = start.elapsed();

        assert!(matches!(err, TimeoutError::Elapsed(_)));
        assert!(elapsed < Duration::from_millis(200), "elapsed={elapsed:?}");
    }
}
