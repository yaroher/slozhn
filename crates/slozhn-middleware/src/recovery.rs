//! Panic recovery: a handler bug must not tear down the whole connection
//! driver. Without this layer, a panic inside the inner service unwinds
//! straight through tower/tonic and kills whatever task is driving the
//! request — on the WS bridge that's the connection's read/write loop, so
//! ONE bad message can drop every other in-flight RPC sharing that
//! connection.
//!
//! `RecoveryLayer` wraps the inner call in
//! `std::panic::AssertUnwindSafe(fut).catch_unwind()` and turns a caught
//! panic into a trailers-only `INTERNAL` (code 13) response instead of
//! letting it propagate. `AssertUnwindSafe` is required because the futures
//! this wraps are generally not provably unwind-safe (they hold `&mut`
//! references across await points); the tradeoff is standard for
//! panic-recovery middleware (compare `tower::catch_panic` /
//! `hyper`'s `service_fn` panic guards) — the recovered state is discarded
//! (this RPC fails), not reused, so unwind-safety violations don't leak
//! partially-mutated state back into a live call.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::FutureExt as _;
use tonic::body::Body as TonicBody;

/// See module docs.
#[derive(Clone, Copy, Debug, Default)]
pub struct RecoveryLayer;

impl RecoveryLayer {
    pub fn new() -> Self {
        Self
    }
}

impl<S> tower::Layer<S> for RecoveryLayer {
    type Service = RecoveryService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RecoveryService { inner }
    }
}

#[derive(Clone)]
pub struct RecoveryService<S> {
    inner: S,
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_owned()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic payload".to_owned()
    }
}

fn internal_error_response() -> http::Response<TonicBody> {
    let mut resp = http::Response::new(TonicBody::default());
    resp.headers_mut().insert(
        "content-type",
        http::header::HeaderValue::from_static("application/grpc"),
    );
    let status = tonic::Status::new(tonic::Code::Internal, "internal error");
    let _ = status.add_header(resp.headers_mut());
    resp
}

impl<S> tower::Service<http::Request<TonicBody>> for RecoveryService<S>
where
    S: tower::Service<http::Request<TonicBody>, Response = http::Response<TonicBody>>,
    S::Future: Send + 'static,
{
    type Response = http::Response<TonicBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<TonicBody>) -> Self::Future {
        let fut = self.inner.call(req);
        Box::pin(async move {
            match AssertUnwindSafe(fut).catch_unwind().await {
                Ok(result) => result,
                Err(payload) => {
                    let message = panic_message(&*payload);
                    tracing::error!(panic = %message, "panic recovered by RecoveryLayer");
                    metrics::counter!("slozhn_panics_recovered_total").increment(1);
                    Ok(internal_error_response())
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::{Layer, Service, ServiceExt};

    fn req() -> http::Request<TonicBody> {
        http::Request::builder().uri("/t.S/M").body(TonicBody::default()).unwrap()
    }

    #[tokio::test]
    async fn panicking_handler_yields_internal_status() {
        let panicking = tower::service_fn(|_req: http::Request<TonicBody>| async move {
            panic!("boom");
            #[allow(unreachable_code)]
            Ok::<_, std::convert::Infallible>(http::Response::new(TonicBody::default()))
        });
        let mut svc = RecoveryLayer::new().layer(panicking);

        let resp = svc.ready().await.unwrap().call(req()).await.unwrap();
        assert_eq!(resp.headers().get("grpc-status").unwrap(), "13");
    }

    #[tokio::test]
    async fn normal_call_passes_through_unchanged() {
        let ok = tower::service_fn(|_req: http::Request<TonicBody>| async move {
            let mut resp = http::Response::new(TonicBody::default());
            resp.headers_mut().insert("x-ok", "1".parse().unwrap());
            Ok::<_, std::convert::Infallible>(resp)
        });
        let mut svc = RecoveryLayer::new().layer(ok);

        let resp = svc.ready().await.unwrap().call(req()).await.unwrap();
        assert!(!resp.headers().contains_key("grpc-status"));
        assert_eq!(resp.headers().get("x-ok").unwrap(), "1");
    }

    #[tokio::test]
    async fn panic_does_not_escape_the_task() {
        // If the panic weren't caught, this whole test task would abort
        // instead of returning a value — the assertion above already proves
        // it, but make the intent explicit via a second panicking call in
        // sequence on the same service instance.
        let panicking = tower::service_fn(|_req: http::Request<TonicBody>| async move {
            panic!("boom again");
            #[allow(unreachable_code)]
            Ok::<_, std::convert::Infallible>(http::Response::new(TonicBody::default()))
        });
        let mut svc = RecoveryLayer::new().layer(panicking);

        for _ in 0..2 {
            let resp = svc.ready().await.unwrap().call(req()).await.unwrap();
            assert_eq!(resp.headers().get("grpc-status").unwrap(), "13");
        }
    }
}
