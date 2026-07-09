//! Forwarding tower service for the gateway pattern: bridges incoming
//! slozhn gRPC-over-WS calls to a plain HTTP/2 upstream (typically a
//! `tonic::transport::Channel` pointed at a regular tonic server). Use this
//! to split a thin, rarely-redeployed gateway tier (WS + sessions) from an
//! app tier that redeploys freely:
//!
//! ```ignore
//! // requires tonic's `channel` feature on the upstream side
//! let upstream = tonic::transport::Channel::from_static("http://127.0.0.1:50051")
//!     .connect_lazy();
//! let proxy = slozhn_server::grpc_proxy(upstream);
//! let _app = axum::Router::new().route("/rpc", slozhn_server::ws::grpc_ws(proxy));
//! ```

use std::task::{Context, Poll};

use bytes::Bytes;
use tonic::body::Body as TonicBody;

/// Wrap `upstream` in a [`GrpcProxy`] that forwards every call to it over
/// regular HTTP/2. `tonic::transport::Channel` works out of the box.
pub fn grpc_proxy<S>(upstream: S) -> GrpcProxy<S> {
    GrpcProxy { inner: upstream }
}

/// Forwards gRPC calls to an upstream tower service, mapping any upstream
/// failure (connect/poll_ready or call) into a trailers-only `UNAVAILABLE`
/// (14) gRPC response instead of a service error.
///
/// This is required to satisfy `grpc_ws`'s bound of `Error = Infallible`: a
/// business-tier redeploy must surface to the caller as a normal gRPC error
/// on the in-flight call, not tear down the WS connection or the service
/// itself.
#[derive(Clone)]
pub struct GrpcProxy<S> {
    inner: S,
}

impl<S, RB> tower::Service<http::Request<TonicBody>> for GrpcProxy<S>
where
    S: tower::Service<http::Request<TonicBody>, Response = http::Response<RB>>
        + Clone
        + Send
        + 'static,
    S::Future: Send,
    S::Error: std::fmt::Display,
    RB: http_body::Body<Data = Bytes> + Send + 'static,
    RB::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    type Response = http::Response<TonicBody>;
    type Error = std::convert::Infallible;
    type Future =
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Always ready: readiness of a reconnecting upstream (e.g. a
        // `connect_lazy` channel between app-tier deploys) is handled
        // per-call below instead. Reporting not-ready here would wedge the
        // whole WS dispatch loop on a transient upstream outage; reporting
        // an error here is impossible (`Error = Infallible`). Each call
        // polls the upstream itself and maps failure into a gRPC response.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<TonicBody>) -> Self::Future {
        let method = req.uri().path().to_string();
        // keep the service that was actually used for this call; hand the
        // fresh clone to self so any reserved readiness isn't leaked
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        Box::pin(async move {
            if let Err(e) = futures::future::poll_fn(|cx| inner.poll_ready(cx)).await {
                let msg = format!("upstream not ready: {e}");
                drop(e); // S::Error may be !Send — don't carry it across an await
                return Ok(upstream_unavailable(&method, msg));
            }
            match inner.call(req).await {
                Ok(resp) => {
                    let (parts, body) = resp.into_parts();
                    Ok(http::Response::from_parts(parts, TonicBody::new(body)))
                }
                Err(e) => {
                    let msg = format!("upstream call failed: {e}");
                    drop(e);
                    Ok(upstream_unavailable(&method, msg))
                }
            }
        })
    }
}

fn upstream_unavailable(method: &str, msg: String) -> http::Response<TonicBody> {
    tracing::warn!(method = %method, error = %msg, "grpc proxy upstream failed");
    metrics::counter!("slozhn_proxy_upstream_errors_total").increment(1);

    let mut resp = http::Response::new(TonicBody::default());
    resp.headers_mut().insert(
        "content-type",
        http::header::HeaderValue::from_static("application/grpc"),
    );
    // tonic percent-encodes grpc-message per the gRPC spec (see
    // slozhn-middleware::auth::rejection for the same pattern).
    let status = tonic::Status::new(tonic::Code::Unavailable, msg);
    let _ = status.add_header(resp.headers_mut());
    resp
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use tower::{Service, ServiceExt};

    fn req() -> http::Request<TonicBody> {
        http::Request::builder()
            .uri("/t.S/M")
            .body(TonicBody::default())
            .unwrap()
    }

    #[tokio::test]
    async fn success_passthrough_preserves_status_headers_body() {
        let upstream = tower::service_fn(|_req: http::Request<TonicBody>| async move {
            let mut resp = http::Response::new(TonicBody::new(http_body_util::Full::new(
                Bytes::from_static(b"payload"),
            )));
            *resp.status_mut() = http::StatusCode::OK;
            resp.headers_mut()
                .insert("x-upstream", "yes".parse().unwrap());
            Ok::<_, std::convert::Infallible>(resp)
        });

        let mut proxy = grpc_proxy(upstream);
        let resp = proxy.ready().await.unwrap().call(req()).await.unwrap();

        assert_eq!(resp.status(), http::StatusCode::OK);
        assert_eq!(resp.headers().get("x-upstream").unwrap(), "yes");
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"payload");
    }

    #[derive(Debug)]
    struct BoomError;
    impl std::fmt::Display for BoomError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "boom")
        }
    }

    #[tokio::test]
    async fn upstream_error_becomes_unavailable() {
        let upstream = tower::service_fn(|_req: http::Request<TonicBody>| async move {
            Err::<http::Response<TonicBody>, _>(BoomError)
        });

        let mut proxy = grpc_proxy(upstream);
        let resp = proxy.ready().await.unwrap().call(req()).await.unwrap();

        assert_eq!(resp.headers().get("grpc-status").unwrap(), "14");
        assert!(
            resp.headers()
                .get("grpc-message")
                .unwrap()
                .to_str()
                .unwrap()
                .contains("boom")
        );
    }

    /// Proves the response body is reboxed into `tonic::body::Body` even
    /// when the upstream returns a different body type.
    #[tokio::test]
    async fn works_with_non_tonic_response_body_type() {
        let upstream = tower::service_fn(|_req: http::Request<TonicBody>| async move {
            // http_body_util::Full is not tonic::body::Body.
            let resp = http::Response::new(http_body_util::Full::new(Bytes::from_static(
                b"reboxed",
            )));
            Ok::<_, std::convert::Infallible>(resp)
        });

        let mut proxy = grpc_proxy(upstream);
        let resp = proxy.ready().await.unwrap().call(req()).await.unwrap();

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"reboxed");
    }
}
