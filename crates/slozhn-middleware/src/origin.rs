//! Origin allowlisting for a browser-first WebSocket transport (CSWSH
//! defense: Cross-Site WebSocket Hijacking).
//!
//! Unlike `fetch`/XHR, browsers do NOT enforce the same-origin policy for
//! WebSocket connections and do NOT run CORS preflight on the upgrade
//! request — any page on the web can open a WS connection to this server
//! and have the browser attach ambient credentials (cookies) to it. If the
//! server accepts session cookies for auth, that's a hijack primitive
//! unless the SERVER checks the `Origin` header itself. This layer does
//! that check, so it belongs directly above `grpc_ws`/`grpc_ws_session` —
//! before any request-specific logic (auth, validation, ...) runs.
//!
//! `slozhn` carries the gRPC method inside protocol frames rather than the
//! HTTP path, so `OriginLayer` operates purely on the upgrade request's
//! `Origin` header and rejects the whole connection, not a single RPC —
//! there is no per-method exemption.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use tonic::body::Body as TonicBody;

/// See module docs. Exact-match allowlist by default; `.allow_any()` is an
/// explicit, documented escape hatch for non-browser deployments where
/// `Origin` carries no security meaning.
#[derive(Clone)]
pub struct OriginLayer {
    allowed: std::sync::Arc<Vec<String>>,
    allow_any: bool,
    allow_missing: bool,
}

impl OriginLayer {
    /// Exact-match allowlist, e.g. `["https://app.example.com"]`. Requests
    /// without an `Origin` header are rejected by default — see
    /// [`Self::allow_missing_origin`] to change that for non-browser
    /// callers that never send one.
    pub fn new<I, S>(allowed: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            allowed: std::sync::Arc::new(allowed.into_iter().map(Into::into).collect()),
            allow_any: false,
            allow_missing: false,
        }
    }

    /// Disable the origin check entirely. INSECURE for a browser-facing
    /// deployment — only use this when the server is never reachable from a
    /// browser (e.g. purely native-client-to-server over a private network).
    pub fn allow_any() -> Self {
        Self { allowed: std::sync::Arc::new(Vec::new()), allow_any: true, allow_missing: true }
    }

    /// Accept requests with no `Origin` header at all (native clients don't
    /// send one; browsers always do for cross-origin and same-origin
    /// requests alike). Default: rejected.
    pub fn allow_missing_origin(mut self) -> Self {
        self.allow_missing = true;
        self
    }

    fn check(&self, headers: &http::HeaderMap) -> Result<(), ()> {
        if self.allow_any {
            return Ok(());
        }
        match headers.get(http::header::ORIGIN).and_then(|v| v.to_str().ok()) {
            Some(origin) if self.allowed.iter().any(|a| a == origin) => Ok(()),
            Some(_) => Err(()),
            None if self.allow_missing => Ok(()),
            None => Err(()),
        }
    }
}

impl<S> tower::Layer<S> for OriginLayer {
    type Service = OriginService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        OriginService { inner, layer: self.clone() }
    }
}

#[derive(Clone)]
pub struct OriginService<S> {
    inner: S,
    layer: OriginLayer,
}

/// Trailers-only `PERMISSION_DENIED` (code 7) response — mirrors
/// `auth::rejection`: percent-encoding of `grpc-message` is handled by
/// `tonic::Status::add_header`, never hand-rolled.
fn rejection(message: &str) -> http::Response<TonicBody> {
    let mut resp = http::Response::new(TonicBody::default());
    resp.headers_mut().insert(
        "content-type",
        http::header::HeaderValue::from_static("application/grpc"),
    );
    let status = tonic::Status::new(tonic::Code::PermissionDenied, message);
    let _ = status.add_header(resp.headers_mut());
    resp
}

impl<S> tower::Service<http::Request<TonicBody>> for OriginService<S>
where
    S: tower::Service<http::Request<TonicBody>, Response = http::Response<TonicBody>>
        + Send
        + 'static,
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
        if self.layer.check(req.headers()).is_err() {
            tracing::warn!(
                origin = ?req.headers().get(http::header::ORIGIN),
                "rejected by origin check",
            );
            metrics::counter!("slozhn_origin_rejected_total").increment(1);
            return Box::pin(std::future::ready(Ok(rejection("origin not allowed"))));
        }
        let fut = self.inner.call(req);
        Box::pin(fut)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::{Layer, Service, ServiceExt};

    fn ok_probe() -> impl tower::Service<
        http::Request<TonicBody>,
        Response = http::Response<TonicBody>,
        Error = std::convert::Infallible,
        Future = impl Send,
    > + Clone
    + Send {
        tower::service_fn(|_req: http::Request<TonicBody>| async move {
            Ok(http::Response::new(TonicBody::default()))
        })
    }

    fn req_with_origin(origin: Option<&str>) -> http::Request<TonicBody> {
        let mut b = http::Request::builder().uri("/t.S/M");
        if let Some(o) = origin {
            b = b.header(http::header::ORIGIN, o);
        }
        b.body(TonicBody::default()).unwrap()
    }

    #[tokio::test]
    async fn allowed_origin_passes() {
        let mut svc = OriginLayer::new(["https://app.example.com"]).layer(ok_probe());
        let resp = svc
            .ready()
            .await
            .unwrap()
            .call(req_with_origin(Some("https://app.example.com")))
            .await
            .unwrap();
        assert!(!resp.headers().contains_key("grpc-status"));
    }

    #[tokio::test]
    async fn disallowed_origin_is_permission_denied() {
        let mut svc = OriginLayer::new(["https://app.example.com"]).layer(ok_probe());
        let resp = svc
            .ready()
            .await
            .unwrap()
            .call(req_with_origin(Some("https://evil.example.com")))
            .await
            .unwrap();
        assert_eq!(resp.headers().get("grpc-status").unwrap(), "7");
    }

    #[tokio::test]
    async fn missing_origin_is_rejected_by_default() {
        let mut svc = OriginLayer::new(["https://app.example.com"]).layer(ok_probe());
        let resp = svc.ready().await.unwrap().call(req_with_origin(None)).await.unwrap();
        assert_eq!(resp.headers().get("grpc-status").unwrap(), "7");
    }

    #[tokio::test]
    async fn missing_origin_allowed_when_configured() {
        let mut svc = OriginLayer::new(["https://app.example.com"])
            .allow_missing_origin()
            .layer(ok_probe());
        let resp = svc.ready().await.unwrap().call(req_with_origin(None)).await.unwrap();
        assert!(!resp.headers().contains_key("grpc-status"));
    }

    #[tokio::test]
    async fn allow_any_accepts_anything() {
        let mut svc = OriginLayer::allow_any().layer(ok_probe());
        let resp = svc
            .ready()
            .await
            .unwrap()
            .call(req_with_origin(Some("https://anything.example")))
            .await
            .unwrap();
        assert!(!resp.headers().contains_key("grpc-status"));

        let resp = svc.ready().await.unwrap().call(req_with_origin(None)).await.unwrap();
        assert!(!resp.headers().contains_key("grpc-status"));
    }
}
