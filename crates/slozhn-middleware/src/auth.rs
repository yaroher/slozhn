//! Authentication middleware, modeled on go-grpc-middleware's auth
//! interceptor: a server-side async `AuthFn` runs before the service and
//! either rejects the call with a gRPC status (the service is never invoked)
//! or produces an identity that handlers read from request extensions.
//! Client side: [`AuthTokenLayer`] injects `authorization` metadata per call.
//!
//! slozhn carries per-RPC metadata inside protocol frames — NOT in WebSocket
//! upgrade headers — so this works from the browser too, where upgrade
//! headers are unavailable.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use tonic::body::Body as TonicBody;

/// Authentication outcome carried into handlers via request extensions:
/// `request.extensions().get::<Identity<MyUser>>()`.
#[derive(Clone, Debug)]
pub struct Identity<T>(pub T);

/// Rejection produced by an [`AuthFn`]; becomes a trailers-only gRPC error.
#[derive(Clone, Debug)]
pub struct AuthError {
    /// gRPC status code (default 16 UNAUTHENTICATED).
    pub code: u32,
    pub message: String,
}

impl AuthError {
    pub fn unauthenticated(message: impl Into<String>) -> Self {
        Self { code: 16, message: message.into() }
    }

    pub fn permission_denied(message: impl Into<String>) -> Self {
        Self { code: 7, message: message.into() }
    }
}

/// Async authentication function: metadata + method → identity or rejection.
pub type AuthFn<I> = Arc<
    dyn Fn(&http::HeaderMap, &http::Uri) -> BoxFuture<'static, Result<I, AuthError>>
        + Send
        + Sync,
>;

/// Extract a bearer token from `authorization` metadata (case-insensitive
/// scheme), like go-grpc-middleware's `AuthFromMD(ctx, "bearer")`.
pub fn bearer(headers: &http::HeaderMap) -> Option<&str> {
    let value = headers.get("authorization")?.to_str().ok()?;
    let (scheme, token) = value.split_once(' ')?;
    if scheme.eq_ignore_ascii_case("bearer") {
        Some(token.trim())
    } else {
        None
    }
}

/// Server-side auth layer: wrap it around `tonic::service::Routes` (or the
/// traced routes) before `grpc_ws`/`grpc_ws_session`.
pub struct AuthLayer<I> {
    f: AuthFn<I>,
}

impl<I> AuthLayer<I> {
    pub fn new(f: AuthFn<I>) -> Self {
        Self { f }
    }
}

impl<I> Clone for AuthLayer<I> {
    fn clone(&self) -> Self {
        Self { f: self.f.clone() }
    }
}

impl<S, I> tower::Layer<S> for AuthLayer<I> {
    type Service = AuthService<S, I>;

    fn layer(&self, inner: S) -> Self::Service {
        AuthService { inner, f: self.f.clone() }
    }
}

pub struct AuthService<S, I> {
    inner: S,
    f: AuthFn<I>,
}

impl<S: Clone, I> Clone for AuthService<S, I> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone(), f: self.f.clone() }
    }
}

fn rejection(e: &AuthError) -> http::Response<TonicBody> {
    let mut resp = http::Response::new(TonicBody::default());
    let headers = resp.headers_mut();
    headers.insert("content-type", "application/grpc".parse().expect("static"));
    headers.insert(
        "grpc-status",
        e.code.to_string().parse().expect("numeric status"),
    );
    if let Ok(v) = http::header::HeaderValue::from_str(&e.message) {
        headers.insert("grpc-message", v);
    }
    resp
}

impl<S, I> tower::Service<http::Request<TonicBody>> for AuthService<S, I>
where
    S: tower::Service<
            http::Request<TonicBody>,
            Response = http::Response<TonicBody>,
        > + Clone
        + Send
        + 'static,
    S::Future: Send,
    I: Clone + Send + Sync + 'static,
{
    type Response = http::Response<TonicBody>;
    type Error = S::Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: http::Request<TonicBody>) -> Self::Future {
        let mut inner = self.inner.clone();
        let auth = (self.f)(req.headers(), req.uri());
        Box::pin(async move {
            match auth.await {
                Ok(identity) => {
                    req.extensions_mut().insert(Identity(identity));
                    inner.call(req).await
                }
                Err(e) => {
                    tracing::warn!(code = e.code, message = %e.message, "rpc rejected by auth");
                    Ok(rejection(&e))
                }
            }
        })
    }
}

/// Client-side layer: injects an `authorization` metadata entry per call from
/// a token provider (rotation-friendly). An explicitly set `authorization`
/// header on the request wins — per-call credentials are not overridden.
#[derive(Clone)]
pub struct AuthTokenLayer {
    provider: Arc<dyn Fn() -> String + Send + Sync>,
}

impl AuthTokenLayer {
    /// `authorization: Bearer {token}` from the provider.
    pub fn bearer(provider: impl Fn() -> String + Send + Sync + 'static) -> Self {
        Self { provider: Arc::new(move || format!("Bearer {}", provider())) }
    }

    /// Raw `authorization` value from the provider (any scheme).
    pub fn raw(provider: impl Fn() -> String + Send + Sync + 'static) -> Self {
        Self { provider: Arc::new(provider) }
    }
}

impl<S> tower::Layer<S> for AuthTokenLayer {
    type Service = AuthTokenService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AuthTokenService { inner, provider: self.provider.clone() }
    }
}

#[derive(Clone)]
pub struct AuthTokenService<S> {
    inner: S,
    provider: Arc<dyn Fn() -> String + Send + Sync>,
}

impl<S, B, RB> tower::Service<http::Request<B>> for AuthTokenService<S>
where
    S: tower::Service<http::Request<B>, Response = http::Response<RB>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: http::Request<B>) -> Self::Future {
        if !req.headers().contains_key("authorization")
            && let Ok(v) = http::header::HeaderValue::from_str(&(self.provider)())
        {
            req.headers_mut().insert("authorization", v);
        }
        self.inner.call(req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::{Layer, Service, ServiceExt};

    fn empty_req() -> http::Request<TonicBody> {
        http::Request::builder()
            .uri("/t.S/M")
            .body(TonicBody::default())
            .unwrap()
    }

    fn token_auth() -> AuthFn<String> {
        Arc::new(|headers, _uri| {
            let token = bearer(headers).map(str::to_owned);
            Box::pin(async move {
                match token.as_deref() {
                    Some("secret") => Ok("user-1".to_string()),
                    Some(_) => Err(AuthError::permission_denied("bad token")),
                    None => Err(AuthError::unauthenticated("missing token")),
                }
            })
        })
    }

    /// Inner service asserting the identity extension is present.
    fn identity_probe() -> impl tower::Service<
        http::Request<TonicBody>,
        Response = http::Response<TonicBody>,
        Error = std::convert::Infallible,
        Future = impl Send,
    > + Clone
           + Send {
        tower::service_fn(|req: http::Request<TonicBody>| async move {
            let id = req
                .extensions()
                .get::<Identity<String>>()
                .expect("identity must be injected")
                .0
                .clone();
            let mut resp = http::Response::new(TonicBody::default());
            resp.headers_mut()
                .insert("x-identity", id.parse().unwrap());
            Ok(resp)
        })
    }

    #[tokio::test]
    async fn rejects_without_token() {
        let mut svc = AuthLayer::new(token_auth()).layer(identity_probe());
        let resp = svc.ready().await.unwrap().call(empty_req()).await.unwrap();
        assert_eq!(resp.headers().get("grpc-status").unwrap(), "16");
        assert_eq!(resp.headers().get("grpc-message").unwrap(), "missing token");
    }

    #[tokio::test]
    async fn wrong_token_is_permission_denied() {
        let mut svc = AuthLayer::new(token_auth()).layer(identity_probe());
        let mut req = empty_req();
        req.headers_mut()
            .insert("authorization", "Bearer nope".parse().unwrap());
        let resp = svc.ready().await.unwrap().call(req).await.unwrap();
        assert_eq!(resp.headers().get("grpc-status").unwrap(), "7");
    }

    #[tokio::test]
    async fn valid_token_passes_and_identity_reaches_handler() {
        let mut svc = AuthLayer::new(token_auth()).layer(identity_probe());
        let mut req = empty_req();
        req.headers_mut()
            .insert("authorization", "bEaReR secret".parse().unwrap()); // scheme регистронезависимая
        let resp = svc.ready().await.unwrap().call(req).await.unwrap();
        assert!(!resp.headers().contains_key("grpc-status"));
        assert_eq!(resp.headers().get("x-identity").unwrap(), "user-1");
    }

    #[tokio::test]
    async fn client_layer_injects_and_respects_explicit() {
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let probe = tower::service_fn({
            let seen = seen.clone();
            move |req: http::Request<TonicBody>| {
                let seen = seen.clone();
                async move {
                    seen.lock().unwrap().push(
                        req.headers()
                            .get("authorization")
                            .unwrap()
                            .to_str()
                            .unwrap()
                            .to_owned(),
                    );
                    Ok::<_, std::convert::Infallible>(http::Response::new(TonicBody::default()))
                }
            }
        });
        let mut svc = AuthTokenLayer::bearer(|| "tok-1".to_string()).layer(probe);

        svc.ready().await.unwrap().call(empty_req()).await.unwrap();

        let mut explicit = empty_req();
        explicit
            .headers_mut()
            .insert("authorization", "Bearer per-call".parse().unwrap());
        svc.ready().await.unwrap().call(explicit).await.unwrap();

        let seen = seen.lock().unwrap();
        assert_eq!(seen[0], "Bearer tok-1");
        assert_eq!(seen[1], "Bearer per-call"); // явный заголовок не перетёрт
    }
}
