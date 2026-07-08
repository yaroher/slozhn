//! Per-RPC tracing/logging for both sides of a slozhn stack.
//!
//! Span per call with OTel RPC semconv fields; INFO event on completion with
//! duration and grpc status (WARN for non-OK). The grpc status is taken from
//! response headers when present (trailers-only responses) and otherwise
//! captured from the trailers frame by wrapping the response body.

use std::pin::Pin;
use std::task::{Context, Poll};

use http_body::Frame;
use pin_project_lite::pin_project;
use tower::Layer as _;
use tracing::{Instrument, Span};

#[derive(Clone, Copy, Debug)]
enum Kind {
    Client,
    Server,
}

impl Kind {
    fn as_str(self) -> &'static str {
        match self {
            Kind::Client => "client",
            Kind::Server => "server",
        }
    }
}

/// One tracing layer for both sides: `TraceLayer::client()` around a channel,
/// `TraceLayer::server()` around `tonic::service::Routes`.
#[derive(Clone, Copy, Debug)]
pub struct TraceLayer {
    kind: Kind,
}

impl TraceLayer {
    pub fn client() -> Self {
        Self { kind: Kind::Client }
    }

    pub fn server() -> Self {
        Self { kind: Kind::Server }
    }
}

impl<S> tower::Layer<S> for TraceLayer {
    type Service = TraceService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TraceService { inner, kind: self.kind }
    }
}

#[derive(Clone)]
pub struct TraceService<S> {
    inner: S,
    kind: Kind,
}

fn split_path(path: &str) -> (&str, &str) {
    // "/pkg.Service/Method"
    let mut it = path.trim_start_matches('/').splitn(2, '/');
    let service = it.next().unwrap_or("");
    let method = it.next().unwrap_or("");
    (service, method)
}

fn grpc_status_from_headers(headers: &http::HeaderMap) -> Option<u32> {
    headers
        .get("grpc-status")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok())
}

fn record_status(span: &Span, code: u32) {
    span.record("rpc.grpc.status_code", code);
    if code == 0 {
        tracing::info!(parent: span, code, "rpc finished");
    } else {
        tracing::warn!(parent: span, code, "rpc finished with error");
    }
}

impl<S, B, RB> tower::Service<http::Request<B>> for TraceService<S>
where
    S: tower::Service<http::Request<B>, Response = http::Response<RB>>,
{
    type Response = http::Response<TracedBody<RB>>;
    type Error = S::Error;
    type Future = TraceFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: http::Request<B>) -> Self::Future {
        let (service, method) = split_path(req.uri().path());
        let span = tracing::info_span!(
            "rpc",
            rpc.system = "grpc",
            rpc.service = %service,
            rpc.method = %method,
            otel.kind = self.kind.as_str(),
            rpc.grpc.status_code = tracing::field::Empty,
        );

        #[cfg(feature = "otel")]
        match self.kind {
            Kind::Client => crate::otel::inject(&span, req.headers_mut()),
            Kind::Server => crate::otel::extract_parent(&span, req.headers()),
        }
        #[cfg(not(feature = "otel"))]
        let _ = &mut req;

        tracing::info!(parent: &span, "rpc started");
        let fut = {
            let _enter = span.enter();
            self.inner.call(req)
        };
        TraceFuture { inner: fut.instrument(span.clone()), span: Some(span) }
    }
}

pin_project! {
    pub struct TraceFuture<F> {
        #[pin]
        inner: tracing::instrument::Instrumented<F>,
        span: Option<Span>,
    }
}

impl<F, RB, E> std::future::Future for TraceFuture<F>
where
    F: std::future::Future<Output = Result<http::Response<RB>, E>>,
{
    type Output = Result<http::Response<TracedBody<RB>>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match std::task::ready!(this.inner.poll(cx)) {
            Ok(resp) => {
                let span = this.span.take().expect("polled after completion");
                // trailers-only: статус уже в headers
                if let Some(code) = grpc_status_from_headers(resp.headers()) {
                    record_status(&span, code);
                }
                let (parts, body) = resp.into_parts();
                let done = parts.headers.contains_key("grpc-status");
                Poll::Ready(Ok(http::Response::from_parts(
                    parts,
                    TracedBody { inner: body, span, done },
                )))
            }
            Err(e) => {
                if let Some(span) = this.span.take() {
                    tracing::warn!(parent: &span, "rpc transport error");
                }
                Poll::Ready(Err(e))
            }
        }
    }
}

pin_project! {
    /// Response body wrapper: captures the grpc status from the trailers
    /// frame; an end-of-body without any status is reported as an abort.
    pub struct TracedBody<B> {
        #[pin]
        inner: B,
        span: Span,
        done: bool,
    }
}

impl<B: http_body::Body> http_body::Body for TracedBody<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        let span = this.span;
        let done = this.done;
        let out = std::task::ready!(this.inner.poll_frame(cx));
        match &out {
            Some(Ok(frame)) => {
                if let Some(trailers) = frame.trailers_ref()
                    && !*done
                    && let Some(code) = grpc_status_from_headers(trailers)
                {
                    *done = true;
                    record_status(span, code);
                }
            }
            Some(Err(_)) => {
                if !*done {
                    *done = true;
                    tracing::warn!(parent: &*span, "rpc body error");
                }
            }
            None => {
                if !*done {
                    *done = true;
                    tracing::warn!(parent: &*span, "rpc ended without grpc-status");
                }
            }
        }
        Poll::Ready(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http_body_util::BodyExt;
    use std::sync::{Arc, Mutex};
    use tower::{Layer, Service, ServiceExt};
    use tracing_subscriber::layer::SubscriberExt;

    /// Collects closed span names + recorded fields via a tracing layer.
    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<String>>>);

    struct CaptureLayer(Captured);
    impl<S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>>
        tracing_subscriber::Layer<S> for CaptureLayer
    {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut msg = String::new();
            struct V<'a>(&'a mut String);
            impl tracing::field::Visit for V<'_> {
                fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
                    self.0.push_str(&format!("{}={:?};", f.name(), v));
                }
            }
            event.record(&mut V(&mut msg));
            self.0 .0.lock().unwrap().push(msg);
        }
    }

    fn full_body(data: &'static [u8]) -> http_body_util::Full<Bytes> {
        http_body_util::Full::new(Bytes::from_static(data))
    }

    #[tokio::test]
    async fn records_status_from_headers() {
        let captured = Captured::default();
        let subscriber =
            tracing_subscriber::registry().with(CaptureLayer(captured.clone()));
        let _guard = tracing::subscriber::set_default(subscriber);

        let svc = tower::service_fn(|_req: http::Request<http_body_util::Full<Bytes>>| async {
            let mut resp = http::Response::new(full_body(b""));
            resp.headers_mut().insert("grpc-status", "14".parse().unwrap());
            Ok::<_, std::convert::Infallible>(resp)
        });
        let mut traced = TraceLayer::client().layer(svc);

        let req = http::Request::builder()
            .uri("/test.Echo/Unary")
            .body(full_body(b"x"))
            .unwrap();
        let resp = traced.ready().await.unwrap().call(req).await.unwrap();
        drop(resp);

        let events = captured.0.lock().unwrap().join("\n");
        assert!(events.contains("rpc started"), "{events}");
        assert!(events.contains("code=14"), "{events}");
    }

    #[tokio::test]
    async fn records_status_from_trailers() {
        let captured = Captured::default();
        let subscriber =
            tracing_subscriber::registry().with(CaptureLayer(captured.clone()));
        let _guard = tracing::subscriber::set_default(subscriber);

        let svc = tower::service_fn(|_req: http::Request<http_body_util::Full<Bytes>>| async {
            let mut trailers = http::HeaderMap::new();
            trailers.insert("grpc-status", "0".parse().unwrap());
            let body = http_body_util::Full::new(Bytes::from_static(b"payload"))
                .with_trailers(async move { Some(Ok(trailers)) });
            Ok::<_, std::convert::Infallible>(http::Response::new(body))
        });
        let mut traced = TraceLayer::server().layer(svc);

        let req = http::Request::builder()
            .uri("/test.Echo/Unary")
            .body(full_body(b"x"))
            .unwrap();
        let resp = traced.ready().await.unwrap().call(req).await.unwrap();
        let _collected = resp.into_body().collect().await.unwrap();

        let events = captured.0.lock().unwrap().join("\n");
        assert!(events.contains("code=0"), "{events}");
    }
}

/// Server-side wrapper: like `TraceLayer::server()`, but the response body is
/// re-boxed into `tonic::body::Body`, so the result plugs straight into
/// `grpc_ws`/`grpc_ws_session` (their bounds require that exact body type).
pub fn trace_server<S>(svc: S) -> ServerTraced<S>
where
    S: tower::Service<
        http::Request<tonic::body::Body>,
        Response = http::Response<tonic::body::Body>,
    >,
{
    ServerTraced { inner: TraceLayer::server().layer(svc) }
}

#[derive(Clone)]
pub struct ServerTraced<S> {
    inner: TraceService<S>,
}

impl<S> tower::Service<http::Request<tonic::body::Body>> for ServerTraced<S>
where
    S: tower::Service<
        http::Request<tonic::body::Body>,
        Response = http::Response<tonic::body::Body>,
    >,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = S::Error;
    type Future = ReboxFuture<TraceFuture<S::Future>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<tonic::body::Body>) -> Self::Future {
        ReboxFuture { inner: self.inner.call(req) }
    }
}

pin_project! {
    pub struct ReboxFuture<F> {
        #[pin]
        inner: F,
    }
}

impl<F, E> std::future::Future for ReboxFuture<F>
where
    F: std::future::Future<Output = Result<http::Response<TracedBody<tonic::body::Body>>, E>>,
{
    type Output = Result<http::Response<tonic::body::Body>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match std::task::ready!(this.inner.poll(cx)) {
            Ok(resp) => Poll::Ready(Ok(resp.map(tonic::body::Body::new))),
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}
