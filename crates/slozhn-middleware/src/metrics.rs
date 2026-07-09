//! Prometheus-style metrics for both sides of a slozhn stack, emitted purely
//! through the [`metrics`] facade (crate `metrics`) — this module never talks
//! to Prometheus (or any other backend) directly; wire up whichever exporter
//! you like (`metrics-exporter-prometheus`, etc.) at process start and these
//! calls flow through it.
//!
//! Mirrors [`super::TraceLayer`]'s shape: one layer, `MetricsLayer::client()`
//! / `MetricsLayer::server()`, wrapping the same `http::Request<B>` →
//! `http::Response<RB>` generic service both sides share.
//!
//! Emitted series (labels in `{}`):
//! - `slozhn_rpc_started_total{side, method}` — counter, incremented when the
//!   call starts.
//! - `slozhn_rpc_inflight{side}` — gauge, incremented on start and
//!   decremented exactly once at the terminal point (response headers for a
//!   trailers-only response, the trailers frame for a streamed response, a
//!   transport error, or the body ending without a status). A small
//!   `Drop`-based guard makes the decrement leak-proof even if the response
//!   future/body is dropped early instead of driven to completion.
//! - `slozhn_rpc_duration_seconds{side, method, code}` — histogram, recorded
//!   at the same terminal point; `code` is the grpc-status as a decimal
//!   string, or `"error"` for a transport error / a body that ended without
//!   ever producing a status.
//!
//! `method` is the full RPC path, e.g. `/pkg.Svc/Method`.
//!
//! wasm: the `metrics` facade itself is a thin, allocation-light dispatch
//! layer with no I/O, so it compiles fine for `wasm32-unknown-unknown`; this
//! module is therefore unconditional (not gated like [`super::DedupLayer`]).

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use http_body::Frame;
use metrics::{counter, gauge, histogram};
use pin_project_lite::pin_project;

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

/// See module docs.
#[derive(Clone, Copy, Debug)]
pub struct MetricsLayer {
    kind: Kind,
}

impl MetricsLayer {
    pub fn client() -> Self {
        Self { kind: Kind::Client }
    }

    pub fn server() -> Self {
        Self { kind: Kind::Server }
    }
}

impl<S> tower::Layer<S> for MetricsLayer {
    type Service = MetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MetricsService { inner, kind: self.kind }
    }
}

#[derive(Clone)]
pub struct MetricsService<S> {
    inner: S,
    kind: Kind,
}

fn grpc_status_from_headers(headers: &http::HeaderMap) -> Option<u32> {
    headers.get("grpc-status").and_then(|v| v.to_str().ok()).and_then(|s| s.parse().ok())
}

fn record_duration(side: &'static str, method: &str, code: u32, elapsed: Duration) {
    histogram!(
        "slozhn_rpc_duration_seconds",
        "side" => side,
        "method" => method.to_owned(),
        "code" => code.to_string(),
    )
    .record(elapsed.as_secs_f64());
}

fn record_duration_error(side: &'static str, method: &str, elapsed: Duration) {
    histogram!(
        "slozhn_rpc_duration_seconds",
        "side" => side,
        "method" => method.to_owned(),
        "code" => "error",
    )
    .record(elapsed.as_secs_f64());
}

/// Leak-proof `slozhn_rpc_inflight{side}` decrement: whichever path lets go
/// of this guard first (terminal response, transport error, or an early
/// drop of the future/body) triggers exactly one decrement.
struct InflightGuard {
    side: &'static str,
}

impl InflightGuard {
    fn new(side: &'static str) -> Self {
        gauge!("slozhn_rpc_inflight", "side" => side).increment(1.0);
        Self { side }
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        gauge!("slozhn_rpc_inflight", "side" => self.side).decrement(1.0);
    }
}

impl<S, B, RB> tower::Service<http::Request<B>> for MetricsService<S>
where
    S: tower::Service<http::Request<B>, Response = http::Response<RB>>,
{
    type Response = http::Response<MetricsBody<RB>>;
    type Error = S::Error;
    type Future = MetricsFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        let side = self.kind.as_str();
        let method = req.uri().path().to_owned();

        counter!("slozhn_rpc_started_total", "side" => side, "method" => method.clone())
            .increment(1);
        let guard = InflightGuard::new(side);

        let start = Instant::now();
        let fut = self.inner.call(req);
        MetricsFuture { inner: fut, guard: Some(guard), start, method, side }
    }
}

pin_project! {
    pub struct MetricsFuture<F> {
        #[pin]
        inner: F,
        guard: Option<InflightGuard>,
        start: Instant,
        method: String,
        side: &'static str,
    }
}

impl<F, RB, E> Future for MetricsFuture<F>
where
    F: Future<Output = Result<http::Response<RB>, E>>,
{
    type Output = Result<http::Response<MetricsBody<RB>>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match std::task::ready!(this.inner.poll(cx)) {
            Ok(resp) => {
                let elapsed = this.start.elapsed();
                let headers_code = grpc_status_from_headers(resp.headers());
                let (parts, body) = resp.into_parts();

                if let Some(code) = headers_code {
                    // Trailers-only: the terminal status is already known.
                    record_duration(this.side, this.method, code, elapsed);
                    return Poll::Ready(Ok(http::Response::from_parts(
                        parts,
                        MetricsBody {
                            inner: body,
                            guard: None, // dropped here: decrements inflight
                            start: *this.start,
                            method: this.method.clone(),
                            side: this.side,
                            done: true,
                        },
                    )));
                }

                let guard = this.guard.take().expect("polled after completion");
                Poll::Ready(Ok(http::Response::from_parts(
                    parts,
                    MetricsBody {
                        inner: body,
                        guard: Some(guard),
                        start: *this.start,
                        method: this.method.clone(),
                        side: this.side,
                        done: false,
                    },
                )))
            }
            Err(e) => {
                if this.guard.take().is_some() {
                    record_duration_error(this.side, this.method, this.start.elapsed());
                }
                Poll::Ready(Err(e))
            }
        }
    }
}

pin_project! {
    /// Response body wrapper: records `slozhn_rpc_duration_seconds` at the
    /// terminal point (trailers frame, body error, or end-of-body without a
    /// status) and releases the inflight guard there.
    pub struct MetricsBody<B> {
        #[pin]
        inner: B,
        guard: Option<InflightGuard>,
        start: Instant,
        method: String,
        side: &'static str,
        done: bool,
    }
}

impl<B: http_body::Body> http_body::Body for MetricsBody<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        let out = std::task::ready!(this.inner.poll_frame(cx));
        if !*this.done {
            match &out {
                Some(Ok(frame)) => {
                    if let Some(trailers) = frame.trailers_ref() {
                        *this.done = true;
                        let elapsed = this.start.elapsed();
                        match grpc_status_from_headers(trailers) {
                            Some(code) => record_duration(this.side, this.method, code, elapsed),
                            None => record_duration_error(this.side, this.method, elapsed),
                        }
                        this.guard.take();
                    }
                }
                Some(Err(_)) => {
                    *this.done = true;
                    record_duration_error(this.side, this.method, this.start.elapsed());
                    this.guard.take();
                }
                None => {
                    *this.done = true;
                    record_duration_error(this.side, this.method, this.start.elapsed());
                    this.guard.take();
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
    use http_body_util::BodyExt as _;
    use metrics_util::CompositeKey;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use tonic::body::Body as TonicBody;
    use tower::{Layer, Service, ServiceExt};

    fn req() -> http::Request<TonicBody> {
        http::Request::builder().uri("/test.Echo/Unary").body(TonicBody::default()).unwrap()
    }

    fn find<'a>(
        snapshot: &'a [(CompositeKey, Option<metrics::Unit>, Option<metrics::SharedString>, DebugValue)],
        name: &str,
    ) -> Vec<&'a (CompositeKey, Option<metrics::Unit>, Option<metrics::SharedString>, DebugValue)> {
        snapshot.iter().filter(|(key, ..)| key.key().name() == name).collect()
    }

    fn label<'a>(key: &'a CompositeKey, name: &str) -> Option<&'a str> {
        key.key().labels().find(|l| l.key() == name).map(|l| l.value())
    }

    #[tokio::test]
    async fn trailers_only_records_counter_histogram_and_inflight() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            futures::executor::block_on(async {
                let svc = tower::service_fn(|_req: http::Request<TonicBody>| async move {
                    let mut resp = http::Response::new(TonicBody::default());
                    resp.headers_mut().insert("grpc-status", "0".parse().unwrap());
                    Ok::<_, std::convert::Infallible>(resp)
                });
                let mut svc = MetricsLayer::server().layer(svc);
                let resp = svc.ready().await.unwrap().call(req()).await.unwrap();
                let _ = resp.into_body().collect().await.unwrap();
            });
        });

        let snapshot = snapshotter.snapshot().into_vec();

        let started = find(&snapshot, "slozhn_rpc_started_total");
        assert_eq!(started.len(), 1);
        assert_eq!(label(&started[0].0, "side"), Some("server"));
        assert_eq!(label(&started[0].0, "method"), Some("/test.Echo/Unary"));
        assert!(matches!(started[0].3, DebugValue::Counter(1)));

        let duration = find(&snapshot, "slozhn_rpc_duration_seconds");
        assert_eq!(duration.len(), 1);
        assert_eq!(label(&duration[0].0, "code"), Some("0"));
        assert!(matches!(&duration[0].3, DebugValue::Histogram(v) if v.len() == 1));

        let inflight = find(&snapshot, "slozhn_rpc_inflight");
        assert_eq!(inflight.len(), 1);
        assert!(matches!(inflight[0].3, DebugValue::Gauge(v) if v == 0.0));
    }

    #[tokio::test]
    async fn trailers_frame_records_histogram_with_status_and_zeroes_inflight() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            futures::executor::block_on(async {
                let svc = tower::service_fn(|_req: http::Request<TonicBody>| async move {
                    let mut trailers = http::HeaderMap::new();
                    trailers.insert("grpc-status", "14".parse().unwrap());
                    let body = http_body_util::Full::new(Bytes::from_static(b"payload"))
                        .map_err(|e: std::convert::Infallible| match e {})
                        .with_trailers(async move { Some(Ok(trailers)) });
                    Ok::<_, std::convert::Infallible>(http::Response::new(TonicBody::new(body)))
                });
                let mut svc = MetricsLayer::client().layer(svc);
                let resp = svc.ready().await.unwrap().call(req()).await.unwrap();
                let _ = resp.into_body().collect().await.unwrap();
            });
        });

        let snapshot = snapshotter.snapshot().into_vec();

        let duration = find(&snapshot, "slozhn_rpc_duration_seconds");
        assert_eq!(duration.len(), 1);
        assert_eq!(label(&duration[0].0, "side"), Some("client"));
        assert_eq!(label(&duration[0].0, "code"), Some("14"));

        let inflight = find(&snapshot, "slozhn_rpc_inflight");
        assert!(matches!(inflight[0].3, DebugValue::Gauge(v) if v == 0.0));
    }

    #[tokio::test]
    async fn dropped_body_still_zeroes_inflight() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            futures::executor::block_on(async {
                let svc = tower::service_fn(|_req: http::Request<TonicBody>| async move {
                    // No grpc-status header, and the body never emits a
                    // trailers frame — an early drop must still zero
                    // inflight via the guard's Drop impl.
                    let body = http_body_util::Full::new(Bytes::from_static(b"x"))
                        .map_err(|e: std::convert::Infallible| match e {});
                    Ok::<_, std::convert::Infallible>(http::Response::new(TonicBody::new(body)))
                });
                let mut svc = MetricsLayer::server().layer(svc);
                let resp = svc.ready().await.unwrap().call(req()).await.unwrap();
                drop(resp);
            });
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let inflight = find(&snapshot, "slozhn_rpc_inflight");
        assert!(matches!(inflight[0].3, DebugValue::Gauge(v) if v == 0.0));
    }
}
