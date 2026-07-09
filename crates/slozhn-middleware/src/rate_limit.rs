//! Server-side rate limiting on the GCRA algorithm (the one behind
//! `governor` and Redis' redis-cell): one timestamp per key — the
//! "theoretical arrival time" — gives precise sustained-rate enforcement
//! with configurable instant burst, no token-bucket drift, no windows.
//!
//! Shape, matching how this is deployed in production:
//! - a **default [`Quota`]** applies to every method; per-method overrides
//!   ([`RateLimitLayer::method_quota`]) and opt-outs
//!   ([`RateLimitLayer::unlimited_method`]) refine it;
//! - a **key function** picks the bucket per caller — by `authorization`
//!   metadata, an API-key header, or anything derived from
//!   `(headers, uri)`. Buckets are always additionally split per method.
//!   Without a key function all callers share one bucket per method;
//! - the **store is pluggable** ([`RateLimitStore`]): the built-in
//!   [`InMemoryStore`] is per-process (same assumption as `DedupLayer`);
//!   behind a load balancer implement the trait over a shared store —
//!   with Redis this is redis-cell's `CL.THROTTLE` or a small Lua script
//!   doing the same compare-and-set on the stored arrival time;
//! - **fail-open by default**: a store error lets the call through with a
//!   warning (availability over strictness); [`RateLimitLayer::fail_closed`]
//!   flips that to UNAVAILABLE for strict setups.
//!
//! Rejections are trailers-only `RESOURCE_EXHAUSTED` (8) responses with a
//! `retry-after` metadata entry (integer seconds, rounded up) so
//! well-behaved clients can pace themselves.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use tonic::body::Body as TonicBody;

/// Sustained rate + instant burst. `rate` requests per `per` long-term;
/// up to `burst` requests instantly from an idle bucket (default: `rate`).
#[derive(Clone, Copy, Debug)]
pub struct Quota {
    pub rate: u32,
    pub per: Duration,
    pub burst: u32,
}

impl Quota {
    /// `rate` requests per `per`. Burst defaults to `rate`.
    pub fn new(rate: u32, per: Duration) -> Self {
        assert!(rate > 0, "quota rate must be positive");
        assert!(!per.is_zero(), "quota period must be positive");
        Self { rate, per, burst: rate }
    }

    pub fn per_second(rate: u32) -> Self {
        Self::new(rate, Duration::from_secs(1))
    }

    pub fn per_minute(rate: u32) -> Self {
        Self::new(rate, Duration::from_secs(60))
    }

    /// Override the instant-burst allowance (clamped to at least 1).
    pub fn burst(mut self, burst: u32) -> Self {
        self.burst = burst.max(1);
        self
    }

    /// GCRA emission interval: one request "costs" this much time.
    fn emission_interval(&self) -> Duration {
        self.per / self.rate
    }
}

/// Outcome of a store check for one request.
#[derive(Clone, Copy, Debug)]
pub struct Decision {
    pub allowed: bool,
    /// How long until the next request would be admitted (zero if allowed).
    pub retry_after: Duration,
    /// Requests left in the burst allowance right now.
    pub remaining: u32,
}

/// Where the per-key GCRA state lives. Implement this over Redis (redis-cell
/// `CL.THROTTLE`, or a Lua compare-and-set on the stored arrival time) to
/// share limits across instances; the whole decision must be atomic in the
/// store, which is why the trait boundary is "check", not "get/set".
pub trait RateLimitStore: Send + Sync + 'static {
    fn check(&self, key: String, quota: Quota) -> BoxFuture<'static, Result<Decision, String>>;
}

/// Per-process GCRA store: one `Instant` per key under a Mutex.
///
/// Memory bound: when the map exceeds `max_keys` (default 100 000) expired
/// entries are swept; if every key is still live past the cap, new keys are
/// admitted WITHOUT storing state (fail-open per key) and a warning is
/// logged — a hard deny under memory pressure would turn the limiter itself
/// into a denial-of-service lever.
pub struct InMemoryStore {
    max_keys: usize,
    state: Mutex<HashMap<String, Instant>>,
}

impl Default for InMemoryStore {
    fn default() -> Self {
        Self::new(100_000)
    }
}

impl InMemoryStore {
    pub fn new(max_keys: usize) -> Self {
        Self { max_keys, state: Mutex::new(HashMap::new()) }
    }

    fn decide(&self, key: String, quota: Quota, now: Instant) -> Decision {
        let t = quota.emission_interval();
        let tolerance = t.saturating_mul(quota.burst);
        let mut state = self.state.lock().expect("rate limit store lock");

        let tat = state.get(&key).map_or(now, |v| (*v).max(now));
        let new_tat = tat + t;
        // time by which the bucket is "ahead" of real time if we admit this
        let delta = new_tat.saturating_duration_since(now);

        if delta > tolerance {
            return Decision {
                allowed: false,
                retry_after: delta - tolerance,
                remaining: 0,
            };
        }

        if !state.contains_key(&key) && state.len() >= self.max_keys {
            state.retain(|_, stored_tat| *stored_tat > now);
            if state.len() >= self.max_keys {
                tracing::warn!(
                    max_keys = self.max_keys,
                    "rate limit store full of live keys; admitting new key untracked",
                );
                return Decision { allowed: true, retry_after: Duration::ZERO, remaining: 0 };
            }
        }
        state.insert(key, new_tat);

        let headroom = tolerance - delta;
        let remaining = (headroom.as_nanos() / t.as_nanos().max(1)) as u32;
        Decision { allowed: true, retry_after: Duration::ZERO, remaining }
    }
}

impl RateLimitStore for InMemoryStore {
    fn check(&self, key: String, quota: Quota) -> BoxFuture<'static, Result<Decision, String>> {
        let decision = self.decide(key, quota, Instant::now());
        Box::pin(async move { Ok(decision) })
    }
}

/// Bucket key from request metadata: `(headers, uri) -> Some(key)`.
/// `None` groups the call into a shared `"anon"` bucket (still per method).
pub type RateKeyFn = Arc<dyn Fn(&http::HeaderMap, &http::Uri) -> Option<String> + Send + Sync>;

/// Server layer: GCRA rate limiting per method × caller key.
/// See the module docs for the production shape.
#[derive(Clone)]
pub struct RateLimitLayer {
    quota: Quota,
    /// `Some(quota)` — override; `None` — method is unlimited.
    per_method: HashMap<String, Option<Quota>>,
    key_fn: Option<RateKeyFn>,
    store: Arc<dyn RateLimitStore>,
    fail_open: bool,
}

impl RateLimitLayer {
    /// Rate-limit every method with `quota`, one shared bucket per method,
    /// state in a per-process [`InMemoryStore`].
    pub fn new(quota: Quota) -> Self {
        Self {
            quota,
            per_method: HashMap::new(),
            key_fn: None,
            store: Arc::new(InMemoryStore::default()),
            fail_open: true,
        }
    }

    /// Override the quota for one method (`"/pkg.Service/Method"`).
    pub fn method_quota(mut self, method: impl Into<String>, quota: Quota) -> Self {
        self.per_method.insert(normalize(method.into()), Some(quota));
        self
    }

    /// Exempt one method from limiting entirely.
    pub fn unlimited_method(mut self, method: impl Into<String>) -> Self {
        self.per_method.insert(normalize(method.into()), None);
        self
    }

    /// Split buckets per caller with a custom key function.
    pub fn key_by(
        mut self,
        f: impl Fn(&http::HeaderMap, &http::Uri) -> Option<String> + Send + Sync + 'static,
    ) -> Self {
        self.key_fn = Some(Arc::new(f));
        self
    }

    /// Split buckets per caller by a metadata entry (e.g. `"x-api-key"` or
    /// `"authorization"`). Missing header → the shared `"anon"` bucket.
    pub fn key_by_header(self, name: &'static str) -> Self {
        self.key_by(move |headers, _uri| {
            headers.get(name).and_then(|v| v.to_str().ok()).map(str::to_owned)
        })
    }

    /// Use an external [`RateLimitStore`] (shared across instances).
    pub fn store(mut self, store: Arc<dyn RateLimitStore>) -> Self {
        self.store = store;
        self
    }

    /// Reject with UNAVAILABLE when the store errors, instead of the
    /// default fail-open passthrough.
    pub fn fail_closed(mut self) -> Self {
        self.fail_open = false;
        self
    }
}

fn normalize(method: String) -> String {
    if method.starts_with('/') { method } else { format!("/{method}") }
}

impl<S> tower::Layer<S> for RateLimitLayer {
    type Service = RateLimitService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RateLimitService { inner, config: Arc::new(self.clone()) }
    }
}

pub struct RateLimitService<S> {
    inner: S,
    config: Arc<RateLimitLayer>,
}

impl<S: Clone> Clone for RateLimitService<S> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone(), config: self.config.clone() }
    }
}

fn rejection(code: tonic::Code, message: &str, retry_after: Duration) -> http::Response<TonicBody> {
    let mut resp = http::Response::new(TonicBody::default());
    let headers = resp.headers_mut();
    headers.insert(
        "content-type",
        http::header::HeaderValue::from_static("application/grpc"),
    );
    if !retry_after.is_zero() {
        let secs = retry_after.as_secs() + u64::from(retry_after.subsec_nanos() > 0);
        if let Ok(v) = http::header::HeaderValue::from_str(&secs.to_string()) {
            headers.insert("retry-after", v);
        }
    }
    let status = tonic::Status::new(code, message);
    let _ = status.add_header(headers);
    resp
}

impl<S> tower::Service<http::Request<TonicBody>> for RateLimitService<S>
where
    S: tower::Service<http::Request<TonicBody>, Response = http::Response<TonicBody>>
        + Clone
        + Send
        + 'static,
    S::Future: Send,
    S::Error: Send,
{
    type Response = http::Response<TonicBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<TonicBody>) -> Self::Future {
        // keep the poll_ready'ed instance, park the fresh clone in self
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let config = self.config.clone();

        let method = req.uri().path().to_owned();
        let quota = match config.per_method.get(&method) {
            Some(Some(q)) => *q,
            Some(None) => return Box::pin(async move { inner.call(req).await }),
            None => config.quota,
        };
        let caller = config
            .key_fn
            .as_ref()
            .and_then(|f| f(req.headers(), req.uri()))
            .unwrap_or_else(|| "anon".to_owned());
        let key = format!("{method}\u{1f}{caller}");

        Box::pin(async move {
            match config.store.check(key, quota).await {
                Ok(d) if d.allowed => inner.call(req).await,
                Ok(d) => {
                    tracing::debug!(
                        method,
                        retry_after_ms = d.retry_after.as_millis() as u64,
                        "rpc rejected by rate limit",
                    );
                    metrics::counter!("slozhn_rate_limited_total", "method" => method.clone())
                        .increment(1);
                    Ok(rejection(
                        tonic::Code::ResourceExhausted,
                        "rate limit exceeded",
                        d.retry_after,
                    ))
                }
                Err(e) if config.fail_open => {
                    tracing::warn!(method, error = %e, "rate limit store failed; failing open");
                    inner.call(req).await
                }
                Err(e) => {
                    tracing::warn!(method, error = %e, "rate limit store failed; failing closed");
                    Ok(rejection(
                        tonic::Code::Unavailable,
                        "rate limiter unavailable",
                        Duration::ZERO,
                    ))
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::{Layer, Service, ServiceExt};

    fn req(path: &str, header: Option<(&'static str, &str)>) -> http::Request<TonicBody> {
        let mut builder = http::Request::builder().uri(path);
        if let Some((k, v)) = header {
            builder = builder.header(k, v);
        }
        builder.body(TonicBody::default()).unwrap()
    }

    fn ok_service() -> impl tower::Service<
        http::Request<TonicBody>,
        Response = http::Response<TonicBody>,
        Error = std::convert::Infallible,
        Future = impl Send,
    > + Clone
    + Send {
        tower::service_fn(|_req: http::Request<TonicBody>| async {
            Ok(http::Response::new(TonicBody::default()))
        })
    }

    fn status_of(resp: &http::Response<TonicBody>) -> Option<u32> {
        resp.headers()
            .get("grpc-status")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok())
    }

    #[tokio::test]
    async fn burst_then_resource_exhausted_with_retry_after() {
        let mut svc = RateLimitLayer::new(Quota::per_minute(60).burst(2)).layer(ok_service());

        for _ in 0..2 {
            let resp = svc.ready().await.unwrap().call(req("/t.S/M", None)).await.unwrap();
            assert_eq!(status_of(&resp), None, "burst must pass");
        }
        let resp = svc.ready().await.unwrap().call(req("/t.S/M", None)).await.unwrap();
        assert_eq!(status_of(&resp), Some(8));
        let retry_after: u64 = resp
            .headers()
            .get("retry-after")
            .expect("retry-after set on rejection")
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        assert!(retry_after >= 1);
    }

    #[tokio::test]
    async fn replenishes_after_waiting() {
        // 20/sec → a new slot every 50ms
        let mut svc = RateLimitLayer::new(Quota::per_second(20).burst(1)).layer(ok_service());

        let resp = svc.ready().await.unwrap().call(req("/t.S/M", None)).await.unwrap();
        assert_eq!(status_of(&resp), None);
        let resp = svc.ready().await.unwrap().call(req("/t.S/M", None)).await.unwrap();
        assert_eq!(status_of(&resp), Some(8));

        tokio::time::sleep(Duration::from_millis(80)).await;
        let resp = svc.ready().await.unwrap().call(req("/t.S/M", None)).await.unwrap();
        assert_eq!(status_of(&resp), None, "slot must replenish");
    }

    #[tokio::test]
    async fn per_method_override_and_unlimited() {
        let mut svc = RateLimitLayer::new(Quota::per_minute(60).burst(1))
            .method_quota("/t.S/Wide", Quota::per_minute(60).burst(3))
            .unlimited_method("/t.S/Free")
            .layer(ok_service());

        // default: second call over
        svc.ready().await.unwrap().call(req("/t.S/M", None)).await.unwrap();
        let resp = svc.ready().await.unwrap().call(req("/t.S/M", None)).await.unwrap();
        assert_eq!(status_of(&resp), Some(8));

        // override: 3 pass
        for _ in 0..3 {
            let resp = svc.ready().await.unwrap().call(req("/t.S/Wide", None)).await.unwrap();
            assert_eq!(status_of(&resp), None);
        }
        let resp = svc.ready().await.unwrap().call(req("/t.S/Wide", None)).await.unwrap();
        assert_eq!(status_of(&resp), Some(8));

        // unlimited: never rejected
        for _ in 0..10 {
            let resp = svc.ready().await.unwrap().call(req("/t.S/Free", None)).await.unwrap();
            assert_eq!(status_of(&resp), None);
        }
    }

    #[tokio::test]
    async fn keys_get_independent_buckets() {
        let mut svc = RateLimitLayer::new(Quota::per_minute(60).burst(1))
            .key_by_header("x-api-key")
            .layer(ok_service());

        let a = || req("/t.S/M", Some(("x-api-key", "alice")));
        let b = || req("/t.S/M", Some(("x-api-key", "bob")));

        assert_eq!(status_of(&svc.ready().await.unwrap().call(a()).await.unwrap()), None);
        assert_eq!(status_of(&svc.ready().await.unwrap().call(b()).await.unwrap()), None);
        assert_eq!(status_of(&svc.ready().await.unwrap().call(a()).await.unwrap()), Some(8));
        // missing key → shared "anon" bucket, independent of alice/bob
        assert_eq!(status_of(&svc.ready().await.unwrap().call(req("/t.S/M", None)).await.unwrap()), None);
    }

    struct FailingStore;

    impl RateLimitStore for FailingStore {
        fn check(&self, _key: String, _quota: Quota) -> BoxFuture<'static, Result<Decision, String>> {
            Box::pin(async { Err("store down".to_owned()) })
        }
    }

    #[tokio::test]
    async fn store_failure_fail_open_and_closed() {
        let mut open = RateLimitLayer::new(Quota::per_second(1))
            .store(Arc::new(FailingStore))
            .layer(ok_service());
        let resp = open.ready().await.unwrap().call(req("/t.S/M", None)).await.unwrap();
        assert_eq!(status_of(&resp), None, "fail-open lets the call through");

        let mut closed = RateLimitLayer::new(Quota::per_second(1))
            .store(Arc::new(FailingStore))
            .fail_closed()
            .layer(ok_service());
        let resp = closed.ready().await.unwrap().call(req("/t.S/M", None)).await.unwrap();
        assert_eq!(status_of(&resp), Some(14));
    }

    #[test]
    fn gcra_math_is_exact() {
        let store = InMemoryStore::default();
        let q = Quota::per_second(10).burst(3); // t=100ms, tolerance=300ms
        let t0 = Instant::now();

        // 3 instant requests pass, remaining counts down
        assert!(store.decide("k".into(), q, t0).allowed);
        assert!(store.decide("k".into(), q, t0).allowed);
        let third = store.decide("k".into(), q, t0);
        assert!(third.allowed);
        assert_eq!(third.remaining, 0);

        let fourth = store.decide("k".into(), q, t0);
        assert!(!fourth.allowed);
        assert_eq!(fourth.retry_after, Duration::from_millis(100));

        // 100ms later exactly one slot freed
        let t1 = t0 + Duration::from_millis(100);
        assert!(store.decide("k".into(), q, t1).allowed);
        assert!(!store.decide("k".into(), q, t1).allowed);
    }

    #[test]
    fn store_cap_sweeps_expired_keys() {
        let store = InMemoryStore::new(2);
        let q = Quota::per_second(10).burst(1); // TAT expires 100ms after last hit
        let t0 = Instant::now();

        assert!(store.decide("a".into(), q, t0).allowed);
        assert!(store.decide("b".into(), q, t0).allowed);
        // both expired by t1 → sweep makes room, new key is tracked
        let t1 = t0 + Duration::from_millis(200);
        assert!(store.decide("c".into(), q, t1).allowed);
        assert!(!store.decide("c".into(), q, t1).allowed, "c must be tracked after sweep");
    }
}
