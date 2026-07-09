//! Server-side response deduplication, completing the idempotency story
//! started by [`super::IdempotencyKeyLayer`] / [`super::IDEMPOTENCY_KEY_METADATA`]:
//! a replay carrying the same `x-idempotency-key` gets back the exact
//! response the first attempt produced, instead of re-executing the handler.
//!
//! Caching rules:
//! - No `x-idempotency-key` header on the request → passthrough, untouched.
//! - Key present, fresh cache entry → the cached response is rebuilt
//!   (status, headers, body, trailers) and returned WITHOUT calling the
//!   inner service.
//! - Key present, cache miss → the inner service is called; the response
//!   body is buffered up to `max_body_bytes`. If it fits, the response
//!   (including any trailers frame) is cached and a rebuilt copy is
//!   returned. If it doesn't fit (a streaming response), it is NOT cached;
//!   the already-read prefix is chained with the unread remainder so the
//!   caller still sees the complete, unmodified body end to end.
//! - EVERYTHING terminal is cached, including error statuses: an idempotent
//!   retry must observe the same outcome the first attempt produced, not a
//!   fresh execution that might disagree with it.
//!
//! Scaling out: cached entries live in a pluggable [`DedupStore`]. The
//! built-in [`InMemoryDedupStore`] is per-process — behind a load balancer
//! it only dedups retries that land on the same instance. For multi-node
//! deployments implement the trait over a shared store; [`CachedResponse`]
//! is plain data (status + header pairs + bytes) precisely so it
//! serializes trivially (with Redis: `SET key blob EX ttl` on put, `GET` on
//! get). Store errors fail open: the handler runs and nothing is cached —
//! a dead cache must not take the service down with it.
//!
//! Single-flight: concurrent requests carrying the same key on the SAME
//! process coordinate so only one of them (the "leader") reaches the inner
//! service; the rest wait for the leader to finish and then re-check the
//! store. If the leader's response turned out to be uncacheable (e.g. it
//! exceeded `max_body_bytes`) or the store failed, a waiter that still
//! misses on re-check simply executes the inner service itself rather than
//! waiting again. Waiting is async and capped at 30s, so a leader stuck on
//! a slow handler cannot wedge its waiters forever. This coordination is
//! per-process only: concurrent duplicates that land on DIFFERENT nodes
//! behind a load balancer may still both execute — cross-node single-flight
//! would require store-side locking and is out of scope here.
//!
//! **The dedup key is caller-controlled, by design and by the gRPC
//! idempotency-key convention**: `x-idempotency-key` is whatever value the
//! peer put on the request, same as `Idempotency-Key` in the Stripe-style
//! HTTP convention this mirrors. That's fine for its actual job — collapsing
//! a client's own retries — but it means DO NOT use the raw dedup cache
//! entry as a security decision (e.g. "this key was already spent by this
//! user, so trust the cached result for anyone"): nothing stops a different,
//! unrelated caller from supplying the same key and observing (or, if it
//! raced the original, receiving) another caller's cached response. If keys
//! must not be guessable/reusable across callers, scope them to a verified
//! identity with [`DedupLayer::key_prefix_by_identity`], which prefixes the
//! cache key with a string derived from the post-auth `Identity<T>` placed
//! in request extensions by `AuthLayer` — order `DedupLayer` AFTER
//! `AuthLayer` in the stack for the prefix to see it.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::future::BoxFuture;
use http::HeaderMap;
use http_body::Frame;
use http_body_util::BodyExt as _;
use tonic::body::Body as TonicBody;

use crate::idempotency::IDEMPOTENCY_KEY_METADATA;

/// Hard cap on how long a waiter blocks for the leader before giving up and
/// executing the inner service itself.
const SINGLE_FLIGHT_WAIT_CAP: Duration = Duration::from_secs(30);

/// Per-process single-flight coordination: one `watch` channel per in-flight
/// key, flipped from `false` to `true` when the leader is done. Shared
/// across `DedupService` clones the same way the store `Arc` is.
type InFlightMap = Arc<Mutex<HashMap<String, tokio::sync::watch::Receiver<bool>>>>;

/// Registers `key` as in-flight for the caller (making it the leader) if no
/// other leader is currently registered for it; otherwise returns the
/// existing leader's completion receiver so the caller can wait on it.
fn try_become_leader(
    in_flight: &InFlightMap,
    key: &str,
) -> Result<tokio::sync::watch::Sender<bool>, tokio::sync::watch::Receiver<bool>> {
    let mut map = in_flight.lock().unwrap();
    if let Some(rx) = map.get(key) {
        return Err(rx.clone());
    }
    let (tx, rx) = tokio::sync::watch::channel(false);
    map.insert(key.to_owned(), rx);
    Ok(tx)
}

/// Cleans up a leader's in-flight registration and signals completion on
/// every exit path (success, inner error, oversized body, panic, or
/// cancellation), so waiters are never wedged.
struct LeaderGuard {
    in_flight: InFlightMap,
    key: String,
    tx: tokio::sync::watch::Sender<bool>,
}

impl Drop for LeaderGuard {
    fn drop(&mut self) {
        self.in_flight.lock().unwrap().remove(&self.key);
        let _ = self.tx.send(true);
    }
}

/// One cached terminal response, as plain data so external stores can
/// serialize it however they like (headers/trailers are `(name, value)`
/// pairs; values are raw bytes).
#[derive(Clone, Debug)]
pub struct CachedResponse {
    pub status: u16,
    pub headers: Vec<(String, Bytes)>,
    pub body: Bytes,
    pub trailers: Option<Vec<(String, Bytes)>>,
}

/// Where dedup entries live. Implement over a shared store (Redis etc.) to
/// dedup across instances; `get`/`put` need no atomicity between them (the
/// layer only single-flights within a process, not across the store), only
/// `put` visibility after completion.
pub trait DedupStore: Send + Sync + 'static {
    fn get(&self, key: String) -> BoxFuture<'static, Result<Option<CachedResponse>, String>>;
    fn put(
        &self,
        key: String,
        response: CachedResponse,
        ttl: Duration,
    ) -> BoxFuture<'static, Result<(), String>>;
}

/// Per-process [`DedupStore`]: TTL + FIFO eviction over `max_entries`.
pub struct InMemoryDedupStore {
    max_entries: usize,
    inner: Mutex<CacheInner>,
}

#[derive(Default)]
struct CacheInner {
    map: HashMap<String, (CachedResponse, Instant)>,
    order: VecDeque<(String, Instant)>,
}

impl InMemoryDedupStore {
    pub fn new(max_entries: usize) -> Self {
        Self { max_entries, inner: Mutex::new(CacheInner::default()) }
    }

    fn evict_expired_locked(inner: &mut CacheInner, now: Instant) {
        while let Some((_key, deadline)) = inner.order.front() {
            if *deadline > now {
                break;
            }
            let (key, deadline) = inner.order.pop_front().unwrap();
            // Only remove from the map if this is still the entry that
            // scheduled this eviction (a re-insert under the same key
            // pushes a fresh order entry; the stale one is a no-op here).
            if let Some((_, current_deadline)) = inner.map.get(&key)
                && *current_deadline == deadline
            {
                inner.map.remove(&key);
            }
        }
    }

    fn get_fresh(&self, key: &str) -> Option<CachedResponse> {
        let now = Instant::now();
        let mut inner = self.inner.lock().unwrap();
        Self::evict_expired_locked(&mut inner, now);
        inner.map.get(key).map(|(entry, _)| entry.clone())
    }

    fn insert(&self, key: String, entry: CachedResponse, ttl: Duration) {
        let now = Instant::now();
        let deadline = now + ttl;
        let mut inner = self.inner.lock().unwrap();
        Self::evict_expired_locked(&mut inner, now);
        inner.map.insert(key.clone(), (entry, deadline));
        inner.order.push_back((key, deadline));
        while inner.map.len() > self.max_entries {
            let Some((oldest_key, oldest_deadline)) = inner.order.pop_front() else {
                break;
            };
            if let Some((_, current_deadline)) = inner.map.get(&oldest_key)
                && *current_deadline == oldest_deadline
            {
                inner.map.remove(&oldest_key);
            }
        }
    }
}

impl DedupStore for InMemoryDedupStore {
    fn get(&self, key: String) -> BoxFuture<'static, Result<Option<CachedResponse>, String>> {
        let hit = self.get_fresh(&key);
        Box::pin(async move { Ok(hit) })
    }

    fn put(
        &self,
        key: String,
        response: CachedResponse,
        ttl: Duration,
    ) -> BoxFuture<'static, Result<(), String>> {
        self.insert(key, response, ttl);
        Box::pin(async move { Ok(()) })
    }
}

/// Derives a cache-key prefix from request extensions (a post-auth
/// `Identity<T>`, typically). `None` means the request carries no identity
/// this resolver recognizes.
type IdentityPrefixFn = Arc<dyn Fn(&http::Extensions) -> Option<String> + Send + Sync>;

/// Server layer: dedups replays of the same `x-idempotency-key`.
///
/// Defaults: 300s TTL, 10_000 max entries (in-memory store), 256 KiB max
/// cached body. Use [`DedupLayer::store`] to share entries across instances.
///
/// See the module docs: `x-idempotency-key` is caller-supplied and MUST NOT
/// be treated as a security boundary on its own — use
/// [`DedupLayer::key_prefix_by_identity`] to scope it to a verified caller.
#[derive(Clone)]
pub struct DedupLayer {
    pub ttl: Duration,
    pub max_body_bytes: usize,
    store: Arc<dyn DedupStore>,
    in_flight: InFlightMap,
    identity_prefix: Option<IdentityPrefixFn>,
}

impl Default for DedupLayer {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(300),
            max_body_bytes: 256 * 1024,
            store: Arc::new(InMemoryDedupStore::new(10_000)),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            identity_prefix: None,
        }
    }
}

impl DedupLayer {
    /// Use an external [`DedupStore`] (shared across instances).
    pub fn store(mut self, store: Arc<dyn DedupStore>) -> Self {
        self.store = store;
        self
    }

    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    pub fn max_body_bytes(mut self, max_body_bytes: usize) -> Self {
        self.max_body_bytes = max_body_bytes;
        self
    }

    /// Scope the dedup cache key to a verified caller: the `x-idempotency-key`
    /// value is prefixed with a string derived from the post-auth
    /// `Identity<T>` that `AuthLayer` places in request extensions (order
    /// `DedupLayer` AFTER `AuthLayer` for this to see it). Without a prefix,
    /// two different callers who happen to submit the same idempotency key
    /// would collide in the cache; with it, they land in disjoint entries.
    /// Requests with no matching `Identity<T>` extension fall back to the
    /// raw, unprefixed key.
    pub fn key_prefix_by_identity<T, F>(mut self, f: F) -> Self
    where
        T: Clone + Send + Sync + 'static,
        F: Fn(&T) -> String + Send + Sync + 'static,
    {
        self.identity_prefix = Some(Arc::new(move |extensions: &http::Extensions| {
            extensions.get::<crate::auth::Identity<T>>().map(|identity| f(&identity.0))
        }));
        self
    }
}

impl<S> tower::Layer<S> for DedupLayer {
    type Service = DedupService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        DedupService {
            inner,
            store: self.store.clone(),
            ttl: self.ttl,
            max_body_bytes: self.max_body_bytes,
            in_flight: self.in_flight.clone(),
            identity_prefix: self.identity_prefix.clone(),
        }
    }
}

#[derive(Clone)]
pub struct DedupService<S> {
    inner: S,
    store: Arc<dyn DedupStore>,
    ttl: Duration,
    max_body_bytes: usize,
    in_flight: InFlightMap,
    identity_prefix: Option<IdentityPrefixFn>,
}

fn headers_to_pairs(headers: &HeaderMap) -> Vec<(String, Bytes)> {
    headers
        .iter()
        .map(|(name, value)| {
            (name.as_str().to_owned(), Bytes::copy_from_slice(value.as_bytes()))
        })
        .collect()
}

fn pairs_to_headers(pairs: &[(String, Bytes)]) -> HeaderMap {
    let mut headers = HeaderMap::with_capacity(pairs.len());
    for (name, value) in pairs {
        let (Ok(name), Ok(value)) = (
            name.parse::<http::header::HeaderName>(),
            http::header::HeaderValue::from_bytes(value),
        ) else {
            continue; // a pair invalid for HTTP came out of the store — skip
        };
        headers.append(name, value);
    }
    headers
}

/// A body that replays a cached data frame + optional trailers frame.
struct CachedBody {
    data: Option<Bytes>,
    trailers: Option<HeaderMap>,
}

impl http_body::Body for CachedBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let this = self.get_mut();
        if let Some(data) = this.data.take()
            && !data.is_empty()
        {
            return Poll::Ready(Some(Ok(Frame::data(data))));
        }
        if let Some(trailers) = this.trailers.take() {
            return Poll::Ready(Some(Ok(Frame::trailers(trailers))));
        }
        Poll::Ready(None)
    }
}

/// A body that replays an already-read prefix (data + optional trailers or
/// error) and then continues from the unread remainder of the original
/// body. Used when a response body exceeds the cache's size cap: the caller
/// still sees the exact, complete stream even though nothing gets cached.
struct PrefixThenRest {
    prefix_data: Option<Bytes>,
    prefix_trailers: Option<HeaderMap>,
    prefix_error: Option<tonic::Status>,
    rest: TonicBody,
}

impl http_body::Body for PrefixThenRest {
    type Data = Bytes;
    type Error = tonic::Status;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let this = self.get_mut();
        if let Some(data) = this.prefix_data.take()
            && !data.is_empty()
        {
            return Poll::Ready(Some(Ok(Frame::data(data))));
        }
        if let Some(trailers) = this.prefix_trailers.take() {
            return Poll::Ready(Some(Ok(Frame::trailers(trailers))));
        }
        if let Some(err) = this.prefix_error.take() {
            return Poll::Ready(Some(Err(err)));
        }
        Pin::new(&mut this.rest).poll_frame(cx)
    }
}

fn concat(chunks: Vec<Bytes>) -> Bytes {
    if chunks.len() == 1 {
        return chunks.into_iter().next().unwrap();
    }
    let mut buf = Vec::with_capacity(chunks.iter().map(Bytes::len).sum());
    for chunk in chunks {
        buf.extend_from_slice(&chunk);
    }
    Bytes::from(buf)
}

/// Buffer a response body up to `cap` bytes of data.
///
/// `Ok((data, trailers))` — the body ended within the cap, safe to cache.
/// `Err(rebuilt_body)` — the cap was exceeded (or a frame error occurred);
/// the caller must return the rebuilt body but must NOT cache anything.
async fn collect_body(
    mut body: TonicBody,
    cap: usize,
) -> Result<(Bytes, Option<HeaderMap>), TonicBody> {
    let mut collected: Vec<Bytes> = Vec::new();
    let mut total = 0usize;
    let mut trailers: Option<HeaderMap> = None;

    loop {
        match body.frame().await {
            Some(Ok(frame)) => match frame.into_data() {
                Ok(data) => {
                    total += data.len();
                    collected.push(data);
                    if total > cap {
                        let prefix = concat(collected);
                        return Err(TonicBody::new(PrefixThenRest {
                            prefix_data: Some(prefix),
                            prefix_trailers: None,
                            prefix_error: None,
                            rest: body,
                        }));
                    }
                }
                Err(trailer_frame) => {
                    if let Ok(tr) = trailer_frame.into_trailers() {
                        trailers = Some(tr);
                    }
                }
            },
            Some(Err(err)) => {
                let prefix = concat(collected);
                return Err(TonicBody::new(PrefixThenRest {
                    prefix_data: Some(prefix),
                    prefix_trailers: trailers.take(),
                    prefix_error: Some(err),
                    rest: body,
                }));
            }
            None => return Ok((concat(collected), trailers)),
        }
    }
}

fn rebuild_response(cached: CachedResponse) -> http::Response<TonicBody> {
    let status =
        http::StatusCode::from_u16(cached.status).unwrap_or(http::StatusCode::OK);
    let headers = pairs_to_headers(&cached.headers);
    let trailers = cached.trailers.as_deref().map(pairs_to_headers);
    let body = TonicBody::new(CachedBody { data: Some(cached.body), trailers });
    let mut resp = http::Response::builder()
        .status(status)
        .body(body)
        .expect("rebuilt dedup response");
    *resp.headers_mut() = headers;
    resp
}

impl<S> tower::Service<http::Request<TonicBody>> for DedupService<S>
where
    S: tower::Service<http::Request<TonicBody>, Response = http::Response<TonicBody>>
        + Clone
        + Send
        + 'static,
    S::Future: Send,
    S::Error: Send,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<TonicBody>) -> Self::Future {
        let raw_key = req
            .headers()
            .get(IDEMPOTENCY_KEY_METADATA)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);

        let Some(raw_key) = raw_key else {
            return Box::pin(self.inner.call(req));
        };

        // Scope the key to the verified caller when a resolver is
        // configured, so unrelated callers reusing the same idempotency
        // key can't collide (or peek at each other's cached response).
        let key = match self.identity_prefix.as_ref().and_then(|f| f(req.extensions())) {
            Some(prefix) => format!("{prefix}\u{1f}{raw_key}"),
            None => raw_key,
        };

        let store = self.store.clone();
        let ttl = self.ttl;
        let max_body_bytes = self.max_body_bytes;
        let in_flight = self.in_flight.clone();
        // keep the poll_ready'ed instance, park the fresh clone in self
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            let mut store_ok = true;
            match store.get(key.clone()).await {
                Ok(Some(cached)) => {
                    tracing::debug!(%key, "idempotency dedup hit");
                    metrics::counter!("slozhn_dedup_hits_total").increment(1);
                    return Ok(rebuild_response(cached));
                }
                Ok(None) => {}
                Err(e) => {
                    // fail open: a dead cache must not take the service down
                    tracing::warn!(%key, error = %e, "dedup store get failed; failing open");
                    store_ok = false;
                }
            }

            // Single-flight: become the leader for this key, or wait for
            // whoever already is one, then re-check the store.
            let leader_guard = match try_become_leader(&in_flight, &key) {
                Ok(tx) => Some(LeaderGuard { in_flight: in_flight.clone(), key: key.clone(), tx }),
                Err(mut rx) => {
                    let waited = tokio::time::timeout(SINGLE_FLIGHT_WAIT_CAP, rx.changed()).await;
                    if waited.is_ok() {
                        // leader finished (or its guard was dropped); re-check the store
                        match store.get(key.clone()).await {
                            Ok(Some(cached)) => {
                                tracing::debug!(%key, "idempotency dedup hit after single-flight wait");
                                metrics::counter!("slozhn_dedup_hits_total").increment(1);
                                return Ok(rebuild_response(cached));
                            }
                            Ok(None) => {}
                            Err(e) => {
                                tracing::warn!(
                                    %key, error = %e,
                                    "dedup store get failed after single-flight wait; failing open"
                                );
                                store_ok = false;
                            }
                        }
                    } else {
                        tracing::warn!(%key, "single-flight wait cap exceeded; executing inner directly");
                    }
                    // still a miss (leader's result was uncacheable, store
                    // failed, or the wait cap was hit): execute directly,
                    // without becoming a leader ourselves.
                    None
                }
            };

            let resp = inner.call(req).await?;
            let (parts, body) = resp.into_parts();

            let result = match collect_body(body, max_body_bytes).await {
                Ok((data, trailers)) => {
                    let cached = CachedResponse {
                        status: parts.status.as_u16(),
                        headers: headers_to_pairs(&parts.headers),
                        body: data,
                        trailers: trailers.as_ref().map(headers_to_pairs),
                    };
                    if store_ok
                        && let Err(e) = store.put(key.clone(), cached.clone(), ttl).await
                    {
                        tracing::warn!(%key, error = %e, "dedup store put failed; not cached");
                    }
                    Ok(rebuild_response(cached))
                }
                Err(body) => Ok(http::Response::from_parts(parts, body)),
            };

            drop(leader_guard); // release + signal waiters, if we were the leader
            result
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::Full;
    use std::sync::atomic::{AtomicU32, Ordering};
    use tower::{Layer, Service, ServiceExt};

    fn req_with_key(key: Option<&str>) -> http::Request<TonicBody> {
        let mut builder = http::Request::builder().uri("/t.S/M");
        if let Some(key) = key {
            builder = builder.header(IDEMPOTENCY_KEY_METADATA, key);
        }
        builder
            .body(TonicBody::new(
                Full::new(Bytes::new()).map_err(|e: std::convert::Infallible| match e {}),
            ))
            .unwrap()
    }

    async fn body_of(resp: TonicBody) -> Bytes {
        let collected = resp.collect().await.unwrap();
        collected.to_bytes()
    }

    fn counting_echo(
        calls: Arc<AtomicU32>,
        payload: &'static [u8],
    ) -> impl tower::Service<
        http::Request<TonicBody>,
        Response = http::Response<TonicBody>,
        Error = std::convert::Infallible,
        Future = impl Send,
    > + Clone
    + Send {
        tower::service_fn(move |_req: http::Request<TonicBody>| {
            let calls = calls.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                let body = TonicBody::new(
                    Full::new(Bytes::from_static(payload))
                        .map_err(|e: std::convert::Infallible| match e {}),
                );
                Ok(http::Response::new(body))
            }
        })
    }

    #[tokio::test]
    async fn same_key_twice_calls_inner_once() {
        let calls = Arc::new(AtomicU32::new(0));
        let mut svc = DedupLayer::default().layer(counting_echo(calls.clone(), b"hello"));

        let r1 = svc.ready().await.unwrap().call(req_with_key(Some("k1"))).await.unwrap();
        let b1 = body_of(r1.into_body()).await;
        let r2 = svc.ready().await.unwrap().call(req_with_key(Some("k1"))).await.unwrap();
        let b2 = body_of(r2.into_body()).await;

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(b1, Bytes::from_static(b"hello"));
        assert_eq!(b1, b2);
    }

    #[tokio::test]
    async fn different_keys_call_inner_twice() {
        let calls = Arc::new(AtomicU32::new(0));
        let mut svc = DedupLayer::default().layer(counting_echo(calls.clone(), b"hello"));

        svc.ready().await.unwrap().call(req_with_key(Some("k1"))).await.unwrap();
        svc.ready().await.unwrap().call(req_with_key(Some("k2"))).await.unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn no_key_always_calls_inner() {
        let calls = Arc::new(AtomicU32::new(0));
        let mut svc = DedupLayer::default().layer(counting_echo(calls.clone(), b"hello"));

        svc.ready().await.unwrap().call(req_with_key(None)).await.unwrap();
        svc.ready().await.unwrap().call(req_with_key(None)).await.unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn expired_ttl_calls_inner_again() {
        let calls = Arc::new(AtomicU32::new(0));
        let mut svc = DedupLayer::default()
            .ttl(Duration::from_millis(20))
            .layer(counting_echo(calls.clone(), b"hello"));

        svc.ready().await.unwrap().call(req_with_key(Some("k1"))).await.unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        svc.ready().await.unwrap().call(req_with_key(Some("k1"))).await.unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn oversized_body_is_not_cached_but_stays_intact() {
        let calls = Arc::new(AtomicU32::new(0));
        let payload: &'static [u8] = b"way-too-big-body-for-the-cache-cap";
        let mut svc = DedupLayer::default()
            .max_body_bytes(4)
            .layer(counting_echo(calls.clone(), payload));

        let r1 = svc.ready().await.unwrap().call(req_with_key(Some("k1"))).await.unwrap();
        let b1 = body_of(r1.into_body()).await;
        assert_eq!(b1, Bytes::from_static(payload), "prefix + remainder reassemble intact");

        let r2 = svc.ready().await.unwrap().call(req_with_key(Some("k1"))).await.unwrap();
        let b2 = body_of(r2.into_body()).await;
        assert_eq!(b2, Bytes::from_static(payload));

        assert_eq!(calls.load(Ordering::SeqCst), 2, "oversized responses are never cached");
    }

    /// Two services sharing one store = two nodes behind a balancer: a
    /// replay landing on the other node still dedups.
    #[tokio::test]
    async fn shared_store_dedups_across_services() {
        let store: Arc<dyn DedupStore> = Arc::new(InMemoryDedupStore::new(100));
        let calls = Arc::new(AtomicU32::new(0));
        let mut node_a =
            DedupLayer::default().store(store.clone()).layer(counting_echo(calls.clone(), b"x"));
        let mut node_b =
            DedupLayer::default().store(store).layer(counting_echo(calls.clone(), b"x"));

        node_a.ready().await.unwrap().call(req_with_key(Some("k1"))).await.unwrap();
        let r = node_b.ready().await.unwrap().call(req_with_key(Some("k1"))).await.unwrap();
        assert_eq!(body_of(r.into_body()).await, Bytes::from_static(b"x"));

        assert_eq!(calls.load(Ordering::SeqCst), 1, "second node must hit the shared cache");
    }

    struct FailingStore;

    impl DedupStore for FailingStore {
        fn get(&self, _key: String) -> BoxFuture<'static, Result<Option<CachedResponse>, String>> {
            Box::pin(async { Err("store down".to_owned()) })
        }
        fn put(
            &self,
            _key: String,
            _response: CachedResponse,
            _ttl: Duration,
        ) -> BoxFuture<'static, Result<(), String>> {
            Box::pin(async { Err("store down".to_owned()) })
        }
    }

    /// Like `counting_echo` but sleeps before responding, so tests can get
    /// several concurrent calls to overlap in time.
    fn sleepy_echo(
        calls: Arc<AtomicU32>,
        payload: &'static [u8],
    ) -> impl tower::Service<
        http::Request<TonicBody>,
        Response = http::Response<TonicBody>,
        Error = std::convert::Infallible,
        Future = impl Send,
    > + Clone
    + Send {
        tower::service_fn(move |_req: http::Request<TonicBody>| {
            let calls = calls.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(100)).await;
                let body = TonicBody::new(
                    Full::new(Bytes::from_static(payload))
                        .map_err(|e: std::convert::Infallible| match e {}),
                );
                Ok(http::Response::new(body))
            }
        })
    }

    #[tokio::test]
    async fn concurrent_same_key_executes_inner_once() {
        let calls = Arc::new(AtomicU32::new(0));
        let svc = DedupLayer::default().layer(sleepy_echo(calls.clone(), b"hello"));

        let futs = (0..5).map(|_| {
            let mut svc = svc.clone();
            async move {
                let resp =
                    svc.ready().await.unwrap().call(req_with_key(Some("k1"))).await.unwrap();
                body_of(resp.into_body()).await
            }
        });
        let bodies = futures::future::join_all(futs).await;

        assert_eq!(calls.load(Ordering::SeqCst), 1, "single-flight: only the leader executes");
        for body in bodies {
            assert_eq!(body, Bytes::from_static(b"hello"));
        }
    }

    #[tokio::test]
    async fn concurrent_different_keys_run_in_parallel() {
        let calls = Arc::new(AtomicU32::new(0));
        let svc = DedupLayer::default().layer(sleepy_echo(calls.clone(), b"hello"));

        let mut svc1 = svc.clone();
        let mut svc2 = svc.clone();
        let fut1 = async move {
            svc1.ready().await.unwrap().call(req_with_key(Some("k1"))).await.unwrap();
        };
        let fut2 = async move {
            svc2.ready().await.unwrap().call(req_with_key(Some("k2"))).await.unwrap();
        };
        tokio::join!(fut1, fut2);

        assert_eq!(calls.load(Ordering::SeqCst), 2, "different keys never share a leader");
    }

    #[tokio::test]
    async fn waiter_executes_if_leader_result_uncacheable() {
        let calls = Arc::new(AtomicU32::new(0));
        let payload: &'static [u8] = b"too-big-for-the-cache-cap";
        let svc = DedupLayer::default()
            .max_body_bytes(4)
            .layer(sleepy_echo(calls.clone(), payload));

        let mut svc1 = svc.clone();
        let mut svc2 = svc.clone();
        let fut1 = async move {
            let resp = svc1.ready().await.unwrap().call(req_with_key(Some("k1"))).await.unwrap();
            body_of(resp.into_body()).await
        };
        let fut2 = async move {
            // let svc1 register as leader first
            tokio::time::sleep(Duration::from_millis(20)).await;
            let resp = svc2.ready().await.unwrap().call(req_with_key(Some("k1"))).await.unwrap();
            body_of(resp.into_body()).await
        };
        let (b1, b2) = tokio::join!(fut1, fut2);

        assert_eq!(b1, Bytes::from_static(payload));
        assert_eq!(b2, Bytes::from_static(payload));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "leader's oversized result forces the waiter to execute after recheck-miss"
        );
    }

    #[tokio::test]
    async fn identity_prefix_separates_colliding_keys_across_callers() {
        let calls = Arc::new(AtomicU32::new(0));
        let mut svc = DedupLayer::default()
            .key_prefix_by_identity::<String, _>(|id: &String| id.clone())
            .layer(counting_echo(calls.clone(), b"hello"));

        let mut alice_req = req_with_key(Some("k1"));
        alice_req.extensions_mut().insert(crate::auth::Identity("alice".to_owned()));
        let mut bob_req = req_with_key(Some("k1"));
        bob_req.extensions_mut().insert(crate::auth::Identity("bob".to_owned()));

        svc.ready().await.unwrap().call(alice_req).await.unwrap();
        svc.ready().await.unwrap().call(bob_req).await.unwrap();

        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "same raw idempotency key from two identities must not collide"
        );
    }

    #[tokio::test]
    async fn identity_prefix_still_dedups_the_same_caller() {
        let calls = Arc::new(AtomicU32::new(0));
        let mut svc = DedupLayer::default()
            .key_prefix_by_identity::<String, _>(|id: &String| id.clone())
            .layer(counting_echo(calls.clone(), b"hello"));

        let mut req1 = req_with_key(Some("k1"));
        req1.extensions_mut().insert(crate::auth::Identity("alice".to_owned()));
        let mut req2 = req_with_key(Some("k1"));
        req2.extensions_mut().insert(crate::auth::Identity("alice".to_owned()));

        svc.ready().await.unwrap().call(req1).await.unwrap();
        svc.ready().await.unwrap().call(req2).await.unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 1, "same caller replay is still deduped");
    }

    #[tokio::test]
    async fn store_failure_fails_open() {
        let calls = Arc::new(AtomicU32::new(0));
        let mut svc = DedupLayer::default()
            .store(Arc::new(FailingStore))
            .layer(counting_echo(calls.clone(), b"hello"));

        let r1 = svc.ready().await.unwrap().call(req_with_key(Some("k1"))).await.unwrap();
        assert_eq!(body_of(r1.into_body()).await, Bytes::from_static(b"hello"));
        let r2 = svc.ready().await.unwrap().call(req_with_key(Some("k1"))).await.unwrap();
        assert_eq!(body_of(r2.into_body()).await, Bytes::from_static(b"hello"));

        assert_eq!(calls.load(Ordering::SeqCst), 2, "dead store → every call executes");
    }
}
