//! Ready-made tower layers for slozhn stacks.
//!
//! - [`TraceLayer`] — one layer for BOTH sides: a `tracing` span per RPC with
//!   OTel RPC semconv fields (`rpc.system`, `rpc.service`, `rpc.method`,
//!   `rpc.grpc.status_code`) plus start/finish events with duration — wire a
//!   `tracing_subscriber::fmt` and these are your logs; wire
//!   `tracing-opentelemetry` and they are your traces. The `otel` feature
//!   adds W3C `traceparent` propagation (client inject / server extract).
//! - [`RetryLayer`] — client-side unary retries on `UNAVAILABLE`, gated by
//!   protobuf `idempotency_level` descriptors or explicit method allowlists.
//!   The request body is buffered up to a cap and replayed with jittered
//!   backoff; streaming requests (or bodies above the cap) pass through
//!   untouched.
//! - [`DedupLayer`] (server, non-wasm) — completes the idempotency story:
//!   replays carrying the same `x-idempotency-key` get back the cached
//!   response instead of re-executing the handler.
//! - [`TimeoutLayer`] — client-side per-call deadline; sets `grpc-timeout`
//!   for the server and races the call locally, canceling it on expiry.
//! - [`ValidateLayer`] — PGV message validation before the handler:
//!   zero-registration via descriptors (`validate` feature), typed and
//!   manual overrides; a user caster turns violations into the response
//!   (domain error details included).
//! - [`RateLimitLayer`] (server, non-wasm) — GCRA rate limiting per
//!   method × caller key, pluggable store ([`RateLimitStore`]) for shared
//!   limits behind a load balancer; over-limit calls get
//!   `RESOURCE_EXHAUSTED` with `retry-after` metadata.
//! - [`DeadlineLayer`] (server, non-wasm) — server-side enforcement of the
//!   `grpc-timeout` header (native tonic transport does this; our WS bridge
//!   otherwise wouldn't): cancels the handler, and cuts off a streaming
//!   response body, once the deadline elapses.
//! - [`MetricsLayer`] — one layer for BOTH sides: emits RPC start/inflight/
//!   duration series through the `metrics` facade (exporter-agnostic).
//!
//! Client wiring:
//! ```ignore
//! let channel = slozhn::client::builder(url).resume().build();
//! let stack = tower::ServiceBuilder::new()
//!     .layer(slozhn::middleware::TraceLayer::client())
//!     .layer(slozhn::middleware::RetryLayer::from_file_descriptor_set(
//!         my_proto::FILE_DESCRIPTOR_SET,
//!     )?)
//!     .service(channel);
//! let client = EchoClient::new(stack);
//! ```
//!
//! Server wiring:
//! ```ignore
//! let routes = tonic::service::Routes::new(EchoServer::new(MyEcho));
//! let traced = slozhn::middleware::trace_server(routes);
//! Router::new().route("/rpc", slozhn::server::grpc_ws(traced));
//! ```

mod auth;
mod idempotency;
mod metrics;
mod retry;
mod timeout;
mod trace;
mod validate;

#[cfg(not(target_arch = "wasm32"))]
mod deadline;
#[cfg(not(target_arch = "wasm32"))]
mod dedup;
#[cfg(not(target_arch = "wasm32"))]
mod rate_limit;

#[cfg(feature = "otel")]
mod otel;
#[cfg(feature = "otel")]
mod otel_metrics;

pub use auth::{
    AuthError, AuthFn, AuthLayer, AuthService, AuthTokenLayer, AuthTokenService, Identity, bearer,
};
#[cfg(not(target_arch = "wasm32"))]
pub use deadline::{DeadlineLayer, DeadlineService};
#[cfg(not(target_arch = "wasm32"))]
pub use dedup::{CachedResponse, DedupLayer, DedupService, DedupStore, InMemoryDedupStore};
pub use idempotency::{
    IDEMPOTENCY_KEY_METADATA, IdempotencyIndex, IdempotencyKeyLayer, IdempotencyKeyService,
    IdempotencyLevel,
};
pub use metrics::{MetricsBody, MetricsFuture, MetricsLayer, MetricsService};
#[cfg(feature = "otel")]
pub use otel_metrics::{OtelMetricsRecorder, otel_metrics_recorder};
#[cfg(not(target_arch = "wasm32"))]
pub use rate_limit::{
    Decision, InMemoryStore, Quota, RateKeyFn, RateLimitLayer, RateLimitService, RateLimitStore,
};
pub use retry::{RetryLayer, RetryPolicy, RetryService};
pub use timeout::{TimeoutError, TimeoutLayer, TimeoutService};
pub use trace::{ServerTraced, TraceLayer, TraceService, trace_server};
pub use validate::{ValidateLayer, ValidateService};
