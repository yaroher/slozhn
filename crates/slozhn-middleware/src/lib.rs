//! Ready-made tower layers for slozhn stacks.
//!
//! - [`TraceLayer`] — one layer for BOTH sides: a `tracing` span per RPC with
//!   OTel RPC semconv fields (`rpc.system`, `rpc.service`, `rpc.method`,
//!   `rpc.grpc.status_code`) plus start/finish events with duration — wire a
//!   `tracing_subscriber::fmt` and these are your logs; wire
//!   `tracing-opentelemetry` and they are your traces. The `otel` feature
//!   adds W3C `traceparent` propagation (client inject / server extract).
//! - [`RetryLayer`] — client-side unary retries on `UNAVAILABLE`: the request
//!   body is buffered up to a cap and replayed with jittered backoff;
//!   streaming requests (or bodies above the cap) pass through untouched.
//!
//! Client wiring:
//! ```ignore
//! let channel = slozhn::client::builder(url).resume().build();
//! let stack = tower::ServiceBuilder::new()
//!     .layer(slozhn::middleware::TraceLayer::client())
//!     .layer(slozhn::middleware::RetryLayer::default())
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
mod retry;
mod trace;

#[cfg(feature = "otel")]
mod otel;

pub use auth::{
    bearer, AuthError, AuthFn, AuthLayer, AuthService, AuthTokenLayer, AuthTokenService,
    Identity,
};
pub use retry::{RetryLayer, RetryService};
pub use trace::{trace_server, ServerTraced, TraceLayer, TraceService};
