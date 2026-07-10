# slozhn

**gRPC over WebSocket for native and wasm**: all four streaming kinds,
metadata, trailers, statuses, automatic reconnect, and transparent stream
resume after network breaks. Plain tonic services and tonic clients — no
tonic fork.

## Why

Browsers cannot speak real gRPC: `fetch` does not expose trailers (where
`grpc-status` lives) and cannot stream request bodies, which caps every
browser gRPC stack (grpc-web, tonic-web) at unary + server-streaming.
WebSocket is the only full-duplex channel available everywhere. slozhn
carries the entire gRPC stack over it: the same client code runs on native
(tokio) and in the browser (wasm32).

## Getting started

### Prerequisites

| What | Needed for |
|---|---|
| Rust (stable) + cargo | everything |
| `protoc-gen-prost`, `protoc-gen-tonic` (`cargo install protoc-gen-prost protoc-gen-tonic`) | generating types from your `.proto` |
| a protoc driver — [easyp](https://github.com/easyp-tech/easyp) (used in this repo) or `buf` / `protoc` | running the generators |
| `rustup target add wasm32-unknown-unknown` + [`wasm-pack`](https://rustwasm.github.io/wasm-pack/) | browser builds only |

### 1. Define a service and generate code

Standard prost/tonic codegen — slozhn ships no generators of its own.
With easyp (see `easyp.yaml` in this repo for a working config):

```yaml
generate:
  inputs:
    - directory: protocols
  plugins:
    - name: prost
      out: gen/src
      opts:
        bytes: "."
        file_descriptor_set: true
    - name: tonic
      out: gen/src
      opts: { no_transport: "true" }   # keep wasm builds possible
```

```bash
easyp generate
```

Mark retry-safe unary RPCs in protobuf and pass the generated descriptor set
to `RetryLayer`:

```proto
import "google/protobuf/descriptor.proto";

rpc GetUser(GetUserRequest) returns (GetUserResponse) {
  option idempotency_level = NO_SIDE_EFFECTS;
}

rpc PutSettings(PutSettingsRequest) returns (PutSettingsResponse) {
  option idempotency_level = IDEMPOTENT;
}
```

### 2. Server (axum)

```toml
slozhn = { version = "1", features = ["client", "server"] }
```

```rust
let routes = tonic::service::Routes::new(EchoServer::new(MyEcho));

// plain endpoint
let app = axum::Router::new().route("/rpc", slozhn::server::grpc_ws(routes));

// or with the session layer: streams survive network breaks
let manager = slozhn::server::SessionManager::new(Default::default());
let app = axum::Router::new().route("/rpc", slozhn::server::grpc_ws_session(routes, manager));

axum::serve(listener, app).await?;
```

Your service implementations are ordinary `#[tonic::async_trait]` code and
get the full tonic stack: interceptors, tower layers, metadata, trailers.

### 3. Client (native and browser — the same code)

```rust
let channel = slozhn::client::builder("ws://host/rpc")
    .resume()   // opt-in: requires grpc_ws_session on the server
    .build();   // lazy: connects on the first call

let mut client = EchoClient::new(channel); // ordinary generated tonic client
```

The builder assembles the whole stack (WebSocket → frame codec → optional
session → reconnect) and picks the executor per platform (`tokio::spawn`
on native, `spawn_local` on wasm). On disconnect, in-flight RPCs end with
`UNAVAILABLE` (or survive transparently with `.resume()`); new calls wait
for the reconnect with exponential backoff.

Browser notes: build with `wasm32-unknown-unknown`; the client crate pulls
no tokio runtime. The browser `WebSocket` API cannot set headers — pass
auth via cookies or per-RPC metadata (`AuthTokenLayer`). `.header(...)` is
native-only: it does not exist on wasm, so misuse is a compile error rather
than a runtime surprise.

> **Warning — query-string tokens leak.** A token on the connection URL
> (`?token=...`) ends up in reverse-proxy/CDN access logs, browser history,
> and any `Referer` header sent from that page — all outside your control.
> Prefer an `HttpOnly`, `SameSite` session cookie for the WebSocket upgrade
> (paired with `OriginLayer`, see [Security](#security)), or send the token
> in-band as per-RPC metadata once connected (`AuthTokenLayer`, see
> [Authentication](#authentication)) instead of putting it on the URL.

## How it works

```
tonic client ──► Channel ──► [session] ──► codec ──► WebSocket ──► axum ──► serve ──► tonic Routes
    stubs       tower svc     seq/ack      Frame       native:       bridge into the full
   as-is      auto-reconnect  replay      envelope   tungstenite     tonic middleware stack
                              dedup       (proto)    wasm: web-sys
```

- **Envelope protocol** (`protocols/slozhn/v1/frame.proto`): gRPC semantics
  reified as proto frames — `Open/Headers/Message/HalfClose/Status/Cancel` —
  multiplexed by `stream_id` over one socket, with h2-style credit flow
  control, `GoAway`, and session frames.
- **Session layer** (spec §8): seq/ack, a bounded replay buffer, and dedup —
  streams survive physical disconnects; a rejected resume honestly fails
  RPCs with `UNAVAILABLE`, and the reconnect wrapper builds a fresh session.
- **Wire specification**: [`docs/protocol.md`](docs/protocol.md) — normative,
  language-neutral, backed by a conformance suite.
- Full design document: `docs/superpowers/specs/2026-07-08-slozhn-design.md`.

## Crates

| Crate | What it is |
|---|---|
| `slozhn` | Facade: `client::builder`, `server::{grpc_ws, grpc_ws_session}`, `testing` |
| `slozhn-frame` | Envelope + stream-multiplexing state machine (portable core) |
| `slozhn-ws` | WebSocket byte duplex: tungstenite (native) / web-sys (wasm) |
| `slozhn-client` | tower channel for tonic stubs + reconnect |
| `slozhn-server` | WS → tonic `Routes` bridge (axum) + `grpc_proxy` |
| `slozhn-session` | Resume layer: seq/ack/replay |
| `slozhn-middleware` | tower layers: trace, metrics, retry, auth, rate limit, dedup, validate, deadline, origin, recovery |
| `slozhn-proto` | Generated types (easyp, committed) |

## Compatibility

- **MSRV**: Rust 1.88 (edition 2024 + let-chains).
- **Features** (all additive): `client` (default), `server`, `middleware`,
  `otel` (W3C traceparent + `metrics`→OTel bridge), `validate` (PGV message
  validation), `testing` (in-process harness, dev-only).
  `validate` needs **`protoc` on the build machine** — it pulls
  `prost-validate-types`, whose `build.rs` compiles `validate.proto`.
- **Semver**: public error enums, `ConnState` and `GoAwayCode` are
  `#[non_exhaustive]` — match them with a `_` arm. slozhn re-exports tonic's
  surface (`Status`, `Code`, `body::Body`), so a tonic major bump implies one
  here.
- **Wire protocol**: version 1, specified in
  [`docs/protocol.md`](docs/protocol.md) and pinned by the conformance suite
  (`crates/slozhn-frame/tests/conformance.rs`) — an implementer in another
  language can build against it without reading the Rust.

## Examples

| | |
|---|---|
| `examples/echo-ws` | Native e2e: server+client bins, network tests (reconnect, resume) |
| `examples/echo-wasm` | Browser test suite (`./run-browser-tests.sh`, headless chrome) |
| `examples/browser-app` | Live demo: TS UI (Vite) + Rust wasm core; survives server restarts |


## Reconnect state & control

The channel exposes its reconnect machinery:

```rust
let channel = slozhn::client::builder(url)
    .reconnect_config(slozhn_client::reconnect::AutoConfig {
        initial_backoff: Duration::from_millis(200),
        max_backoff: Duration::from_secs(10),
    })
    .keepalive_config(Some(slozhn_client::reconnect::KeepaliveConfig {
        interval: Duration::from_secs(30),
        timeout: Duration::from_secs(10),
    }))
    .build();

let mut state = channel.state(); // tokio watch: Idle/Connecting/Backoff{delay,attempt}/Connected/Disconnected
channel.reconnect_now();         // punch through a backoff wait (a "retry now" button)
```

For stream resume, enable the session layer. Its reconnect backoff inherits
`reconnect_config`; use `.resume_with(SessionConfig{..})` to control it
separately.

```rust
let channel = slozhn::client::builder(url)
    .reconnect_config(slozhn_client::reconnect::AutoConfig {
        initial_backoff: Duration::from_millis(200),
        max_backoff: Duration::from_secs(10),
    })
    .resume()   // session backoff inherits reconnect_config;
                // .resume_with(SessionConfig{..}) to control it separately
    .build();
```

Backoff is exponential with equal jitter (default 100 ms → 5 s). `Backoff`
carries the chosen delay and the attempt number — render your own countdown
from receipt time (wasm has no portable clock, so no absolute deadline is
exposed).

Raw channels also send Ping/Pong keepalives by default (`30s` interval,
`10s` timeout). Call `.keepalive_config(None)` to disable it. Session channels
do not use logical keepalive pings during reconnect gaps; the session transport
publishes reconnect state and resumes the logical connection itself.

## Testing in one process (feature `testing`)

The slozhn analogue of gRPC's `bufconn`: a real tonic client talking to real
`tonic::Routes` over an in-memory transport — one runtime, no sockets, no
ports, no axum. Milliseconds instead of hundreds of them, and no flaky
port binding.

```toml
[dev-dependencies]
slozhn = { version = "1", features = ["testing"] }
```

```rust
let routes = tonic::service::Routes::new(EchoServer::new(MyEcho));
let mut client = EchoClient::new(slozhn::testing::channel(routes));
let resp = client.unary(Request::new(msg)).await?;   // ordinary tonic
```

Three fidelity levels — pick the cheapest that still exercises what you test:

| Function | What runs for real | Use for |
|---|---|---|
| `channel(routes)` | envelope state machine, flow control, streams, metadata, trailers, middleware | service logic, middleware |
| `channel_over_bytes(routes)` | + the byte codec (the literal `[]byte` transport) | encoding, framing, size limits |
| `session_channel(routes)` | + session layer: seq/ack, replay, resume | reconnect and stream-resume behavior |

`session_channel` returns a `Breaker` that severs the physical transport on
demand — the client reconnects through the in-memory factory and resumes,
so a mid-stream network break is testable without touching the network.
`Breaker::connect_count()` proves a break actually happened and recovered
(instead of a test passing because nothing was ever severed):

```rust
let (channel, breaker) = slozhn::testing::session_channel(routes).await;
let mut stream = client.server_stream(req).await?.into_inner();
stream.next().await;          // first message arrives
breaker.kill();               // network dies mid-stream
while let Some(m) = stream.next().await { m?; }   // resumed transparently
assert_eq!(breaker.connect_count(), 2);
```

## Performance

`cargo bench -p slozhn-frame` (criterion; figures below from an Intel
i5-14600K, in-memory loopback — no sockets, so they bound the protocol
machinery, not your network):

| Benchmark | Result |
|---|---|
| Frame codec, 1 KiB message encode+decode | ~61 ns (~15.6 GiB/s) |
| Full stack echo, 1 KiB (open → send → echo → recv) | ~3.8 µs/call |
| 100 × 4 KiB burst down one stream (flow control engaged) | ~84 µs (~4.5 GiB/s) |

## Graceful server drain

Keep a `ConnectionRegistry` next to your axum server and route through the
`*_with_registry` helpers. On process shutdown, drain live WS connections
before stopping the listener:

```rust
let registry = slozhn::server::ConnectionRegistry::new();
let app = axum::Router::new().route(
    "/rpc",
    slozhn::server::grpc_ws_session_with_registry(routes, manager, registry.clone()),
);

registry.drain_all(slozhn::frame::GoAwayCode::Graceful);
```

### Limits & observability

Every resource axis is capped by default — a peer cannot force unbounded
work or memory:

| Limit | Default | Over the limit |
|---|---|---|
| `frame::Config.max_streams` | 1024 / connection | inbound `Open` → `RESOURCE_EXHAUSTED`; local open → `OpenError::LimitExceeded` |
| `frame::Config.max_metadata_bytes` / `max_metadata_entries` | 16 KiB / 128 | `Open`/`Headers` rejected stream-level (`RESOURCE_EXHAUSTED`) |
| `frame::Config.handshake_timeout` | 10 s | pre-Hello silence → connection dropped (slowloris guard) |
| `MAX_MESSAGE_SIZE` | 4 MiB | rejected on both send and receive; the WS transport is capped to match |
| receive flow-control window | 64 KiB | a peer ignoring its credit → `FlowControlViolation`, connection closed |
| `ServerSessionConfig.max_sessions` | 10 000 | new sessions get a `resume_rejected` Hello; resumes of existing sessions always pass |
| `ConnectionRegistry::with_max_connections(n)` | uncapped | new WS upgrades rejected with GoAway |

For metrics, poll `SessionManager::session_count()` and
`ConnectionRegistry::len()`, or wire the `metrics` facade (see below) —
`slozhn_sessions_active`, `slozhn_ws_connections_active`,
`slozhn_reconnects_total`, `slozhn_session_resume_total` are emitted from
the transport itself.

gRPC server reflection and `grpc.health.v1.Health` are ordinary tonic
services and work over the bridge unmodified — add `tonic-reflection` /
`tonic-health` to your app and register them in the same `Routes`
(see `router_full()` in `examples/echo-ws`).

## Gateway: deploy business logic without dropping clients

Long-lived WS connections shouldn't die every time business logic
redeploys. Split into two tiers: a thin gateway (slozhn WS + sessions,
rarely deployed) that proxies every gRPC call over regular HTTP/2 to an
upstream app tier (a plain tonic server, deploys freely):

```
 browser/client                gateway tier               app tier
┌──────────────┐   WS/slozhn  ┌───────────────┐  HTTP/2   ┌──────────────┐
│ slozhn client│─────────────▶│ grpc_ws +      │──────────▶│ tonic server │
│ (EchoClient) │◀─────────────│ GrpcProxy      │◀──────────│ (redeploys)  │
└──────────────┘  survives    └───────────────┘  Channel   └──────────────┘
                  app deploys   (rarely deploys) connect_lazy
```

`grpc_ws` accepts any tower `Service`, so `grpc_proxy` is a ready-made one
that forwards to a `tonic::transport::Channel`:

```rust
let upstream = tonic::transport::Channel::from_shared(format!("http://{app_addr}"))?
    .connect_lazy();
let proxy = slozhn::server::grpc_proxy(upstream);
let app = axum::Router::new().route("/rpc", slozhn::server::grpc_ws(proxy));
```

WS connections and sessions live in the thin gateway, which rarely
redeploys; the app tier restarts freely behind it. An app-tier restart
surfaces to in-flight calls as `UNAVAILABLE` on that single RPC — the WS
channel itself is untouched — and `RetryLayer` + `DedupLayer` (see
Middleware below) already recover that exactly-once for idempotent
methods, so a redeploy is invisible to the caller beyond one retried call.

## Middleware (feature `middleware`)

Ready-made tower layers, usable on both sides:

```rust
// client: logging/tracing + descriptor-driven unary retries
let stack = tower::ServiceBuilder::new()
    .layer(slozhn::middleware::TraceLayer::client())
    .layer(slozhn::middleware::RetryLayer::from_file_descriptor_set(
        my_proto::FILE_DESCRIPTOR_SET,
    )?)
    .service(channel);
let client = EchoClient::new(stack);

// server: same tracing around the tonic routes
let traced = slozhn::middleware::trace_server(routes);
Router::new().route("/rpc", slozhn::server::grpc_ws(traced));
```

`TraceLayer` emits a `tracing` span per RPC with OTel semconv fields
(`rpc.system/service/method`, `rpc.grpc.status_code`) plus start/finish
events — hook up `tracing_subscriber::fmt` for logs or `tracing-opentelemetry`
for traces; the `otel` feature adds W3C `traceparent` propagation.
`RetryLayer::default()` is deny-by-default. It retries unary calls on
`UNAVAILABLE` only when the protobuf descriptor marks the RPC
`NO_SIDE_EFFECTS`/`IDEMPOTENT`, or when you explicitly allow a method:

```rust
let retry = slozhn::middleware::RetryLayer::from_file_descriptor_set(
    my_proto::FILE_DESCRIPTOR_SET,
)?
.retry_method("/legacy.v1.Legacy/Get")
.never_retry_method("/legacy.v1.Legacy/Create");
```

Retried calls use jittered exponential backoff (clamped to `max_backoff`,
30 s by default — also what keeps the exponent computation from overflowing
at very high `max_attempts`) and buffer request bodies up to 256 KiB;
streaming requests are never replayed. A retried RPC may execute twice
server-side — use `unsafe_retry_all_buffered()` only when the wrapped client is
already scoped to idempotent methods.

### Descriptor-driven idempotency & retries

Mark methods with the STANDARD protobuf option — no custom extensions:

```protobuf
rpc Upsert(Req) returns (Resp) { option idempotency_level = IDEMPOTENT; }
rpc Get(Req) returns (Resp)    { option idempotency_level = NO_SIDE_EFFECTS; }
```

With `file_descriptor_set: true` in the prost generator options, the embedded
`FILE_DESCRIPTOR_SET` drives the client stack automatically:

```rust
let fds = my_proto::FILE_DESCRIPTOR_SET;
let idx = Arc::new(IdempotencyIndex::from_descriptor_set(fds)?);

let stack = tower::ServiceBuilder::new()
    .layer(IdempotencyKeyLayer::new(idx))               // x-idempotency-key on IDEMPOTENT calls
    .layer(RetryLayer::from_file_descriptor_set(fds)?)  // retries ONLY marked methods
    .service(channel);
```

`RetryLayer` is safe-by-default: without an allow source it retries nothing;
explicit `never_retry_method(...)` entries win over descriptors. The index
takes manual markers in the same builder style — `idempotent_method(...)`,
`no_side_effects_methods([...])`, `unknown_method(...)` (overrides a
descriptor marker) — for methods you can't annotate.

Server-side, `DedupLayer` (non-wasm) completes the story: a replay carrying
the same `x-idempotency-key` gets back the cached response (any terminal
outcome, bodies up to 256 KiB, 300 s TTL by default) instead of re-executing
the handler:

```rust
let deduped = slozhn::middleware::DedupLayer::default().layer(routes);
Router::new().route("/rpc", slozhn::server::grpc_ws(deduped));
```

Entries live in a pluggable `DedupStore` (default: per-process
`InMemoryDedupStore`). Behind a load balancer pass a shared implementation
via `.store(...)` — `CachedResponse` is plain data (status, header pairs,
bytes) so it serializes trivially to Redis (`SET key blob EX ttl` / `GET`).
Store errors fail open: the handler runs, nothing is cached. Concurrent
same-key requests on one node single-flight: only the first executes, the
rest wait and read the cache (cross-node duplicates may still race — that
would need store-side locking).

### Scaling out

Every stateful server layer takes an external store, so a multi-node
deployment only has to share it: `DedupLayer::store(...)` and
`RateLimitLayer::store(...)` accept `Arc<dyn DedupStore>` /
`Arc<dyn RateLimitStore>`, and with a shared store the guarantees hold
fleet-wide (covered by `examples/echo-ws/tests/ws_multinode_e2e.rs`). The
remaining layers are stateless per call — Trace/otel (`traceparent` even
correlates spans across nodes), Auth, and the client-side
Retry/Idempotency/Timeout — so they scale without coordination. The one
sticky thing is the **session layer** (`grpc_ws_session`): resume state is
per-process, so a balancer must pin a client's reconnects to the node that
holds its session (or resume is honestly rejected and the client starts a
fresh session).

Note the session id itself travels inside protocol frames, invisible to the
balancer — pin by a proxy attribute instead. With Caddy, a sticky cookie
(browsers attach cookies to WS upgrade requests automatically):

```caddyfile
example.com {
    reverse_proxy /rpc node1:8080 node2:8080 {
        lb_policy cookie slozhn_node
    }
}
```

For non-browser clients that don't persist cookies, use
`lb_policy client_ip_hash` instead. Getting unpinned is not fatal either
way: the resume is rejected, `AutoChannel` starts a fresh session, and the
retry + shared-store dedup layers recover idempotent calls exactly once.

### Deadlines

`TimeoutLayer` races each call against a local timer and sets the standard
`grpc-timeout` header (unless the caller already did) so the server enforces
the same deadline. On expiry the pending RPC is canceled and the call fails
with `TimeoutError::Elapsed`. Place it above `RetryLayer` if the deadline
should cover all attempts; timeouts are never retried.

```rust
let stack = tower::ServiceBuilder::new()
    .layer(slozhn::middleware::TimeoutLayer::new(Duration::from_secs(10)))
    .layer(slozhn::middleware::RetryLayer::from_file_descriptor_set(fds)?)
    .service(channel);
```

Server-side, `DeadlineLayer` (non-wasm) enforces the received `grpc-timeout`
— native tonic transport does this, a bare WS bridge would not: the handler
is cancelled on expiry (trailers-only `DEADLINE_EXCEEDED`), and a streaming
response that outlives the deadline is cut off mid-stream the same way.
`.max(...)` bounds worst-case handler runtime regardless of what callers ask
for:

```rust
let deadlined = slozhn::middleware::DeadlineLayer::new()
    .max(Duration::from_secs(30))
    .layer(routes);
```

### Metrics

`MetricsLayer::client()` / `::server()` emit through the exporter-agnostic
[`metrics`](https://docs.rs/metrics) facade — wire any exporter
(`metrics-exporter-prometheus`, ...) at process start:

- `slozhn_rpc_started_total{side, method}` — counter;
- `slozhn_rpc_inflight{side}` — gauge, leak-proof decrement on any exit;
- `slozhn_rpc_duration_seconds{side, method, code}` — histogram, `code` is
  the grpc-status (or `"error"` for transport failures).

The transport and middleware layers also emit through the same facade
(no layer needed — they fire at the source):

- `slozhn_reconnects_total{outcome}` — client connection attempts (ok/fail);
- `slozhn_session_resume_total{outcome}` — resume handshakes (ok/rejected/error);
- `slozhn_sessions_active`, `slozhn_ws_connections_active` — server gauges;
- `slozhn_sessions_rejected_total` — `max_sessions` cap hits;
- `slozhn_rate_limited_total{method}`, `slozhn_auth_rejected_total{code}`,
  `slozhn_dedup_hits_total`, `slozhn_retries_total{method}`,
  `slozhn_deadline_exceeded_total{stage}` — middleware events.

Without an installed `metrics` recorder all of this is a no-op.

To ship them through OpenTelemetry instead of a Prometheus scrape, the
`otel` feature provides a recorder bridging the whole `metrics` facade
(these series and any of your own) into an OTel `Meter`:

```rust
let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
    .with_reader(/* OTLP exporter / Prometheus reader */)
    .build();
metrics::set_global_recorder(
    slozhn::middleware::otel_metrics_recorder(provider.meter("slozhn")),
)?;
```

### Load shedding

The layers keep the tower readiness contract (reserved `poll_ready`
capacity is never leaked), so standard tower limiters compose directly.
Server-side, cap in-flight RPCs per node (`grpc_ws` needs an infallible
service, so use back-pressure via `concurrency_limit` rather than
`load_shed`, which changes the error type):

```rust
let limited = tower::limit::ConcurrencyLimitLayer::new(1024)
    .layer(slozhn::middleware::trace_server(routes));
```

Client-side, `load_shed` works too — tonic stubs map the shed error through
`Status::from_error`:

```rust
let stack = tower::ServiceBuilder::new()
    .load_shed()
    .concurrency_limit(256)
    .service(channel);
```

### Compression

The bridge carries opaque length-prefixed gRPC message bytes (the 5-byte
prefix already encodes the compressed-flag), and metadata/headers travel
inside protocol frames — so tonic's standard per-message gzip compression
works over the WS transport unchanged, no bridge-side support needed. Enable
the `gzip` feature on `tonic` in your app's `Cargo.toml`, then use the usual
codegen builder methods:

```rust
// server
let echo = EchoServer::new(EchoImpl)
    .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
    .send_compressed(tonic::codec::CompressionEncoding::Gzip);
Router::new().route("/rpc", slozhn::server::grpc_ws(tonic::service::Routes::new(echo)));

// client
let mut client = EchoClient::new(channel)
    .send_compressed(tonic::codec::CompressionEncoding::Gzip)
    .accept_compressed(tonic::codec::CompressionEncoding::Gzip);
```

### Validation

`ValidateLayer` checks request messages against protoc-gen-validate rules
BEFORE the handler runs — every message of every streaming shape, on the
server (inbound) or on the client (outbound, fail-fast). With the `validate`
feature, one line covers all methods of all services via descriptors — no
per-method registration:

```rust
let v = slozhn::middleware::ValidateLayer::from_descriptor_sets([
    my_proto::validate::FILE_DESCRIPTOR_SET,   // dependencies first
    my_proto::shop::v1::FILE_DESCRIPTOR_SET,
])?
// the caster owns the response entirely — code, message, details:
.caster(|method, violations: Vec<prost_validate::Error>| {
    let domain = my_domain_error(&violations);         // your error proto
    tonic::Status::with_details(
        Code::InvalidArgument,
        "validation failed",
        domain.encode_to_vec().into(),                 // → grpc-status-details-bin
    )
});
Router::new().route("/rpc", slozhn::server::grpc_ws(v.layer(routes)));
```

Overrides on top of the reflective default: `.message::<M>(path)` uses `M`'s
derived `prost_validate::Validator` (no reflection, ~10× faster — for hot
methods), `.method(path, |m: &M| ...)` for rules PGV can't express (no
optional dependencies needed). Compressed messages are not validated;
malformed protobuf is left to the tonic codec. `prost-validate` is
fail-fast today, so the caster receives one violation per failing message —
the `Vec` signature is ready for a validate-all upstream.

Every message's declared length is capped at `.max_message_bytes(n)`
(`DEFAULT_MAX_MESSAGE_BYTES`, 4 MiB, if unset). A peer whose length prefix
declares more than the cap fails the body immediately with
`RESOURCE_EXHAUSTED` — the layer never buffers anywhere near the declared
size trying to reassemble a message that's over the limit.

### Rate limiting

`RateLimitLayer` (server, non-wasm) enforces GCRA quotas — precise sustained
rate with configurable instant burst — per method × caller key. Over-limit
calls get `RESOURCE_EXHAUSTED` with `retry-after` metadata (seconds):

```rust
let limited = slozhn::middleware::RateLimitLayer::new(Quota::per_second(50))
    .method_quota("/shop.v1.Shop/Search", Quota::per_second(200).burst(400))
    .unlimited_method("/grpc.health.v1.Health/Check")
    .key_by_header("authorization")   // bucket per caller, "anon" if absent
    .layer(routes);
Router::new().route("/rpc", slozhn::server::grpc_ws(limited));
```

State lives in a pluggable `RateLimitStore`. The built-in `InMemoryStore` is
per-process; behind a load balancer implement the trait over a shared store
(with Redis: redis-cell's `CL.THROTTLE`, or a small Lua compare-and-set on
the stored arrival time — the decision must be atomic in the store). Store
errors fail open by default (availability over strictness); `.fail_closed()`
turns them into `UNAVAILABLE`.

> **Key on verified identity, not raw headers.** `.key_by_header(...)` (and
> `.key_by`) key on whatever the peer sent — a caller can rotate a header to
> get a fresh bucket and defeat per-caller limiting. When `AuthLayer` runs
> upstream, key on its verified `Identity<T>` instead:
> ```rust
> let limited = slozhn::middleware::RateLimitLayer::new(Quota::per_second(50))
>     .key_by_identity::<UserId, _>(|id| id.to_string())
>     .layer(auth_secured_routes); // RateLimitLayer wraps routes that AuthLayer already ran on
> ```
> `.key_by_request(...)` is the general escape hatch if the key needs more
> than just the identity (headers, uri, and extensions together).

Middleware here rate-limits at the RPC layer — it has no visibility into
protocol control frames (`Ping`, `WindowUpdate`, rapid open/cancel churn)
handled by `slozhn-frame`/the connection driver below it. Flooding at that
level isn't bounded by `RateLimitLayer`; it belongs at the connection/proxy
layer (a reverse proxy's connection-rate limits, or frame-level throttling
in the transport itself), which is out of scope for this crate.

### Authentication

Modeled on go-grpc-middleware's auth interceptor. Server: an async `AuthFn`
runs before the service — reject with a gRPC status or inject an identity
that handlers read from request extensions. Client: a token layer adds
`authorization` metadata per call. Metadata travels inside protocol frames
(not WS upgrade headers), so this works from browsers unchanged.

```rust
// server
let auth: AuthFn<UserId> = Arc::new(|headers, _uri| Box::pin(async move {
    match slozhn::middleware::bearer(headers) {
        Some(token) => verify(token).await.map_err(|_| AuthError::unauthenticated("bad token")),
        None => Err(AuthError::unauthenticated("token required")),
    }
}));
let secured = AuthLayer::new(auth).layer(routes);
Router::new().route("/rpc", slozhn::server::grpc_ws(secured));
// handlers: request.extensions().get::<Identity<UserId>>()

// client (native and wasm)
let stack = AuthTokenLayer::bearer(|| current_token()).layer(channel);
let client = EchoClient::new(stack);
```

## Security

Deployment hardening notes for a browser-facing bridge — read this alongside
[Middleware](#middleware-feature-middleware) before going to production.

### Origin checking (CSWSH defense)

Browsers do **not** enforce same-origin for WebSocket connections and run no
CORS preflight on the upgrade request — any page can open a WS connection to
your server and have the browser attach ambient credentials (cookies) to it.
If the server accepts cookie-based auth, that is a hijack primitive unless
the server checks `Origin` itself. `OriginLayer` does that check; wrap it
around the routes before `grpc_ws`/`grpc_ws_session`, above every other
layer:

```rust
let checked = slozhn::middleware::OriginLayer::new(["https://app.example.com"])
    .layer(routes);
Router::new().route("/rpc", slozhn::server::grpc_ws(checked));
```

Exact-match allowlist by default; a request with no `Origin` header is
rejected unless you call `.allow_missing_origin()` (native clients that never
send one). `OriginLayer::allow_any()` disables the check entirely — an
explicit, documented escape hatch for deployments that are never reachable
from a browser. A mismatch returns a trailers-only `PERMISSION_DENIED`
(code 7), same shape as `AuthLayer`'s rejection.

### TLS

`slozhn` does not terminate TLS itself — it speaks plain `ws://`/HTTP over
whatever axum listener you give it. In production, put it behind a reverse
proxy or `axum-server` configured with a certificate and expose `wss://` to
clients; the bridge only sees decrypted traffic either way. Serving `ws://`
across an untrusted network exposes bearer tokens, cookies, and RPC payloads
to on-path observers — treat plaintext WebSocket as a local/dev-only setup.

### Identity-bound rate limiting

`RateLimitLayer.key_by_header(...)` keys on peer-controlled data by
default — see [Rate limiting](#rate-limiting) for `.key_by_identity(...)`,
which binds buckets to the verified `Identity<T>` `AuthLayer` places in
request extensions instead, so rotating a header can't buy a fresh quota.

### Protocol control frames are out of scope here

`RateLimitLayer` and `DedupLayer` operate per-RPC, above the frame layer —
they cannot see or bound `Ping`/`WindowUpdate` floods or rapid open/cancel
churn at the `slozhn-frame`/connection level. That protection belongs at the
connection or reverse-proxy layer (connection-rate limits, frame-level
throttling in the transport), not in this middleware stack.

### Panic recovery

A panic inside a handler unwinds straight through tower/tonic by default and
kills whatever task drives the connection — on the WS bridge that tears down
every other in-flight RPC sharing that connection, not just the one that
panicked. `RecoveryLayer` catches it and returns a trailers-only `INTERNAL`
(code 13) response instead:

```rust
let recovered = slozhn::middleware::RecoveryLayer::new().layer(routes);
Router::new().route("/rpc", slozhn::server::grpc_ws(recovered));
```

Every recovered panic logs via `tracing::error!` and increments
`slozhn_panics_recovered_total`. Place it as the outermost layer (or at
least outside anything whose own state could be left inconsistent by an
unwind) so a bug in one handler degrades to a single failed RPC instead of a
dropped connection.

### Logging in the browser

`tracing` has no default output in wasm — route it to the devtools console
with a wasm subscriber (see `examples/browser-app`):

```rust
// once at startup, in your wasm entry point:
tracing_wasm::set_as_global_default();          // tracing-wasm = "0.2"
console_error_panic_hook::set_once();           // panics → console too

// then every TraceLayer'ed RPC logs "rpc started" / "rpc finished code=N"
let stack = slozhn::middleware::TraceLayer::client().layer(channel);
```

On native the same events go to whatever `tracing_subscriber` you install
(`tracing_subscriber::fmt::init()` for plain stdout logs).

## Developing this repo

```bash
make test          # cargo test + clippy (native)
make test-wasm     # wasm32 build + clippy
make test-browser  # browser e2e (wasm-pack, headless chrome)
make gen           # regen crates/slozhn-proto after .proto edits
make release       # interactive tag-driven release to crates.io
```

Releases are tag-driven: `make release` bumps one version across the cargo
workspace, commits, tags `vX.Y.Z`, and pushes; `.github/workflows/release.yml`
re-runs CI, creates a GitHub release, and publishes the crates to crates.io
(secret: `CARGO_REGISTRY_TOKEN`).

## Status

The v1 network core, the middleware suite, the gateway proxy and the
in-process test harness are complete; the wire protocol is specified and
conformance-tested.

Deferred: a typed proto boundary for wasm (WIT replacement, spec §9),
Kotlin/Swift bindings (the client core is FFI-ready — platform-agnostic
transport and executor injection), server→client RPC, protocol-frame rate
limiting (belongs at the proxy/L4 layer, see Security).

## License

MIT
