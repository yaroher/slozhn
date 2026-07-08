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
      opts: { bytes: "." }
    - name: tonic
      out: gen/src
      opts: { no_transport: "true" }   # keep wasm builds possible
```

```bash
easyp generate
```

### 2. Server (axum)

```toml
slozhn = { version = "0.1", features = ["client", "server"] }
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
auth via query parameters or cookies (`.header(...)` is native-only and
panics at build time on wasm).

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
- Full design document: `docs/superpowers/specs/2026-07-08-slozhn-design.md`.

## Crates

| Crate | What it is |
|---|---|
| `slozhn` | Facade: `client::builder`, `server::{grpc_ws, grpc_ws_session}` |
| `slozhn-frame` | Envelope + stream-multiplexing state machine (portable core) |
| `slozhn-ws` | WebSocket byte duplex: tungstenite (native) / web-sys (wasm) |
| `slozhn-client` | tower channel for tonic stubs + reconnect |
| `slozhn-server` | WS → tonic `Routes` bridge (axum) |
| `slozhn-session` | Resume layer: seq/ack/replay |
| `slozhn-proto` | Generated types (easyp, committed) |

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

## Middleware (feature `middleware`)

Ready-made tower layers, usable on both sides:

```rust
// client: logging/tracing + unary retries
let stack = tower::ServiceBuilder::new()
    .layer(slozhn::middleware::TraceLayer::client())
    .layer(slozhn::middleware::RetryLayer::default())
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
`RetryLayer` retries unary calls on `UNAVAILABLE` with jittered backoff,
buffering bodies up to 256 KiB; streaming requests are never replayed.
Retried calls may execute twice server-side — enable for idempotent methods.

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

The v1 network core is complete. Deferred: a typed proto boundary for wasm
(WIT replacement, spec §9), Kotlin/Swift bindings, tracing/middleware,
server→client RPC.

## License

MIT
