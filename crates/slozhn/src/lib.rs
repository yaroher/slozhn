//! slozhn — gRPC over WebSocket for native and wasm: all 4 streaming kinds,
//! metadata/trailers/statuses, auto-reconnect and transparent stream resume.
//!
//! Client (native and browser — the same code):
//! ```ignore
//! let channel = slozhn::client::builder("ws://host/rpc").resume().build();
//! let mut client = EchoClient::new(channel); // an ordinary tonic client
//! ```
//!
//! Server (axum, feature `server`):
//! ```ignore
//! let routes = tonic::service::Routes::new(EchoServer::new(MyImpl));
//! Router::new().route("/rpc", slozhn::server::grpc_ws(routes));
//! ```

#[cfg(feature = "client")]
pub mod client;
#[cfg(all(feature = "server", not(target_arch = "wasm32")))]
pub mod server;

pub use slozhn_frame as frame;
pub use slozhn_session::{SessionConfig, SessionError};
