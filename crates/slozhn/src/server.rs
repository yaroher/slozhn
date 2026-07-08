//! Server entry point (native): axum routes + session manager.

pub use slozhn_server::serve;
pub use slozhn_server::ws::{grpc_ws, grpc_ws_session};
pub use slozhn_session::server::{ServerSessionConfig, SessionManager};
