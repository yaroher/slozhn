//! Server entry point (native): axum routes + session manager.

pub use slozhn_server::serve;
pub use slozhn_server::ws::{
    ConnectionRegistry, grpc_ws, grpc_ws_session, grpc_ws_session_with_registry,
    grpc_ws_with_registry,
};
pub use slozhn_server::{GrpcProxy, grpc_proxy};
pub use slozhn_session::server::{ServerSessionConfig, SessionManager};
