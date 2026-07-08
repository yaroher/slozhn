//! Byte duplex over WebSocket. The transport knows nothing about Frame (spec §4)
//! — the `slozhn_frame::codec::framed` adapter is layered on from outside.
//! Backend per target: native — tokio-tungstenite; wasm32 — web-sys WebSocket
//! living entirely inside the spawn_local glue (JS only at the edge, spec §6).

#[derive(Default)]
pub struct WsConfig {
    /// Upgrade-request headers (auth etc.). Unavailable in the browser —
    /// non-empty headers on wasm yield `WsError::Unsupported`.
    pub headers: http::HeaderMap,
}

#[derive(Debug, thiserror::Error)]
pub enum WsError {
    #[error("invalid url: {0}")]
    Url(String),
    #[error("websocket connect failed: {0}")]
    Connect(String),
    #[error("unsupported on this platform: {0}")]
    Unsupported(&'static str),
}

#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(not(target_arch = "wasm32"))]
pub use native::{connect, WsStream};

#[cfg(target_arch = "wasm32")]
mod wasm;
#[cfg(target_arch = "wasm32")]
pub use wasm::{connect, WsStream};
