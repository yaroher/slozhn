//! Session layer (spec §8): transparent stream recovery across disconnects.
//! Foundations: TCP seq/ack + MQTT QoS1 + SignalR buffered reconnect.
//!
//! Layering: the session transport owns the Hello handshake and physical
//! reconnects; the frame connection is brought up via `bind_pre_negotiated`
//! and lives on top of the changing physical connections, noticing nothing.
//!
//! Known limitation: Ping/Pong is outside the session — a ping lost in a
//! disconnect hangs; ping is liveness of one physical connection, not the session.

pub mod client;
pub mod core;
#[cfg(not(target_arch = "wasm32"))]
pub mod server;

use std::time::Duration;

#[derive(Clone)]
pub struct SessionConfig {
    /// Max bytes of unacknowledged frames; overflow kills the session.
    pub replay_buffer_bytes: usize,
    /// Ack after this many received seq frames…
    pub ack_every: u32,
    /// …or this long after the first unacknowledged one.
    pub ack_delay: Duration,
    /// First reconnect backoff (jittered, doubles per attempt).
    pub initial_backoff: Duration,
    /// Backoff ceiling.
    pub max_backoff: Duration,
    /// Physical-transport keepalive: the client pings while Active and treats
    /// a missing Pong as a break (goes into reconnect); the server treats
    /// prolonged silence as a break (detaches and waits for resume).
    /// `None` disables liveness detection.
    pub keepalive_interval: Option<Duration>,
    /// How long the client waits for Pong before reconnecting.
    pub keepalive_timeout: Duration,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            replay_buffer_bytes: 1024 * 1024,
            ack_every: 16,
            ack_delay: Duration::from_millis(250),
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
            keepalive_interval: Some(Duration::from_secs(30)),
            keepalive_timeout: Duration::from_secs(10),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("replay buffer overflow — session killed (spec §8: no silent drop)")]
    BufferOverflow,
    #[error("resume rejected by peer")]
    ResumeRejected,
    #[error("handshake failed: {0}")]
    Handshake(String),
}
