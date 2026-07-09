//! Shared frame-transport aliases — a single place for all layers
//! (client/session/facade).

use std::pin::Pin;

use futures::{Sink, Stream};

use crate::error::TransportClosed;
use crate::proto::v1::Frame;

pub trait FrameDuplex:
    Stream<Item = Frame> + Sink<Frame, Error = TransportClosed> + Send
{
}
impl<T> FrameDuplex for T where
    T: Stream<Item = Frame> + Sink<Frame, Error = TransportClosed> + Send
{
}

pub type BoxFrameTransport = Pin<Box<dyn FrameDuplex>>;

/// Observable state of a reconnectable transport/channel.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum ConnState {
    /// No connection yet and none in progress (lazy channel before first call).
    #[default]
    Idle,
    Connecting,
    /// Waiting `delay` before attempt `attempt` (1-based). No absolute
    /// deadline: wasm has no portable clock; UIs count down from receipt.
    Backoff {
        delay: std::time::Duration,
        attempt: u32,
    },
    Connected,
    /// Connection lost; a new call (or the session layer) will reconnect.
    Disconnected,
}

/// Reporting + control pair shared by every reconnect loop of a channel:
/// `state` publishes [`ConnState`] transitions, `kick` punches through a
/// backoff wait (see `reconnect_now`). One pair is threaded through both
/// the channel-level and the session-level loops by the facade builder.
#[derive(Clone)]
pub struct ReconnectHooks {
    pub state: tokio::sync::watch::Sender<ConnState>,
    pub kick: std::sync::Arc<tokio::sync::Notify>,
}

impl ReconnectHooks {
    /// New hooks pair + the receiver end for observers.
    pub fn new() -> (Self, tokio::sync::watch::Receiver<ConnState>) {
        let (tx, rx) = tokio::sync::watch::channel(ConnState::Idle);
        (
            Self { state: tx, kick: std::sync::Arc::new(tokio::sync::Notify::new()) },
            rx,
        )
    }

    /// Publish a state; lack of observers is not an error.
    pub fn set(&self, s: ConnState) {
        let _ = self.state.send(s);
    }
}

/// Equal jitter: half the delay fixed + half random. Keeps reconnect storms
/// apart without starving quick recovery.
pub fn jittered(base: std::time::Duration) -> std::time::Duration {
    let half = base / 2;
    half + std::time::Duration::from_millis(fastrand::u64(0..=half.as_millis() as u64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn jitter_stays_in_upper_half() {
        let base = Duration::from_millis(100);
        for _ in 0..100 {
            let d = jittered(base);
            assert!(d >= Duration::from_millis(50) && d <= Duration::from_millis(100), "{d:?}");
        }
    }

    #[test]
    fn hooks_publish_states() {
        let (hooks, rx) = ReconnectHooks::new();
        assert_eq!(*rx.borrow(), ConnState::Idle);
        hooks.set(ConnState::Connecting);
        assert_eq!(*rx.borrow(), ConnState::Connecting);
        hooks.set(ConnState::Backoff { delay: Duration::from_millis(10), attempt: 1 });
        assert!(matches!(*rx.borrow(), ConnState::Backoff { attempt: 1, .. }));
    }
}
