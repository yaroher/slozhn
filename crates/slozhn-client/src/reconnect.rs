//! Reconnecting channel wrapper (spec §6–7): new calls wait for recovery
//! with exponential backoff, active RPCs on a dead connection finish with
//! UNAVAILABLE. Uses tokio::time — native-only until phase 4 (which will add
//! timer injection for wasm).

use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::future::BoxFuture;
use slozhn_frame::connection::{bind, Config};
use slozhn_frame::ids::Side;
use slozhn_frame::transport::{ConnState, ReconnectHooks};
use slozhn_frame::Connection;

use crate::{unavailable_response, Channel, ClientError, Spawner};

#[cfg(not(target_arch = "wasm32"))]
async fn sleep_backoff(d: Duration) {
    tokio::time::sleep(d).await;
}

#[cfg(target_arch = "wasm32")]
async fn sleep_backoff(d: Duration) {
    futures_timer::Delay::new(d).await;
}

pub use slozhn_frame::transport::{BoxFrameTransport, FrameDuplex};

/// What the factory returned: a raw transport (the driver does Hello itself)
/// or pre-negotiated (the session layer did the handshake, peer's Hello attached).
pub enum FactoryOutput {
    Raw(BoxFrameTransport),
    PreNegotiated(BoxFrameTransport, slozhn_frame::proto::v1::Hello),
}

pub type TransportFactory =
    Arc<dyn Fn() -> BoxFuture<'static, Result<FactoryOutput, String>> + Send + Sync>;

#[derive(Clone)]
pub struct AutoConfig {
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for AutoConfig {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
        }
    }
}

/// Channel with auto-reconnect. Clone is cheap, state is shared.
#[derive(Clone)]
pub struct AutoChannel {
    state: Arc<tokio::sync::Mutex<State>>,
    factory: TransportFactory,
    spawner: Spawner,
    config: AutoConfig,
    hooks: ReconnectHooks,
    state_rx: tokio::sync::watch::Receiver<ConnState>,
}

struct State {
    current: Option<(Connection, Channel)>,
}

impl AutoChannel {
    pub fn new(factory: TransportFactory, spawner: Spawner, config: AutoConfig) -> Self {
        let (hooks, state_rx) = ReconnectHooks::new();
        Self::with_hooks(factory, spawner, config, hooks, state_rx)
    }

    /// Like [`Self::new`], but reporting into an externally created hooks
    /// pair — the facade threads ONE pair through both this loop and the
    /// session-layer loop, so observers see a single state stream.
    pub fn with_hooks(
        factory: TransportFactory,
        spawner: Spawner,
        config: AutoConfig,
        hooks: ReconnectHooks,
        state_rx: tokio::sync::watch::Receiver<ConnState>,
    ) -> Self {
        Self {
            state: Arc::new(tokio::sync::Mutex::new(State { current: None })),
            factory,
            spawner,
            config,
            hooks,
            state_rx,
        }
    }

    /// Observable connection state (see [`ConnState`]).
    pub fn state(&self) -> tokio::sync::watch::Receiver<ConnState> {
        self.state_rx.clone()
    }

    /// Punch through a backoff wait and retry connecting immediately.
    /// Only wakes loops that are currently waiting in backoff; outside of
    /// backoff it is a no-op.
    pub fn reconnect_now(&self) {
        self.hooks.kick.notify_waiters();
    }

    /// A live channel; if the connection is dead — reconnect with backoff.
    /// The lock is held for the reconnect: concurrent calls wait right here.
    async fn ensure(&self) -> Channel {
        let mut state = self.state.lock().await;
        if let Some((conn, ch)) = &state.current
            && !conn.is_closed()
        {
            return ch.clone();
        }
        let mut base = self.config.initial_backoff;
        let mut attempt: u32 = 0;
        loop {
            self.hooks.set(ConnState::Connecting);
            match (self.factory)().await {
                Ok(output) => {
                    let (conn, driver_fut): (Connection, BoxFuture<'static, ()>) = match output {
                        FactoryOutput::Raw(t) => {
                            let (conn, driver) = bind(Side::Client, Config::default(), t);
                            (conn, Box::pin(async move {
                                let _ = driver.run().await; // disconnect is a normal outcome
                            }))
                        }
                        FactoryOutput::PreNegotiated(t, peer_hello) => {
                            let (conn, driver) = slozhn_frame::connection::bind_pre_negotiated(
                                Side::Client,
                                Config::default(),
                                peer_hello,
                                t,
                            );
                            (conn, Box::pin(async move {
                                let _ = driver.run().await;
                            }))
                        }
                    };
                    let hooks = self.hooks.clone();
                    (self.spawner)(Box::pin(async move {
                        driver_fut.await;
                        hooks.set(ConnState::Disconnected);
                    }));
                    let ch = Channel::new(conn.clone(), self.spawner.clone());
                    state.current = Some((conn, ch.clone()));
                    self.hooks.set(ConnState::Connected);
                    return ch;
                }
                Err(_) => {
                    attempt += 1;
                    let delay = slozhn_frame::transport::jittered(base);
                    self.hooks.set(ConnState::Backoff { delay, attempt });
                    let kick = self.hooks.kick.clone();
                    tokio::select! {
                        _ = sleep_backoff(delay) => {}
                        _ = kick.notified() => {} // reconnect_now() punched through
                    }
                    base = (base * 2).min(self.config.max_backoff);
                }
            }
        }
    }
}

impl tower::Service<http::Request<tonic::body::Body>> for AutoChannel {
    type Response = http::Response<slozhn_frame::http::RecvBody>;
    type Error = ClientError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<tonic::body::Body>) -> Self::Future {
        let this = self.clone();
        Box::pin(async move {
            let mut ch = this.ensure().await;
            match tower::Service::call(&mut ch, req).await {
                Ok(resp) => Ok(resp),
                // connection died between ensure and open — honest UNAVAILABLE;
                // no retry on our own: the body may have been partially sent
                Err(ClientError::Open(_)) | Err(ClientError::Closed) => {
                    Ok(unavailable_response())
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    fn failing_factory(calls: Arc<AtomicU32>) -> TransportFactory {
        Arc::new(move || {
            calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Err("nope".to_string()) })
        })
    }

    fn test_spawner() -> Spawner {
        Arc::new(|f| {
            tokio::spawn(f);
        })
    }

    #[tokio::test]
    async fn states_flow_and_kick_breaks_backoff() {
        let calls = Arc::new(AtomicU32::new(0));
        let ch = AutoChannel::new(
            failing_factory(calls.clone()),
            test_spawner(),
            AutoConfig {
                initial_backoff: Duration::from_secs(60), // без kick тест бы завис
                max_backoff: Duration::from_secs(60),
            },
        );
        let mut state = ch.state();
        assert_eq!(*state.borrow(), slozhn_frame::transport::ConnState::Idle);

        // ensure() крутится в фоне (через фиктивный вызов канала)
        let ch2 = ch.clone();
        tokio::spawn(async move {
            let _ = ch2.ensure().await; // никогда не завершится — factory всегда Err
        });

        // Connecting → Backoff{attempt: 1}
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                state.changed().await.unwrap();
                if matches!(
                    *state.borrow(),
                    slozhn_frame::transport::ConnState::Backoff { attempt: 1, .. }
                ) {
                    break;
                }
            }
        })
        .await
        .expect("reached first backoff");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // без kick сидели бы 60 секунд; kick → вторая попытка мгновенно
        ch.reconnect_now();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                state.changed().await.unwrap();
                if matches!(
                    *state.borrow(),
                    slozhn_frame::transport::ConnState::Backoff { attempt: 2, .. }
                ) {
                    break;
                }
            }
        })
        .await
        .expect("kick must break the backoff");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
