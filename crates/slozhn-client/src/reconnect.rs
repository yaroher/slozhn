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
}

struct State {
    current: Option<(Connection, Channel)>,
}

impl AutoChannel {
    pub fn new(factory: TransportFactory, spawner: Spawner, config: AutoConfig) -> Self {
        Self {
            state: Arc::new(tokio::sync::Mutex::new(State { current: None })),
            factory,
            spawner,
            config,
        }
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
        let mut delay = self.config.initial_backoff;
        loop {
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
                    (self.spawner)(driver_fut);
                    let ch = Channel::new(conn.clone(), self.spawner.clone());
                    state.current = Some((conn, ch.clone()));
                    return ch;
                }
                Err(_) => {
                    sleep_backoff(delay).await;
                    delay = (delay * 2).min(self.config.max_backoff);
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
