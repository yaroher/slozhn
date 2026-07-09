//! Axum integration: WS endpoint → frame connection → serve() (spec §6–7).

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::routing::{MethodRouter, any};
use bytes::Bytes;
use futures::{Sink, Stream, StreamExt};
use slozhn_frame::MAX_MESSAGE_SIZE;
use slozhn_frame::codec::framed;
use slozhn_frame::connection::{Config, bind};
use slozhn_frame::error::GoAwayCode;
use slozhn_frame::ids::Side;
use slozhn_frame::{Connection, TransportClosed};

/// Slack over `slozhn_frame::MAX_MESSAGE_SIZE` for the protobuf envelope
/// overhead (headers, framing) added on top of the payload.
const WS_MESSAGE_SIZE_SLACK: usize = 64 * 1024;

/// Cap applied to `WebSocketUpgrade::max_message_size` /
/// `max_frame_size`: the frame layer rejects anything over
/// `MAX_MESSAGE_SIZE` on its own, so the WS transport only needs enough
/// headroom to let that check run instead of ballooning to
/// tokio-tungstenite's 64 MiB / 16 MiB defaults.
fn ws_max_size() -> usize {
    MAX_MESSAGE_SIZE + WS_MESSAGE_SIZE_SLACK
}

/// Apply the shared WS message/frame size caps to an upgrade.
fn capped(ws: WebSocketUpgrade) -> WebSocketUpgrade {
    let max = ws_max_size();
    ws.max_message_size(max).max_frame_size(max)
}

#[derive(Default)]
struct RegistryInner {
    state: Mutex<RegistryState>,
    max_connections: Option<usize>,
}

#[derive(Default)]
struct RegistryState {
    /// Lives under the same lock as the map: register() is the only writer
    /// and it locks anyway — a separate atomic buys nothing.
    next_id: u64,
    connections: HashMap<u64, Connection>,
    draining: Option<GoAwayCode>,
}

/// Live server-side WS connections.
///
/// Keep one registry per process and use [`Self::drain_all`] from your
/// server shutdown path before stopping the listener. Existing streams are
/// allowed to finish; new streams are rejected with GoAway.
#[derive(Clone, Default)]
pub struct ConnectionRegistry {
    inner: Arc<RegistryInner>,
}

impl ConnectionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Reject new connections once `max` are concurrently registered.
    /// Connections already accepted before the cap was reached are never
    /// evicted — only [`Self::register`] enforces the limit.
    pub fn with_max_connections(max: usize) -> Self {
        Self {
            inner: Arc::new(RegistryInner {
                state: Mutex::default(),
                max_connections: Some(max),
            }),
        }
    }

    /// Register a freshly bound connection. Returns `None` (without
    /// inserting anything — no slot is leaked) when the registry is at
    /// capacity; the caller must reject the connection.
    fn register(&self, conn: Connection) -> Option<ConnectionRegistration> {
        let (id, draining) = {
            let mut state = self.inner.state.lock().expect("connection registry lock");
            if let Some(max) = self.inner.max_connections
                && state.connections.len() >= max
            {
                tracing::warn!(
                    frame_connection_id = conn.id(),
                    max_connections = max,
                    "rejecting server ws connection: registry at capacity"
                );
                return None;
            }
            let id = state.next_id;
            state.next_id += 1;
            state.connections.insert(id, conn.clone());
            metrics::gauge!("slozhn_ws_connections_active")
                .set(state.connections.len() as f64);
            (id, state.draining)
        };
        tracing::debug!(
            registry_connection_id = id,
            frame_connection_id = conn.id(),
            draining = draining.is_some(),
            "registered server ws connection"
        );
        if let Some(code) = draining {
            conn.go_away(code);
        }
        Some(ConnectionRegistration {
            id,
            registry: self.clone(),
        })
    }

    /// Send GoAway to every currently registered connection.
    pub fn drain_all(&self, code: GoAwayCode) {
        let connections: Vec<_> = {
            let mut state = self.inner.state.lock().expect("connection registry lock");
            state.draining = Some(code);
            state.connections.values().cloned().collect()
        };
        tracing::info!(
            connections = connections.len(),
            code = ?code,
            "draining server ws connections",
        );
        for conn in connections {
            conn.go_away(code);
        }
    }

    /// Force-close every currently registered connection.
    pub fn close_all(&self) {
        let connections: Vec<_> = {
            let state = self.inner.state.lock().expect("connection registry lock");
            state.connections.values().cloned().collect()
        };
        tracing::warn!(
            connections = connections.len(),
            "closing server ws connections"
        );
        for conn in connections {
            conn.close();
        }
    }

    pub fn len(&self) -> usize {
        self.inner
            .state
            .lock()
            .expect("connection registry lock")
            .connections
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_draining(&self) -> bool {
        self.inner
            .state
            .lock()
            .expect("connection registry lock")
            .draining
            .is_some()
    }
}

struct ConnectionRegistration {
    id: u64,
    registry: ConnectionRegistry,
}

impl Drop for ConnectionRegistration {
    fn drop(&mut self) {
        let mut state = self
            .registry
            .inner
            .state
            .lock()
            .expect("connection registry lock");
        state.connections.remove(&self.id);
        metrics::gauge!("slozhn_ws_connections_active").set(state.connections.len() as f64);
        drop(state);
        tracing::debug!(
            registry_connection_id = self.id,
            "unregistered server ws connection"
        );
    }
}

/// gRPC-over-WS handler: `Router::new().route("/rpc", grpc_ws(routes))`.
pub fn grpc_ws<S>(svc: S) -> MethodRouter
where
    S: tower::Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
        > + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send,
    S::Error: std::fmt::Display + Send,
{
    grpc_ws_inner(svc, None)
}

/// gRPC-over-WS handler with an externally controlled connection registry.
pub fn grpc_ws_with_registry<S>(svc: S, registry: ConnectionRegistry) -> MethodRouter
where
    S: tower::Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
        > + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send,
    S::Error: std::fmt::Display + Send,
{
    grpc_ws_inner(svc, Some(registry))
}

fn grpc_ws_inner<S>(svc: S, registry: Option<ConnectionRegistry>) -> MethodRouter
where
    S: tower::Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
        > + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send,
    S::Error: std::fmt::Display + Send,
{
    any(move |ws: WebSocketUpgrade| {
        let svc = svc.clone();
        let registry = registry.clone();
        async move {
            capped(ws).on_upgrade(move |socket| async move {
                let transport = framed(AxumWs { inner: socket });
                let (conn, driver) = bind(Side::Server, Config::default(), transport);
                // no registry supplied → no bookkeeping cost per connection
                let registration = match &registry {
                    Some(r) => match r.register(conn.clone()) {
                        Some(registration) => Some(registration),
                        None => {
                            reject_at_capacity(conn, driver);
                            return;
                        }
                    },
                    None => None,
                };
                let _registration = registration;
                tokio::spawn(async move {
                    if let Err(error) = driver.run().await {
                        tracing::debug!(%error, "server ws connection driver stopped");
                    }
                });
                crate::serve(conn, svc).await;
            })
        }
    })
}

/// Reject a connection that was accepted but is over the registry's
/// connection cap: run the driver just long enough to flush a GoAway, then
/// tear the transport down. Never registers the connection, so no slot is
/// leaked.
fn reject_at_capacity<T>(conn: Connection, driver: slozhn_frame::ConnectionDriver<T>)
where
    T: Stream<Item = slozhn_frame::proto::v1::Frame>
        + Sink<slozhn_frame::proto::v1::Frame, Error = TransportClosed>
        + Unpin
        + Send
        + 'static,
{
    tokio::spawn(async move {
        if let Err(error) = driver.run().await {
            tracing::debug!(%error, "rejected server ws connection driver stopped");
        }
    });
    conn.go_away(GoAwayCode::Internal);
    conn.close();
}

/// gRPC-over-WS with the session layer (spec §8): streams survive disconnects.
pub fn grpc_ws_session<S>(
    svc: S,
    manager: std::sync::Arc<slozhn_session::server::SessionManager>,
) -> MethodRouter
where
    S: tower::Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
        > + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send,
    S::Error: std::fmt::Display + Send,
{
    grpc_ws_session_inner(svc, manager, None)
}

/// gRPC-over-WS with session resume and an externally controlled connection registry.
pub fn grpc_ws_session_with_registry<S>(
    svc: S,
    manager: std::sync::Arc<slozhn_session::server::SessionManager>,
    registry: ConnectionRegistry,
) -> MethodRouter
where
    S: tower::Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
        > + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send,
    S::Error: std::fmt::Display + Send,
{
    grpc_ws_session_inner(svc, manager, Some(registry))
}

fn grpc_ws_session_inner<S>(
    svc: S,
    manager: std::sync::Arc<slozhn_session::server::SessionManager>,
    registry: Option<ConnectionRegistry>,
) -> MethodRouter
where
    S: tower::Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
        > + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send,
    S::Error: std::fmt::Display + Send,
{
    use slozhn_frame::connection::bind_pre_negotiated;
    use slozhn_session::client::BoxFrameTransport;

    any(move |ws: WebSocketUpgrade| {
        let svc = svc.clone();
        let manager = manager.clone();
        let registry = registry.clone();
        async move {
            capped(ws).on_upgrade(move |socket| async move {
                let transport: BoxFrameTransport =
                    Box::pin(framed(AxumWs { inner: socket }));
                match manager.accept(transport).await {
                    Ok(Some((session_transport, client_hello))) => {
                        let (conn, driver) = bind_pre_negotiated(
                            Side::Server,
                            Config::default(),
                            client_hello,
                            session_transport,
                        );
                        let registration = match &registry {
                            Some(r) => match r.register(conn.clone()) {
                                Some(registration) => Some(registration),
                                None => {
                                    reject_at_capacity(conn, driver);
                                    return;
                                }
                            },
                            None => None,
                        };
                        let _registration = registration;
                        tokio::spawn(async move {
                            if let Err(error) = driver.run().await {
                                tracing::debug!(%error, "server session ws connection driver stopped");
                            }
                        });
                        crate::serve(conn, svc).await;
                    }
                    // attach to a live session or a rejected resume — the socket
                    // is already handed off/closed, nothing to bring up
                    Ok(None) => {}
                    Err(error) => {
                        tracing::debug!(%error, "server session ws handshake failed");
                    }
                }
            })
        }
    })
}

/// axum WebSocket → byte duplex.
struct AxumWs {
    inner: WebSocket,
}

impl Stream for AxumWs {
    type Item = Bytes;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Bytes>> {
        loop {
            match std::task::ready!(self.inner.poll_next_unpin(cx)) {
                Some(Ok(Message::Binary(b))) => return Poll::Ready(Some(b)),
                Some(Ok(Message::Close(_))) | None => return Poll::Ready(None),
                Some(Ok(_)) => continue,
                Some(Err(_)) => return Poll::Ready(None),
            }
        }
    }
}

impl Sink<Bytes> for AxumWs {
    type Error = TransportClosed;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner)
            .poll_ready(cx)
            .map_err(|_| TransportClosed)
    }
    fn start_send(mut self: Pin<&mut Self>, item: Bytes) -> Result<(), Self::Error> {
        Pin::new(&mut self.inner)
            .start_send(Message::Binary(item))
            .map_err(|_| TransportClosed)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner)
            .poll_flush(cx)
            .map_err(|_| TransportClosed)
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner)
            .poll_close(cx)
            .map_err(|_| TransportClosed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slozhn_frame::connection::bind;
    use slozhn_frame::error::OpenError;
    use slozhn_frame::ext::MetadataExt;
    use slozhn_frame::ids::Side;
    use slozhn_frame::loopback;
    use slozhn_frame::proto::v1::Metadata;

    #[tokio::test]
    async fn registry_drains_connections_registered_after_drain_started() {
        let registry = ConnectionRegistry::new();
        registry.drain_all(GoAwayCode::Graceful);
        assert!(registry.is_draining());

        let (client_transport, server_transport) = loopback::pair();
        let (client, client_driver) = bind(Side::Client, Config::default(), client_transport);
        let (server, server_driver) = bind(Side::Server, Config::default(), server_transport);
        let _registration = registry.register(server);

        tokio::spawn(async move {
            let _ = client_driver.run().await;
        });
        tokio::spawn(async move {
            let _ = server_driver.run().await;
        });

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                match client
                    .open("/svc.S/AfterDrain".into(), Metadata::empty())
                    .await
                {
                    Err(OpenError::GoingAway) => break,
                    Ok((send, _recv)) => {
                        send.cancel();
                        tokio::task::yield_now().await;
                    }
                    Err(other) => panic!("unexpected open error: {other:?}"),
                }
            }
        })
        .await
        .expect("newly registered connection should receive drain GoAway");
    }

    #[tokio::test]
    async fn registry_rejects_connections_over_max_connections() {
        let registry = ConnectionRegistry::with_max_connections(1);

        let (_client_transport_a, server_transport_a) = loopback::pair();
        let (server_a, server_driver_a) = bind(Side::Server, Config::default(), server_transport_a);
        let registration_a = registry.register(server_a);
        assert!(
            registration_a.is_some(),
            "first connection should be admitted under the cap"
        );
        tokio::spawn(async move {
            let _ = server_driver_a.run().await;
        });
        assert_eq!(registry.len(), 1);

        let (_client_transport_b, server_transport_b) = loopback::pair();
        let (server_b, _server_driver_b) = bind(Side::Server, Config::default(), server_transport_b);
        let registration_b = registry.register(server_b);
        assert!(
            registration_b.is_none(),
            "second connection should be rejected once at capacity"
        );
        // Rejection must not leak a slot into the registry.
        assert_eq!(registry.len(), 1);

        // The first connection stays registered/up while the second was rejected.
        drop(registration_a);
        assert_eq!(registry.len(), 0);
    }
}
