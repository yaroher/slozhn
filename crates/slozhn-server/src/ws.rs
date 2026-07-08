//! Axum integration: WS endpoint → frame connection → serve() (spec §6–7).

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::routing::{MethodRouter, any};
use bytes::Bytes;
use futures::{Sink, Stream, StreamExt};
use slozhn_frame::codec::framed;
use slozhn_frame::connection::{Config, bind};
use slozhn_frame::error::GoAwayCode;
use slozhn_frame::ids::Side;
use slozhn_frame::{Connection, TransportClosed};

#[derive(Default)]
struct RegistryInner {
    next_id: AtomicU64,
    state: Mutex<RegistryState>,
}

#[derive(Default)]
struct RegistryState {
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

    fn register(&self, conn: Connection) -> ConnectionRegistration {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let draining = {
            let mut state = self.inner.state.lock().expect("connection registry lock");
            state.connections.insert(id, conn.clone());
            state.draining
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
        ConnectionRegistration {
            id,
            registry: self.clone(),
        }
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
        self.registry
            .inner
            .state
            .lock()
            .expect("connection registry lock")
            .connections
            .remove(&self.id);
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
    grpc_ws_with_registry(svc, ConnectionRegistry::new())
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
    any(move |ws: WebSocketUpgrade| {
        let svc = svc.clone();
        let registry = registry.clone();
        async move {
            ws.on_upgrade(move |socket| async move {
                let transport = framed(AxumWs { inner: socket });
                let (conn, driver) = bind(Side::Server, Config::default(), transport);
                let _registration = registry.register(conn.clone());
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
    grpc_ws_session_with_registry(svc, manager, ConnectionRegistry::new())
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
    use slozhn_frame::connection::bind_pre_negotiated;
    use slozhn_session::client::BoxFrameTransport;

    any(move |ws: WebSocketUpgrade| {
        let svc = svc.clone();
        let manager = manager.clone();
        let registry = registry.clone();
        async move {
            ws.on_upgrade(move |socket| async move {
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
                        let _registration = registry.register(conn.clone());
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
}
