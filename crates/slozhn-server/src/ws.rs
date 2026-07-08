//! Axum integration: WS endpoint → frame connection → serve() (spec §6–7).

use std::pin::Pin;
use std::task::{Context, Poll};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::routing::{any, MethodRouter};
use bytes::Bytes;
use futures::{Sink, Stream, StreamExt};
use slozhn_frame::codec::framed;
use slozhn_frame::connection::{bind, Config};
use slozhn_frame::ids::Side;
use slozhn_frame::TransportClosed;

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
    any(move |ws: WebSocketUpgrade| {
        let svc = svc.clone();
        async move {
            ws.on_upgrade(move |socket| async move {
                let transport = framed(AxumWs { inner: socket });
                let (conn, driver) = bind(Side::Server, Config::default(), transport);
                tokio::spawn(async move {
                    let _ = driver.run().await; // WS disconnect is a normal outcome
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
    use slozhn_frame::connection::bind_pre_negotiated;
    use slozhn_session::client::BoxFrameTransport;

    any(move |ws: WebSocketUpgrade| {
        let svc = svc.clone();
        let manager = manager.clone();
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
                        tokio::spawn(async move {
                            let _ = driver.run().await;
                        });
                        crate::serve(conn, svc).await;
                    }
                    // attach to a live session or a rejected resume — the socket
                    // is already handed off/closed, nothing to bring up
                    Ok(None) => {}
                    Err(_) => {} // broken handshake — drop silently
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
