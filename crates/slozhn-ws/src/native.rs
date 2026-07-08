//! Native backend: tokio-tungstenite.

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::{Sink, Stream, StreamExt};
use slozhn_frame::TransportClosed;

use super::{WsConfig, WsError};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

pub async fn connect(url: &str, config: WsConfig) -> Result<WsStream, WsError> {
    let mut request = url
        .into_client_request()
        .map_err(|e| WsError::Url(e.to_string()))?;
    request.headers_mut().extend(config.headers);
    let (inner, _resp) = connect_async(request)
        .await
        .map_err(|e| WsError::Connect(e.to_string()))?;
    Ok(WsStream { inner })
}

pub struct WsStream {
    inner: WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
}

impl Stream for WsStream {
    type Item = Bytes;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Bytes>> {
        loop {
            match std::task::ready!(self.inner.poll_next_unpin(cx)) {
                Some(Ok(Message::Binary(b))) => return Poll::Ready(Some(b)),
                Some(Ok(Message::Close(_))) | None => return Poll::Ready(None),
                Some(Ok(_)) => continue, // Text/Ping/Pong — not our data
                Some(Err(_)) => return Poll::Ready(None),
            }
        }
    }
}

impl Sink<Bytes> for WsStream {
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
    use futures::SinkExt;

    #[tokio::test]
    async fn binary_roundtrip_and_headers() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // echo server that checks the upgrade-request header
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_hdr_async(
                stream,
                #[allow(clippy::result_large_err)] // tungstenite callback signature
                |req: &tokio_tungstenite::tungstenite::handshake::server::Request, resp| {
                    assert_eq!(
                        req.headers().get("x-auth").and_then(|v| v.to_str().ok()),
                        Some("secret")
                    );
                    Ok(resp)
                },
            )
            .await
            .unwrap();
            while let Some(Ok(msg)) = ws.next().await {
                if msg.is_binary() {
                    ws.send(msg).await.unwrap();
                }
            }
        });

        let mut headers = http::HeaderMap::new();
        headers.insert("x-auth", "secret".parse().unwrap());
        let mut ws = connect(&format!("ws://{addr}"), WsConfig { headers })
            .await
            .unwrap();

        ws.send(Bytes::from_static(b"ping")).await.unwrap();
        let echoed = ws.next().await.unwrap();
        assert_eq!(echoed.as_ref(), b"ping");
    }
}
