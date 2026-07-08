//! Adapter: byte duplex (WS etc.) → Frame duplex.
//! 1 binary message = 1 Frame (spec §5). A corrupt frame means a broken peer:
//! a decode error ends the stream.

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::{Sink, Stream};
use prost::Message as _;

use crate::error::TransportClosed;
use crate::proto::v1::Frame;

pub fn framed<T>(inner: T) -> Framed<T>
where
    T: Stream<Item = Bytes> + Sink<Bytes, Error = TransportClosed> + Unpin + Send,
{
    Framed { inner }
}

pub struct Framed<T> {
    inner: T,
}

impl<T> Stream for Framed<T>
where
    T: Stream<Item = Bytes> + Sink<Bytes, Error = TransportClosed> + Unpin + Send,
{
    type Item = Frame;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Frame>> {
        match std::task::ready!(Pin::new(&mut self.inner).poll_next(cx)) {
            Some(bytes) => match Frame::decode(bytes.as_ref()) {
                Ok(frame) => Poll::Ready(Some(frame)),
                Err(_) => Poll::Ready(None), // broken peer — drop the connection
            },
            None => Poll::Ready(None),
        }
    }
}

impl<T> Sink<Frame> for Framed<T>
where
    T: Stream<Item = Bytes> + Sink<Bytes, Error = TransportClosed> + Unpin + Send,
{
    type Error = TransportClosed;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_ready(cx)
    }
    fn start_send(mut self: Pin<&mut Self>, item: Frame) -> Result<(), Self::Error> {
        Pin::new(&mut self.inner).start_send(Bytes::from(item.encode_to_vec()))
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_close(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::loopback;
    use futures::{SinkExt, StreamExt};

    #[tokio::test]
    async fn frame_roundtrip_through_bytes() {
        let (a, mut b) = loopback::byte_pair();
        let mut fa = framed(a);
        let frame = Frame { stream_id: 7, seq: 0, kind: None };
        fa.send(frame.clone()).await.unwrap();
        let raw = b.next().await.unwrap();
        assert_eq!(Frame::decode(raw.as_ref()).unwrap(), frame);

        b.send(raw).await.unwrap();
        assert_eq!(fa.next().await.unwrap(), frame);
    }

    #[tokio::test]
    async fn garbage_ends_stream() {
        let (a, mut b) = loopback::byte_pair();
        let mut fa = framed(a);
        b.send(Bytes::from_static(&[0xFF, 0xFF, 0xFF, 0x01, 0x02]))
            .await
            .unwrap();
        assert!(fa.next().await.is_none());
    }
}
