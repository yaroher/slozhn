use futures::channel::mpsc;
use futures::{Sink, Stream};
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;

use crate::error::TransportClosed;
use crate::proto::v1::Frame;

/// Pair of connected in-memory frame transports. The channel capacity is
/// deliberately small: transport backpressure is part of the contract.
pub fn pair() -> (FramePipe, FramePipe) {
    typed_pair()
}

/// Same for raw bytes (codec/ws-layer tests).
pub fn byte_pair() -> (Pipe<Bytes>, Pipe<Bytes>) {
    typed_pair()
}

fn typed_pair<I>() -> (Pipe<I>, Pipe<I>) {
    let (a_tx, b_rx) = mpsc::channel::<I>(8);
    let (b_tx, a_rx) = mpsc::channel::<I>(8);
    (Pipe { tx: a_tx, rx: a_rx }, Pipe { tx: b_tx, rx: b_rx })
}

pub type FramePipe = Pipe<Frame>;

pub struct Pipe<I> {
    tx: mpsc::Sender<I>,
    rx: mpsc::Receiver<I>,
}

impl<I> Stream for Pipe<I> {
    type Item = I;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<I>> {
        Pin::new(&mut self.rx).poll_next(cx)
    }
}

impl<I> Sink<I> for Pipe<I> {
    type Error = TransportClosed;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.tx).poll_ready(cx).map_err(|_| TransportClosed)
    }
    fn start_send(mut self: Pin<&mut Self>, item: I) -> Result<(), Self::Error> {
        Pin::new(&mut self.tx).start_send(item).map_err(|_| TransportClosed)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.tx).poll_flush(cx).map_err(|_| TransportClosed)
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.tx).poll_close(cx).map_err(|_| TransportClosed)
    }
}
