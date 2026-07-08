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
