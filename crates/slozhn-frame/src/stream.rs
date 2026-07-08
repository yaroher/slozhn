use bytes::Bytes;

use crate::error::ProtocolError;
use crate::flow::Window;
use crate::proto::v1::{Message, Metadata, Status};
use crate::MAX_MESSAGE_SIZE;

#[derive(Debug, PartialEq)]
pub enum StreamEvent {
    Headers(Metadata),
    Message(Bytes),
    RemoteHalfClose,
    Terminated(Status),
    Cancelled,
}

/// State of a single stream as seen from the local side. Sans-io: knows
/// nothing about channels or transport, only transition validation + windows.
///
/// After a terminal state (Status/Cancel), incoming frames are not an error —
/// a legal race (the peer hasn't seen the terminal yet); the connection drops
/// them via `is_closed()`.
#[derive(Debug)]
pub struct StreamState {
    is_opener: bool,
    local_half_closed: bool,
    remote_half_closed: bool,
    headers_seen: bool,
    terminated: bool,
    pub send_window: Window,
    pub recv_window: Window,
}

impl StreamState {
    pub fn new(is_opener: bool, send_window: u32, recv_window: u32) -> Self {
        Self {
            is_opener,
            local_half_closed: false,
            remote_half_closed: false,
            headers_seen: false,
            terminated: false,
            send_window: Window::new(send_window),
            recv_window: Window::new(recv_window),
        }
    }

    pub fn is_terminated(&self) -> bool {
        self.terminated
    }

    pub fn is_closed(&self) -> bool {
        self.terminated || (self.local_half_closed && self.remote_half_closed)
    }

    pub fn on_headers(&mut self, metadata: Metadata) -> Result<StreamEvent, ProtocolError> {
        if self.terminated {
            return Ok(StreamEvent::RemoteHalfClose);
        }
        if !self.is_opener || self.headers_seen {
            return Err(ProtocolError::UnexpectedHeaders(0));
        }
        self.headers_seen = true;
        Ok(StreamEvent::Headers(metadata))
    }

    pub fn on_message(&mut self, stream_id: u64, msg: Message) -> Result<StreamEvent, ProtocolError> {
        if msg.payload.len() > MAX_MESSAGE_SIZE {
            return Err(ProtocolError::MessageTooLarge(stream_id));
        }
        if self.terminated {
            return Ok(StreamEvent::RemoteHalfClose);
        }
        if self.remote_half_closed {
            return Err(ProtocolError::AfterHalfClose(stream_id));
        }
        self.recv_window.consume(msg.payload.len());
        Ok(StreamEvent::Message(msg.payload))
    }

    pub fn on_half_close(&mut self) -> Result<StreamEvent, ProtocolError> {
        if !self.terminated {
            self.remote_half_closed = true;
        }
        Ok(StreamEvent::RemoteHalfClose)
    }

    pub fn on_status(&mut self, status: Status) -> StreamEvent {
        self.terminated = true;
        self.local_half_closed = true;
        self.remote_half_closed = true;
        StreamEvent::Terminated(status)
    }

    pub fn on_cancel(&mut self) -> StreamEvent {
        self.terminated = true;
        self.local_half_closed = true;
        self.remote_half_closed = true;
        StreamEvent::Cancelled
    }

    pub fn on_window_update(&mut self, increment: u32) -> Result<(), ProtocolError> {
        self.send_window.credit(increment)
    }

    pub fn local_half_close(&mut self) {
        self.local_half_closed = true;
    }

    pub fn local_terminate(&mut self) {
        self.terminated = true;
        self.local_half_closed = true;
        self.remote_half_closed = true;
    }

    pub fn can_send(&self) -> bool {
        !self.terminated && !self.local_half_closed && self.send_window.can_send()
    }

    pub fn consume_send(&mut self, n: usize) {
        self.send_window.consume(n);
    }

    pub fn credit_recv(&mut self, n: u32) -> Result<(), ProtocolError> {
        self.recv_window.credit(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ext::{MetadataExt, StatusExt};
    use crate::proto::v1::{Message, Metadata, Status};
    use crate::error::ProtocolError;
    use crate::{DEFAULT_WINDOW, MAX_MESSAGE_SIZE};

    fn msg(n: usize) -> Message {
        Message { payload: bytes::Bytes::from(vec![0u8; n]), compressed: false }
    }

    #[test]
    fn unary_happy_path_opener_view() {
        // opener: send → half_close; recv: headers, message, status
        let mut s = StreamState::new(true, DEFAULT_WINDOW, DEFAULT_WINDOW);
        s.consume_send(5);
        s.local_half_close();
        assert!(matches!(s.on_headers(Metadata::empty()).unwrap(), StreamEvent::Headers(_)));
        assert!(matches!(s.on_message(1, msg(3)).unwrap(), StreamEvent::Message(_)));
        assert!(matches!(s.on_status(Status::ok()), StreamEvent::Terminated(_)));
        assert!(s.is_closed());
    }

    #[test]
    fn message_after_remote_half_close_is_protocol_error() {
        let mut s = StreamState::new(false, DEFAULT_WINDOW, DEFAULT_WINDOW);
        s.on_half_close().unwrap();
        assert_eq!(s.on_message(1, msg(1)), Err(ProtocolError::AfterHalfClose(1)));
    }

    #[test]
    fn acceptor_state_rejects_headers() {
        let mut s = StreamState::new(false, DEFAULT_WINDOW, DEFAULT_WINDOW);
        assert_eq!(
            s.on_headers(Metadata::empty()),
            Err(ProtocolError::UnexpectedHeaders(0))
        );
    }

    #[test]
    fn duplicate_headers_rejected() {
        let mut s = StreamState::new(true, DEFAULT_WINDOW, DEFAULT_WINDOW);
        s.on_headers(Metadata::empty()).unwrap();
        assert_eq!(
            s.on_headers(Metadata::empty()),
            Err(ProtocolError::UnexpectedHeaders(0))
        );
    }

    #[test]
    fn oversized_message_is_protocol_error() {
        let mut s = StreamState::new(true, DEFAULT_WINDOW, DEFAULT_WINDOW);
        assert_eq!(
            s.on_message(9, msg(MAX_MESSAGE_SIZE + 1)),
            Err(ProtocolError::MessageTooLarge(9))
        );
    }

    #[test]
    fn terminal_status_wins_over_later_frames() {
        let mut s = StreamState::new(true, DEFAULT_WINDOW, DEFAULT_WINDOW);
        s.on_status(Status::with_code(13, "boom"));
        assert!(s.is_closed());
        // after a terminal, frames are ignored, not an error (Cancel/Status race)
        assert!(matches!(s.on_cancel(), StreamEvent::Cancelled));
        assert!(matches!(s.on_message(1, msg(1)), Ok(StreamEvent::RemoteHalfClose)));
    }

    #[test]
    fn cannot_send_after_local_half_close() {
        let mut s = StreamState::new(true, DEFAULT_WINDOW, DEFAULT_WINDOW);
        assert!(s.can_send());
        s.local_half_close();
        assert!(!s.can_send());
    }
}
