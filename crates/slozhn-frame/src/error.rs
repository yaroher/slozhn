use crate::proto::v1::Status;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ProtocolError {
    #[error("first frame must be HELLO")]
    ExpectedHello,
    #[error("unsupported protocol version {0}")]
    VersionMismatch(u32),
    #[error("frame for unknown stream {0}")]
    UnknownStream(u64),
    #[error("OPEN for already-open stream {0}")]
    DuplicateOpen(u64),
    #[error("OPEN with invalid stream id parity {0}")]
    InvalidParity(u64),
    #[error("data frame after half-close on stream {0}")]
    AfterHalfClose(u64),
    #[error("HEADERS sent by stream opener on stream {0}")]
    UnexpectedHeaders(u64),
    #[error("flow-control window overflow")]
    WindowOverflow,
    #[error("message exceeds MAX_MESSAGE_SIZE on stream {0}")]
    MessageTooLarge(u64),
    #[error("frame without kind")]
    EmptyFrame,
    #[error("connection-level frame kind on stream {0}")]
    ConnectionFrameOnStream(u64),
    #[error("stream-level frame kind on stream 0")]
    StreamFrameOnConnection,
    #[error("peer sent a Message on stream {0} while its receive window was already exhausted")]
    FlowControlViolation(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum GoAwayCode {
    Graceful = 0,
    ProtocolError = 1,
    Internal = 2,
}

impl GoAwayCode {
    pub fn from_u32(v: u32) -> Self {
        match v {
            0 => GoAwayCode::Graceful,
            1 => GoAwayCode::ProtocolError,
            _ => GoAwayCode::Internal,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("transport closed")]
pub struct TransportClosed;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ConnError {
    #[error("protocol violation: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("peer sent GOAWAY code={code:?}: {message}")]
    PeerGoAway { code: GoAwayCode, message: String },
    #[error(transparent)]
    TransportClosed(#[from] TransportClosed),
    #[error("peer did not complete the HELLO handshake in time")]
    HandshakeTimeout,
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum StreamError {
    #[error("stream terminated with status code {}", .0.code)]
    Status(Status),
    #[error("stream cancelled by peer")]
    Cancelled,
    #[error("connection closed: {0}")]
    Connection(String),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum OpenError {
    #[error("connection is going away")]
    GoingAway,
    #[error("connection closed: {0}")]
    Connection(String),
    #[error("per-connection stream limit exceeded")]
    LimitExceeded,
}
