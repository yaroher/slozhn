//! Mapping envelope ↔ the HTTP layer of gRPC (for slozhn-client / slozhn-server).
//!
//! Pure types only (`http`, `http-body`) — wasm-compatible; tonic is not
//! pulled in here. `-bin` metadata at the HTTP level is already base64 →
//! travels as ascii.

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::Frame;

use crate::connection::RecvHalf;
use crate::proto::v1::{metadata_entry, Metadata, MetadataEntry, Status};
use crate::stream::StreamEvent;

#[derive(Debug, thiserror::Error)]
pub enum BodyError {
    #[error("stream cancelled")]
    Cancelled,
    #[error("connection closed")]
    Closed,
}

pub fn headers_to_metadata(headers: &http::HeaderMap) -> Metadata {
    let mut entries = Vec::new();
    for (name, value) in headers {
        if let Ok(v) = value.to_str() {
            entries.push(MetadataEntry {
                key: name.as_str().to_owned(),
                value: Some(metadata_entry::Value::Ascii(v.to_owned())),
            });
        }
    }
    Metadata { entries }
}

pub fn metadata_to_headers(md: &Metadata) -> http::HeaderMap {
    let mut headers = http::HeaderMap::new();
    for e in &md.entries {
        let Some(metadata_entry::Value::Ascii(v)) = &e.value else { continue };
        let (Ok(name), Ok(value)) = (
            e.key.parse::<http::header::HeaderName>(),
            http::header::HeaderValue::from_str(v),
        ) else {
            continue; // skip pairs invalid for HTTP
        };
        headers.append(name, value);
    }
    headers
}

pub fn status_to_trailers(status: &Status) -> http::HeaderMap {
    let empty = Metadata { entries: Vec::new() };
    let mut t = metadata_to_headers(status.trailers.as_ref().unwrap_or(&empty));
    t.insert("grpc-status", status.code.to_string().parse().expect("digits"));
    if !status.message.is_empty()
        && let Ok(v) = http::header::HeaderValue::from_str(&status.message)
    {
        t.insert("grpc-message", v);
    }
    t
}

pub fn trailers_to_status(trailers: &http::HeaderMap) -> Status {
    let code = trailers
        .get("grpc-status")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(2); // INTERNAL when absent
    let message = trailers
        .get("grpc-message")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let mut md_headers = trailers.clone();
    md_headers.remove("grpc-status");
    md_headers.remove("grpc-message");
    Status { code, message, trailers: Some(headers_to_metadata(&md_headers)) }
}

enum Mode {
    /// Server side: request body, end = RemoteHalfClose.
    Request,
    /// Client side: response body, end = Status → trailers frame.
    Response,
}

/// `http_body::Body` on top of [`RecvHalf`].
pub struct RecvBody {
    recv: Option<RecvHalf>,
    mode: Mode,
}

impl RecvBody {
    pub fn request(recv: RecvHalf) -> Self {
        Self { recv: Some(recv), mode: Mode::Request }
    }

    pub fn response(recv: RecvHalf) -> Self {
        Self { recv: Some(recv), mode: Mode::Response }
    }

    /// Empty finished body (trailers-only responses).
    pub fn finished() -> Self {
        Self { recv: None, mode: Mode::Response }
    }
}

impl http_body::Body for RecvBody {
    type Data = Bytes;
    type Error = BodyError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BodyError>>> {
        loop {
            let Some(recv) = self.recv.as_mut() else { return Poll::Ready(None) };
            match std::task::ready!(recv.poll_next_event(cx)) {
                Some(StreamEvent::Message(b)) => return Poll::Ready(Some(Ok(Frame::data(b)))),
                Some(StreamEvent::Headers(_)) => continue, // consumed higher up the stack
                Some(StreamEvent::RemoteHalfClose) => match self.mode {
                    Mode::Request => {
                        self.recv = None; // end of request body
                        return Poll::Ready(None);
                    }
                    Mode::Response => continue, // waiting for Status
                },
                Some(StreamEvent::Terminated(st)) => match self.mode {
                    Mode::Response => {
                        self.recv = None;
                        return Poll::Ready(Some(Ok(Frame::trailers(status_to_trailers(&st)))));
                    }
                    Mode::Request => {
                        self.recv = None;
                        return Poll::Ready(Some(Err(BodyError::Cancelled)));
                    }
                },
                Some(StreamEvent::Cancelled) => {
                    self.recv = None;
                    return Poll::Ready(Some(Err(BodyError::Cancelled)));
                }
                None => {
                    self.recv = None;
                    return match self.mode {
                        // connection died without a Status — honest UNAVAILABLE
                        Mode::Response => {
                            let st = crate::ext::StatusExt::with_code(14, "connection lost");
                            Poll::Ready(Some(Ok(Frame::trailers(status_to_trailers(&st)))))
                        }
                        Mode::Request => Poll::Ready(None),
                    };
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ext::StatusExt;

    #[test]
    fn status_trailers_roundtrip() {
        let mut st = Status::with_code(3, "bad arg");
        st.trailers = Some(Metadata {
            entries: vec![MetadataEntry {
                key: "x-extra".into(),
                value: Some(metadata_entry::Value::Ascii("v".into())),
            }],
        });
        let t = status_to_trailers(&st);
        let back = trailers_to_status(&t);
        assert_eq!(back.code, 3);
        assert_eq!(back.message, "bad arg");
        assert!(back.trailers.unwrap().entries.iter().any(|e| e.key == "x-extra"));
    }

    #[test]
    fn headers_metadata_roundtrip() {
        let mut h = http::HeaderMap::new();
        h.insert("x-a", "1".parse().unwrap());
        let md = headers_to_metadata(&h);
        let back = metadata_to_headers(&md);
        assert_eq!(back.get("x-a").unwrap(), "1");
    }

    #[test]
    fn missing_grpc_status_is_internal() {
        let t = http::HeaderMap::new();
        assert_eq!(trailers_to_status(&t).code, 2);
    }
}
