//! tonic-compatible channel over slozhn-frame: implements
//! `tower::Service<http::Request<tonic::body::Body>>` — the `GrpcService`
//! seam that generated tonic clients plug into unchanged.
//!
//! No spawning of its own: the executor is injected via [`Spawner`]
//! (`tokio::spawn` on native, a `spawn_local` wrapper in wasm).

pub mod reconnect;

use std::sync::Arc;
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use http_body_util::BodyExt;
use slozhn_frame::http::{headers_to_metadata, metadata_to_headers, status_to_trailers, RecvBody};
use slozhn_frame::stream::StreamEvent;
use slozhn_frame::Connection;

pub type Spawner = Arc<dyn Fn(BoxFuture<'static, ()>) + Send + Sync>;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ClientError {
    #[error("open failed: {0}")]
    Open(#[from] slozhn_frame::OpenError),
    #[error("connection closed")]
    Closed,
}

/// Synthetic trailers-only UNAVAILABLE response — the connection died before/
/// during the RPC (spec §7: disconnect without a session layer = honest UNAVAILABLE).
pub(crate) fn unavailable_response() -> http::Response<RecvBody> {
    let st = slozhn_frame::proto::v1::Status {
        code: 14,
        message: "connection lost".into(),
        trailers: None,
    };
    let mut builder = http::Response::builder().status(200);
    let headers = builder.headers_mut().expect("builder");
    *headers = status_to_trailers(&st);
    headers.insert("content-type", "application/grpc".parse().unwrap());
    builder.body(RecvBody::finished()).expect("valid response")
}

#[derive(Clone)]
pub struct Channel {
    conn: Connection,
    spawner: Spawner,
}

impl Channel {
    pub fn new(conn: Connection, spawner: Spawner) -> Self {
        Self { conn, spawner }
    }
}

impl tower::Service<http::Request<tonic::body::Body>> for Channel {
    type Response = http::Response<RecvBody>;
    type Error = ClientError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<tonic::body::Body>) -> Self::Future {
        let conn = self.conn.clone();
        let spawner = self.spawner.clone();
        Box::pin(async move {
            let path = req.uri().path().to_owned();
            let metadata = headers_to_metadata(req.headers());
            let (send, mut recv) = conn.open(path, metadata).await?;

            // pump request body → Message*/HalfClose, concurrently with the response
            let mut body = req.into_body();
            spawner(Box::pin(async move {
                loop {
                    match body.frame().await {
                        Some(Ok(f)) => {
                            // a gRPC client sends no trailers — data only
                            if let Ok(data) = f.into_data()
                                && send.send(data).await.is_err()
                            {
                                return; // stream died — the terminal event already happened
                            }
                        }
                        Some(Err(_)) => {
                            // body error — explicitly cancel so the peer is
                            // told the RPC is dead now, instead of relying
                            // on drop semantics (SendHalf's Drop is a no-op).
                            send.cancel();
                            return;
                        }
                        None => break,
                    }
                }
                let _ = send.half_close().await;
            }));

            // wait for Headers or an early terminal (trailers-only)
            loop {
                match recv.next_event().await {
                    Some(StreamEvent::Headers(md)) => {
                        let mut builder = http::Response::builder().status(200);
                        let headers = builder.headers_mut().expect("builder");
                        *headers = metadata_to_headers(&md);
                        headers.insert("content-type", "application/grpc".parse().unwrap());
                        return Ok(builder
                            .body(RecvBody::response(recv))
                            .expect("valid response"));
                    }
                    Some(StreamEvent::Terminated(st)) => {
                        // trailers-only: status in headers, tonic understands this
                        let mut builder = http::Response::builder().status(200);
                        let headers = builder.headers_mut().expect("builder");
                        *headers = status_to_trailers(&st);
                        headers.insert("content-type", "application/grpc".parse().unwrap());
                        return Ok(builder.body(RecvBody::finished()).expect("valid response"));
                    }
                    Some(StreamEvent::Cancelled) | None => return Ok(unavailable_response()),
                    Some(_) => continue, // RemoteHalfClose before Status — skip
                }
            }
        })
    }
}
