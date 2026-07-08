//! Bridge: incoming slozhn-frame streams → any tower service
//! (primarily `tonic::service::Routes` — tonic's whole middleware stack
//! works as is). Native-only (spec §6): uses `tokio::spawn`.

pub mod ws;

use http_body_util::BodyExt;
use slozhn_frame::ext::StatusExt;
use slozhn_frame::http::{RecvBody, headers_to_metadata, metadata_to_headers, trailers_to_status};
use slozhn_frame::proto::v1::Status;
use slozhn_frame::{Connection, Incoming};

/// Serve the connection until it closes. Every incoming stream gets its own
/// tokio task.
pub async fn serve<S>(conn: Connection, svc: S)
where
    S: tower::Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
        > + Clone
        + Send
        + 'static,
    S::Future: Send,
    S::Error: std::fmt::Display + Send,
{
    while let Some(inc) = conn.accept().await {
        tracing::debug!(method = %inc.method, "accepted incoming slozhn stream");
        tokio::spawn(handle(inc, svc.clone()));
    }
    tracing::debug!("slozhn server connection drained");
}

async fn handle<S>(inc: Incoming, mut svc: S)
where
    S: tower::Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
        >,
    S::Error: std::fmt::Display + Send,
{
    let Incoming {
        method,
        metadata,
        send,
        recv,
    } = inc;
    tracing::debug!(%method, "handling slozhn stream");

    let mut builder = http::Request::builder()
        .method(http::Method::POST)
        .uri(&method);
    let headers = builder.headers_mut().expect("builder");
    *headers = metadata_to_headers(&metadata);
    headers.insert("content-type", "application/grpc".parse().unwrap());
    headers.insert("te", "trailers".parse().unwrap());
    let req = builder
        .body(tonic::body::Body::new(RecvBody::request(recv)))
        .expect("valid request");

    let resp = match svc.call(req).await {
        Ok(r) => r,
        Err(e) => {
            let msg = format!("service error: {e}");
            drop(e); // S::Error may be !Send — don't carry it across an await
            tracing::warn!(%method, error = %msg, "slozhn service call failed");
            let _ = send.finish(Status::with_code(13, &msg)).await;
            return;
        }
    };

    let (parts, body) = resp.into_parts();

    // trailers-only: grpc-status is already in headers
    if parts.headers.contains_key("grpc-status") {
        tracing::debug!(%method, "slozhn stream finished with trailers-only status");
        let _ = send.finish(trailers_to_status(&parts.headers)).await;
        return;
    }

    if send
        .send_headers(headers_to_metadata(&parts.headers))
        .is_err()
    {
        return;
    }

    let mut body = std::pin::pin!(body);
    loop {
        match body.frame().await {
            Some(Ok(f)) => {
                if f.is_data() {
                    let data = f.into_data().expect("checked is_data");
                    if send.send(data).await.is_err() {
                        tracing::debug!(%method, "client cancelled while sending response body");
                        return; // client cancelled
                    }
                } else if let Ok(trailers) = f.into_trailers() {
                    tracing::debug!(%method, "slozhn stream finished with response trailers");
                    let _ = send.finish(trailers_to_status(&trailers)).await;
                    return;
                }
            }
            Some(Err(_)) => {
                tracing::warn!(%method, "slozhn response body error");
                let _ = send
                    .finish(Status::with_code(13, "response body error"))
                    .await;
                return;
            }
            None => {
                // gRPC requires a terminal status; body ended without trailers
                tracing::warn!(%method, "slozhn response body ended without grpc trailers");
                let _ = send
                    .finish(Status::with_code(13, "missing grpc trailers"))
                    .await;
                return;
            }
        }
    }
}
