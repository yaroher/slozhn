//! Phase 3 demo and e2e: tonic Echo service behind an axum WS endpoint + a
//! client with auto-reconnect. Binaries: `--bin server`, `--bin client`.

use std::pin::Pin;

use bytes::Bytes;
use futures::Stream;
use slozhn_proto::testing::v1::echo_server::{Echo, EchoServer};
use slozhn_proto::testing::v1::{Count, Msg};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

pub struct EchoImpl;

type MsgStream = Pin<Box<dyn Stream<Item = Result<Msg, Status>> + Send>>;

#[tonic::async_trait]
impl Echo for EchoImpl {
    async fn unary(&self, req: Request<Msg>) -> Result<Response<Msg>, Status> {
        let echo_meta = req.metadata().get("x-echo").cloned();
        let mut resp = Response::new(req.into_inner());
        if let Some(v) = echo_meta {
            resp.metadata_mut().insert("x-echo-back", v);
        }
        Ok(resp)
    }

    type ServerStreamStream = MsgStream;
    async fn server_stream(&self, req: Request<Count>) -> Result<Response<MsgStream>, Status> {
        let n = req.into_inner().n;
        let s =
            tokio_stream::iter((0..n).map(|i| Ok(Msg { payload: Bytes::from(vec![i as u8]) })));
        Ok(Response::new(Box::pin(s)))
    }

    async fn client_stream(&self, req: Request<Streaming<Msg>>) -> Result<Response<Msg>, Status> {
        let mut inbound = req.into_inner();
        let mut sum: u64 = 0;
        while let Some(m) = inbound.next().await {
            sum += u64::from(m?.payload[0]);
        }
        Ok(Response::new(Msg { payload: Bytes::copy_from_slice(&sum.to_le_bytes()) }))
    }

    type BidiStream = MsgStream;
    async fn bidi(&self, req: Request<Streaming<Msg>>) -> Result<Response<MsgStream>, Status> {
        let mut inbound = req.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            while let Some(m) = inbound.next().await {
                let Ok(m) = m else { break };
                let n = u64::from_le_bytes(m.payload.as_ref().try_into().unwrap());
                if tx
                    .send(Ok(Msg { payload: Bytes::copy_from_slice(&(n * 2).to_le_bytes()) }))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn fail(&self, _req: Request<Msg>) -> Result<Response<Msg>, Status> {
        let mut st = Status::invalid_argument("deliberate failure");
        st.metadata_mut().insert("x-fail-detail", "why-not".parse().unwrap());
        Err(st)
    }
}

/// Server router: gRPC-over-WS on `/rpc`.
pub fn router() -> axum::Router {
    let routes = tonic::service::Routes::new(EchoServer::new(EchoImpl));
    axum::Router::new().route("/rpc", slozhn::server::grpc_ws(routes))
}


/// Channel with auto-reconnect (no resume: active RPCs fail on a break).
pub fn auto_channel(url: String) -> slozhn::client::Channel {
    slozhn::client::builder(url).build()
}

/// Channel with the session layer: streams survive breaks; if resume is
/// rejected, the reconnect wrapper creates a fresh session.
pub fn session_channel(url: String) -> slozhn::client::Channel {
    slozhn::client::builder(url).resume().build()
}

/// Server router with the session layer: streams survive breaks.
pub fn router_session() -> axum::Router {
    let routes = tonic::service::Routes::new(EchoServer::new(EchoImpl));
    let manager = slozhn::server::SessionManager::new(Default::default());
    axum::Router::new().route("/rpc", slozhn::server::grpc_ws_session(routes, manager))
}
