//! Multi-node scaling of the stateful server middleware: two independent
//! server instances (separate listeners = separate "nodes" behind a
//! balancer) share one store, so dedup and rate limiting hold globally, not
//! per instance. The shared `InMemoryDedupStore`/`InMemoryStore` stand in
//! for Redis here — the middleware only sees the trait.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use slozhn::middleware::{DedupLayer, DedupStore, InMemoryDedupStore, InMemoryStore, Quota, RateLimitLayer};
use slozhn_proto::testing::v1::echo_client::EchoClient;
use slozhn_proto::testing::v1::echo_server::{Echo, EchoServer};
use slozhn_proto::testing::v1::{Count, Msg};
use tonic::{Request, Response, Status, Streaming};
use tower::Layer;

/// Echo that counts unary executions — proves dedup skipped the handler.
struct CountingEcho(Arc<AtomicU32>);

type MsgStream =
    std::pin::Pin<Box<dyn futures::Stream<Item = Result<Msg, Status>> + Send>>;

#[tonic::async_trait]
impl Echo for CountingEcho {
    async fn unary(&self, req: Request<Msg>) -> Result<Response<Msg>, Status> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(req.into_inner()))
    }

    type ServerStreamStream = MsgStream;
    async fn server_stream(&self, _: Request<Count>) -> Result<Response<MsgStream>, Status> {
        Err(Status::unimplemented("not under test"))
    }
    async fn client_stream(&self, _: Request<Streaming<Msg>>) -> Result<Response<Msg>, Status> {
        Err(Status::unimplemented("not under test"))
    }
    type BidiStream = MsgStream;
    async fn bidi(&self, _: Request<Streaming<Msg>>) -> Result<Response<MsgStream>, Status> {
        Err(Status::unimplemented("not under test"))
    }
    async fn fail(&self, _: Request<Msg>) -> Result<Response<Msg>, Status> {
        Err(Status::unimplemented("not under test"))
    }
}

async fn start_node<L, S>(layer: L, echo: CountingEcho) -> std::net::SocketAddr
where
    L: FnOnce(tonic::service::Routes) -> S,
    S: tower::Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
            Error = std::convert::Infallible,
            Future: Send,
        > + Clone
        + Send
        + Sync
        + 'static,
{
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let routes = tonic::service::Routes::new(EchoServer::new(echo));
    let svc = layer(routes);
    tokio::spawn(async move {
        let app = axum::Router::new().route("/rpc", slozhn::server::grpc_ws(svc));
        let _ = axum::serve(listener, app).await;
    });
    addr
}

fn client(addr: std::net::SocketAddr) -> EchoClient<slozhn::client::Channel> {
    EchoClient::new(slozhn::client::builder(format!("ws://{addr}/rpc")).build())
}

fn keyed(key: &str) -> Request<Msg> {
    let mut r = Request::new(Msg { payload: Bytes::from_static(b"pay") });
    r.metadata_mut()
        .insert("x-idempotency-key", key.parse().unwrap());
    r
}

#[tokio::test]
async fn dedup_holds_across_nodes_with_shared_store() {
    let store: Arc<dyn DedupStore> = Arc::new(InMemoryDedupStore::new(100));
    let calls = Arc::new(AtomicU32::new(0));

    let a = {
        let store = store.clone();
        start_node(move |r| DedupLayer::default().store(store).layer(r), CountingEcho(calls.clone()))
            .await
    };
    let b = start_node(
        move |r| DedupLayer::default().store(store).layer(r),
        CountingEcho(calls.clone()),
    )
    .await;

    let r1 = client(a).unary(keyed("k-1")).await.unwrap().into_inner();
    // replay lands on the OTHER node — must come from the shared cache
    let r2 = client(b).unary(keyed("k-1")).await.unwrap().into_inner();

    assert_eq!(r1.payload, r2.payload);
    assert_eq!(calls.load(Ordering::SeqCst), 1, "handler must run once across the fleet");

    // a different key executes normally
    client(b).unary(keyed("k-2")).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn rate_limit_holds_across_nodes_with_shared_store() {
    let store = Arc::new(InMemoryStore::default());
    let calls = Arc::new(AtomicU32::new(0));

    let quota = Quota::per_minute(60).burst(2);
    let a = {
        let store = store.clone();
        start_node(
            move |r| RateLimitLayer::new(quota).store(store).layer(r),
            CountingEcho(calls.clone()),
        )
        .await
    };
    let b = start_node(
        move |r| RateLimitLayer::new(quota).store(store).layer(r),
        CountingEcho(calls),
    )
    .await;

    let msg = || Request::new(Msg { payload: Bytes::from_static(b"x") });

    // burst of 2 split across the two nodes...
    client(a).unary(msg()).await.unwrap();
    client(b).unary(msg()).await.unwrap();

    // ...and the third call is over the GLOBAL limit no matter the node
    let err = client(a).unary(msg()).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    let err = client(b).unary(msg()).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);
}
