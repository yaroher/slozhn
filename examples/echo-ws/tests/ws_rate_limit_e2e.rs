//! Rate limit middleware over the real stack: burst passes, the over-limit
//! call gets RESOURCE_EXHAUSTED with retry-after metadata, buckets split per
//! caller key carried in protocol-frame metadata (browser-compatible).

use std::time::Duration;

use bytes::Bytes;
use slozhn::middleware::{Quota, RateLimitLayer};
use slozhn_proto::testing::v1::echo_client::EchoClient;
use slozhn_proto::testing::v1::Msg;
use tonic::Request;
use tower::Layer as _;

async fn start_server(layer: RateLimitLayer) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let routes = tonic::service::Routes::new(
            slozhn_proto::testing::v1::echo_server::EchoServer::new(echo_ws::EchoImpl),
        );
        let limited = layer.layer(routes);
        let app = axum::Router::new().route("/rpc", slozhn::server::grpc_ws(limited));
        let _ = axum::serve(listener, app).await;
    });
    addr
}

fn msg() -> Request<Msg> {
    Request::new(Msg { payload: Bytes::from_static(b"x") })
}

#[tokio::test]
async fn burst_passes_then_resource_exhausted() {
    let addr = start_server(RateLimitLayer::new(Quota::per_minute(60).burst(2))).await;
    let channel = slozhn::client::builder(format!("ws://{addr}/rpc")).build();
    let mut client = EchoClient::new(channel);

    client.unary(msg()).await.unwrap();
    client.unary(msg()).await.unwrap();

    let err = client.unary(msg()).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    assert_eq!(err.message(), "rate limit exceeded");
    let retry_after: u64 = err
        .metadata()
        .get("retry-after")
        .expect("retry-after metadata on rejection")
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!(retry_after >= 1);
}

#[tokio::test]
async fn buckets_split_by_metadata_key() {
    let addr = start_server(
        RateLimitLayer::new(Quota::per_minute(60).burst(1)).key_by_header("x-api-key"),
    )
    .await;
    let channel = slozhn::client::builder(format!("ws://{addr}/rpc")).build();
    let mut client = EchoClient::new(channel);

    let keyed = |key: &str| {
        let mut r = msg();
        r.metadata_mut().insert("x-api-key", key.parse().unwrap());
        r
    };

    client.unary(keyed("alice")).await.unwrap();
    client.unary(keyed("bob")).await.unwrap(); // separate bucket

    let err = client.unary(keyed("alice")).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);
}

#[tokio::test]
async fn slot_replenishes() {
    // 10/sec, burst 1 → a fresh slot every 100ms
    let addr = start_server(RateLimitLayer::new(Quota::per_second(10).burst(1))).await;
    let channel = slozhn::client::builder(format!("ws://{addr}/rpc")).build();
    let mut client = EchoClient::new(channel);

    client.unary(msg()).await.unwrap();
    let err = client.unary(msg()).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);

    tokio::time::sleep(Duration::from_millis(150)).await;
    client.unary(msg()).await.unwrap();
}
