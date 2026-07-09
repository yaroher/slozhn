//! Infra e2e: gRPC server reflection, health checking, and session/connection
//! metrics — all proven over the real WS bridge (`echo_ws::router_full` /
//! `router_session_with_registry`), not just in-process.

use bytes::Bytes;
use slozhn_proto::testing::v1::echo_client::EchoClient;
use slozhn_proto::testing::v1::Msg;
use tokio_stream::StreamExt;
use tonic::Request;
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::HealthCheckRequest;
use tonic_reflection::pb::v1::server_reflection_client::ServerReflectionClient;
use tonic_reflection::pb::v1::server_reflection_request::MessageRequest;
use tonic_reflection::pb::v1::server_reflection_response::MessageResponse;
use tonic_reflection::pb::v1::ServerReflectionRequest;

async fn start_full_server() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, echo_ws::router_full()).await;
    });
    (addr, handle)
}

#[tokio::test]
async fn reflection_lists_echo_service_over_ws() {
    let (addr, _srv) = start_full_server().await;
    let channel = slozhn::client::builder(format!("ws://{addr}/rpc")).build();
    let mut client = ServerReflectionClient::new(channel);

    let req = ServerReflectionRequest {
        host: String::new(),
        message_request: Some(MessageRequest::ListServices(String::new())),
    };
    let mut responses = client
        .server_reflection_info(tokio_stream::once(req))
        .await
        .unwrap()
        .into_inner();

    let resp = responses.next().await.unwrap().unwrap();
    match resp.message_response {
        Some(MessageResponse::ListServicesResponse(list)) => {
            assert!(
                list.service.iter().any(|s| s.name == "testing.v1.Echo"),
                "expected testing.v1.Echo in {:?}",
                list.service
            );
        }
        other => panic!("unexpected reflection response: {other:?}"),
    }
}

#[tokio::test]
async fn health_check_reports_serving_and_not_found() {
    let (addr, _srv) = start_full_server().await;
    let channel = slozhn::client::builder(format!("ws://{addr}/rpc")).build();
    let mut client = HealthClient::new(channel);

    let resp = client
        .check(HealthCheckRequest { service: "testing.v1.Echo".to_string() })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        resp.status,
        tonic_health::pb::health_check_response::ServingStatus::Serving as i32
    );

    let err = client
        .check(HealthCheckRequest { service: "no.such.Service".to_string() })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn session_and_connection_counts_after_one_call() {
    let (router, manager, registry) = echo_ws::router_session_with_registry();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _srv = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });

    let channel = slozhn::client::builder(format!("ws://{addr}/rpc")).resume().build();
    let mut client = EchoClient::new(channel);
    client
        .unary(Request::new(Msg { payload: Bytes::from_static(b"1") }))
        .await
        .unwrap();

    assert_eq!(manager.session_count(), 1);
    assert_eq!(registry.len(), 1);
}
