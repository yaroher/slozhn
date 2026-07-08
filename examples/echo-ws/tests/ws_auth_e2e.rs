//! Auth middleware over the real stack: metadata travels inside protocol
//! frames, so the same flow works from browsers (no WS headers involved).

use std::sync::Arc;

use bytes::Bytes;
use slozhn::middleware::{bearer, AuthError, AuthFn, AuthLayer, AuthTokenLayer};
use slozhn_proto::testing::v1::echo_client::EchoClient;
use slozhn_proto::testing::v1::Msg;
use tonic::Request;
use tower::Layer as _;

async fn start_auth_server() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let auth: AuthFn<String> = Arc::new(|headers, _uri| {
        let token = bearer(headers).map(str::to_owned);
        Box::pin(async move {
            match token.as_deref() {
                Some("secret-token") => Ok("user-1".to_string()),
                _ => Err(AuthError::unauthenticated("token required")),
            }
        })
    });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let routes = tonic::service::Routes::new(
            slozhn_proto::testing::v1::echo_server::EchoServer::new(echo_ws::EchoImpl),
        );
        let secured = AuthLayer::new(auth).layer(routes);
        let app = axum::Router::new().route("/rpc", slozhn::server::grpc_ws(secured));
        let _ = axum::serve(listener, app).await;
    });
    (addr, handle)
}

#[tokio::test]
async fn unauthenticated_without_token() {
    let (addr, _srv) = start_auth_server().await;
    let channel = slozhn::client::builder(format!("ws://{addr}/rpc")).build();
    let mut client = EchoClient::new(channel);

    let err = client
        .unary(Request::new(Msg { payload: Bytes::from_static(b"x") }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    assert_eq!(err.message(), "token required");
}

#[tokio::test]
async fn bearer_token_passes() {
    let (addr, _srv) = start_auth_server().await;
    let channel = slozhn::client::builder(format!("ws://{addr}/rpc")).build();
    let stack = AuthTokenLayer::bearer(|| "secret-token".to_string()).layer(channel);
    let mut client = EchoClient::new(stack);

    let resp = client
        .unary(Request::new(Msg { payload: Bytes::from_static(b"hi") }))
        .await
        .unwrap();
    assert_eq!(resp.into_inner().payload.as_ref(), b"hi");
}
