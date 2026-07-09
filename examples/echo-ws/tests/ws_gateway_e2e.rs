//! Gateway pattern: a thin slozhn WS gateway proxies gRPC calls over plain
//! HTTP/2 to a real tonic app-tier server. Proves the whole point of the
//! split — the app tier can restart without dropping the WS client.

use bytes::Bytes;
use slozhn_proto::testing::v1::echo_client::EchoClient;
use slozhn_proto::testing::v1::echo_server::EchoServer;
use slozhn_proto::testing::v1::{Count, Msg};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Request;

/// Plain tonic gRPC server over TCP — the app tier.
async fn start_upstream(
    listener: tokio::net::TcpListener,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(EchoServer::new(echo_ws::EchoImpl))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    })
}

/// Gateway: slozhn WS + sessions, forwarding every call to `upstream_addr`
/// over HTTP/2 via `GrpcProxy`.
async fn start_gateway(
    upstream_addr: std::net::SocketAddr,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let channel = tonic::transport::Channel::from_shared(format!("http://{upstream_addr}"))
        .unwrap()
        .connect_lazy();
    let proxy = slozhn::server::grpc_proxy(channel);
    let app = axum::Router::new().route("/rpc", slozhn::server::grpc_ws(proxy));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (addr, handle)
}

#[tokio::test]
async fn unary_and_streaming_through_gateway() {
    let upstream_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = upstream_listener.local_addr().unwrap();
    let _upstream = start_upstream(upstream_listener).await;

    let (gateway_addr, _gateway) = start_gateway(upstream_addr).await;
    let channel = slozhn::client::builder(format!("ws://{gateway_addr}/rpc")).build();
    let mut client = EchoClient::new(channel);

    // unary roundtrip, both hops
    let resp = client
        .unary(Request::new(Msg { payload: Bytes::from_static(b"hi") }))
        .await
        .unwrap();
    assert_eq!(resp.into_inner().payload.as_ref(), b"hi");

    // server-stream
    let mut s = client
        .server_stream(Request::new(Count { n: 5 }))
        .await
        .unwrap()
        .into_inner();
    let mut got = vec![];
    while let Some(m) = s.next().await {
        got.push(m.unwrap().payload[0]);
    }
    assert_eq!(got, vec![0, 1, 2, 3, 4]);

    // error propagation: status + metadata survive both hops
    let err = client
        .fail(Request::new(Msg { payload: Bytes::new() }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(err.metadata().get("x-fail-detail").unwrap(), "why-not");
}

/// TCP proxy in front of the upstream so we can sever the gateway's HTTP/2
/// connection deterministically: killing the *tonic server task* alone does
/// not do it (accepted connections keep running in their own hyper tasks,
/// same caveat as `ws_e2e.rs`'s `start_proxy`). Dropping this proxy kills
/// the TCP connection outright, and it can be rebound on the exact same
/// front address once a fresh upstream is ready — modeling a redeploy
/// behind a stable address ("the same port" the gateway's Channel targets).
async fn start_proxy(
    backend: std::net::SocketAddr,
    front: Option<std::net::SocketAddr>,
) -> (std::net::SocketAddr, tokio::task::JoinSet<()>) {
    let bind_addr = front.unwrap_or_else(|| "127.0.0.1:0".parse().unwrap());
    // rebinding the exact same front address right after the previous
    // listener was dropped can transiently fail (TIME_WAIT et al.) — retry
    // briefly rather than flake.
    let mut listener = None;
    for _ in 0..50 {
        match tokio::net::TcpListener::bind(bind_addr).await {
            Ok(l) => {
                listener = Some(l);
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
        }
    }
    let listener = listener.expect("front address free again for rebind");
    let addr = listener.local_addr().unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn(async move {
        let mut conns = tokio::task::JoinSet::new();
        loop {
            let Ok((mut front_sock, _)) = listener.accept().await else { break };
            conns.spawn(async move {
                let Ok(mut back_sock) = tokio::net::TcpStream::connect(backend).await else {
                    return;
                };
                let _ = tokio::io::copy_bidirectional(&mut front_sock, &mut back_sock).await;
            });
        }
    });
    (addr, tasks)
}

#[tokio::test]
async fn upstream_restart_does_not_kill_ws() {
    let upstream_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = upstream_listener.local_addr().unwrap();
    let upstream = start_upstream(upstream_listener).await;
    let (front_addr, proxy) = start_proxy(upstream_addr, None).await;

    let (gateway_addr, _gateway) = start_gateway(front_addr).await;
    let channel = slozhn::client::builder(format!("ws://{gateway_addr}/rpc")).build();
    let mut client = EchoClient::new(channel);

    // call ok
    client
        .unary(Request::new(Msg { payload: Bytes::from_static(b"1") }))
        .await
        .unwrap();

    // shut down the upstream app tier (server + the connection it's serving)
    // — WS/gateway stays up
    drop(proxy);
    upstream.abort();
    let _ = upstream.await;

    // call fails, mapped to UNAVAILABLE by GrpcProxy — but the WS channel
    // itself is untouched (proven by the successful call below)
    let err = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        client.unary(Request::new(Msg { payload: Bytes::from_static(b"2") })),
    )
    .await
    .expect("call should complete, not hang")
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unavailable, "{err:?}");

    // start a NEW upstream instance, then re-front it on the SAME port the
    // gateway's Channel already targets — "deploy finished"
    let new_upstream_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let new_upstream_addr = new_upstream_listener.local_addr().unwrap();
    let _new_upstream = start_upstream(new_upstream_listener).await;

    let (_front_addr2, _new_proxy) = start_proxy(new_upstream_addr, Some(front_addr)).await;

    // the lazily-connecting upstream channel reconnects on next use; the
    // SAME WS channel/client is reused — no client rebuild
    let resp = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        client.unary(Request::new(Msg { payload: Bytes::from_static(b"3") })),
    )
    .await
    .expect("upstream reconnect within 10s")
    .expect("rpc after upstream restart");
    assert_eq!(resp.into_inner().payload.as_ref(), b"3");
}
