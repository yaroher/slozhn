use bytes::Bytes;
use slozhn_proto::testing::v1::echo_client::EchoClient;
use slozhn_proto::testing::v1::{Count, Msg};
use tokio_stream::StreamExt;
use tonic::Request;

async fn start_server() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, echo_ws::router()).await;
    });
    (addr, handle)
}

#[tokio::test]
async fn matrix_over_real_websocket() {
    let (addr, _srv) = start_server().await;
    let mut client = EchoClient::new(echo_ws::auto_channel(format!("ws://{addr}/rpc")));

    // unary + large payload (flow control over the network)
    let payload = Bytes::from(vec![0x5A; 512 * 1024]);
    let resp = client
        .unary(Request::new(Msg { payload: payload.clone() }))
        .await
        .unwrap();
    assert_eq!(resp.into_inner().payload, payload);

    // metadata round-trip
    let mut req = Request::new(Msg { payload: Bytes::from_static(b"m") });
    req.metadata_mut().insert("x-echo", "net".parse().unwrap());
    let resp = client.unary(req).await.unwrap();
    assert_eq!(resp.metadata().get("x-echo-back").unwrap(), "net");

    // server-stream
    let mut s = client
        .server_stream(Request::new(Count { n: 4 }))
        .await
        .unwrap()
        .into_inner();
    let mut got = vec![];
    while let Some(m) = s.next().await {
        got.push(m.unwrap().payload[0]);
    }
    assert_eq!(got, vec![0, 1, 2, 3]);

    // error with status + metadata
    let err = client
        .fail(Request::new(Msg { payload: Bytes::new() }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(err.metadata().get("x-fail-detail").unwrap(), "why-not");
}

/// TCP proxy owned by the test: drop = instant death of all connections
/// (deterministic network break). axum::serve cannot be killed this way —
/// established connections live in separate hyper tasks.
async fn start_proxy(
    backend: std::net::SocketAddr,
    front: Option<std::net::SocketAddr>,
) -> (std::net::SocketAddr, tokio::task::JoinSet<()>) {
    let bind_addr = front.unwrap_or_else(|| "127.0.0.1:0".parse().unwrap());
    let listener = tokio::net::TcpListener::bind(bind_addr).await.unwrap();
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
async fn reconnect_after_network_break() {
    let (backend, _srv) = start_server().await;
    let (front, proxy) = start_proxy(backend, None).await;
    let mut client = EchoClient::new(echo_ws::auto_channel(format!("ws://{front}/rpc")));

    client
        .unary(Request::new(Msg { payload: Bytes::from_static(b"1") }))
        .await
        .unwrap();

    // break the network: dropping the proxy kills all sockets
    drop(proxy);

    // call while the server is down: either UNAVAILABLE (the dead connection
    // is not yet detected) or a timeout while backing off before reconnect —
    // both outcomes are legitimate; success is a bug
    let second = tokio::time::timeout(
        std::time::Duration::from_millis(300),
        client.unary(Request::new(Msg { payload: Bytes::from_static(b"2") })),
    )
    .await;
    match second {
        Err(_elapsed) => {} // waiting for reconnect — correct
        Ok(Err(status)) => {
            assert_eq!(status.code(), tonic::Code::Unavailable, "{status:?}")
        }
        Ok(Ok(_)) => panic!("call must not succeed while server is down"),
    }

    // bring the proxy back on the SAME port — "network is back"
    let (_front2, _proxy2) = start_proxy(backend, Some(front)).await;

    // a new call waits for reconnect (backoff) and succeeds
    let resp = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        client.unary(Request::new(Msg { payload: Bytes::from_static(b"3") })),
    )
    .await
    .expect("reconnect within 10s")
    .expect("rpc after reconnect");
    assert_eq!(resp.into_inner().payload.as_ref(), b"3");
}
