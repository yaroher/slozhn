//! Session-layer e2e: a live bidi stream survives a network break (spec §8).

use bytes::Bytes;
use slozhn_proto::testing::v1::echo_client::EchoClient;
use slozhn_proto::testing::v1::Msg;
use tokio_stream::StreamExt;
use tonic::Request;

async fn start_server_session() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, echo_ws::router_session()).await;
    });
    (addr, handle)
}

/// TCP proxy owned by the test: drop = instant death of all connections.
async fn start_proxy(
    backend: std::net::SocketAddr,
    front: Option<std::net::SocketAddr>,
) -> (std::net::SocketAddr, tokio::task::JoinSet<()>) {
    let bind_addr = front.unwrap_or_else(|| "127.0.0.1:0".parse().unwrap());
    // the previous proxy may not have released the port yet (abort is async)
    let listener = loop {
        match tokio::net::TcpListener::bind(bind_addr).await {
            Ok(l) => break l,
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
        }
    };
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
async fn bidi_stream_survives_network_break() {
    let (backend, _srv) = start_server_session().await;
    let (front, proxy) = start_proxy(backend, None).await;
    let mut client = EchoClient::new(echo_ws::session_channel(format!("ws://{front}/rpc")));

    let (tx, rx) = tokio::sync::mpsc::channel::<Msg>(1);
    let mut inbound = client
        .bidi(Request::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
        .await
        .unwrap()
        .into_inner();

    // 5 rounds before the break
    for i in 1u64..=5 {
        tx.send(Msg { payload: Bytes::copy_from_slice(&i.to_le_bytes()) })
            .await
            .unwrap();
        let m = inbound.next().await.unwrap().unwrap();
        assert_eq!(u64::from_le_bytes(m.payload.as_ref().try_into().unwrap()), i * 2);
    }

    // BREAK in the middle of a live bidi stream; network comes back on the same port
    drop(proxy);
    let (_f2, _proxy2) = start_proxy(backend, Some(front)).await;

    // the same stream keeps working — no errors, no losses, no duplicates
    for i in 6u64..=10 {
        tx.send(Msg { payload: Bytes::copy_from_slice(&i.to_le_bytes()) })
            .await
            .unwrap();
        let m = tokio::time::timeout(std::time::Duration::from_secs(10), inbound.next())
            .await
            .expect("resume within 10s")
            .unwrap()
            .unwrap();
        assert_eq!(u64::from_le_bytes(m.payload.as_ref().try_into().unwrap()), i * 2);
    }
    drop(tx);
    assert!(inbound.next().await.is_none());
}

#[tokio::test]
async fn server_restart_rejects_resume_then_fresh_session_works() {
    let (backend, srv) = start_server_session().await;
    let (front, proxy) = start_proxy(backend, None).await;
    let mut client = EchoClient::new(echo_ws::session_channel(format!("ws://{front}/rpc")));

    client
        .unary(Request::new(Msg { payload: Bytes::from_static(b"1") }))
        .await
        .unwrap();

    // EVERYTHING dies: both network and server (in-memory sessions are gone)
    drop(proxy);
    srv.abort();
    let _ = srv.await;

    // server and network come back on the same addresses, but foreign to the session
    let listener = tokio::net::TcpListener::bind(backend).await.unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, echo_ws::router_session()).await;
    });
    let (_f2, _proxy2) = start_proxy(backend, Some(front)).await;

    // resume is rejected → the current connection dies (UNAVAILABLE), then
    // AutoChannel creates a fresh session — calls succeed again
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client.unary(Request::new(Msg { payload: Bytes::from_static(b"2") })),
        )
        .await
        {
            Ok(Ok(resp)) => {
                assert_eq!(resp.into_inner().payload.as_ref(), b"2");
                break; // fresh session works
            }
            Ok(Err(status)) => {
                // legitimate failure on the way to a fresh session
                assert_eq!(status.code(), tonic::Code::Unavailable, "{status:?}");
            }
            Err(_elapsed) => {} // waiting for reconnect / restart
        }
        assert!(
            std::time::Instant::now() < deadline,
            "fresh session must work within 15s"
        );
    }
}
