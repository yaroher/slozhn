//! Reconnect observability e2e: the state stream is visible from outside
//! and `reconnect_now()` punches through a backoff wait.

use bytes::Bytes;
use slozhn::frame::transport::ConnState;
use slozhn_proto::testing::v1::echo_client::EchoClient;
use slozhn_proto::testing::v1::Msg;
use tonic::Request;

async fn start_server_session() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, echo_ws::router_session()).await;
    });
    (addr, handle)
}

/// Test-owned TCP proxy: dropping it kills every connection instantly.
async fn start_proxy(
    backend: std::net::SocketAddr,
    front: Option<std::net::SocketAddr>,
) -> (std::net::SocketAddr, tokio::task::JoinSet<()>) {
    let bind_addr = front.unwrap_or_else(|| "127.0.0.1:0".parse().unwrap());
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
async fn state_is_observable_and_kick_breaks_backoff() {
    let (backend, _srv) = start_server_session().await;
    let (front, proxy) = start_proxy(backend, None).await;

    // огромный initial backoff: без kick восстановление заняло бы вечность
    let channel = slozhn::client::builder(format!("ws://{front}/rpc"))
        .resume()
        .reconnect_config(slozhn_client::reconnect::AutoConfig {
            initial_backoff: std::time::Duration::from_secs(600),
            max_backoff: std::time::Duration::from_secs(600),
        })
        .build();
    let mut state = channel.state();
    let mut client = EchoClient::new(channel.clone());

    assert_eq!(*state.borrow(), ConnState::Idle);

    client
        .unary(Request::new(Msg { payload: Bytes::from_static(b"1") }))
        .await
        .unwrap();
    assert_eq!(*state.borrow_and_update(), ConnState::Connected);

    // разрыв сети: state должен дойти до Backoff (session-слой реконнектится,
    // сервера за прокси «нет»)
    drop(proxy);
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            state.changed().await.unwrap();
            if matches!(*state.borrow(), ConnState::Backoff { .. }) {
                break;
            }
        }
    })
    .await
    .expect("must observe Backoff after network break");

    // сеть вернулась, но backoff — 600s; kick пробивает его
    let (_front2, _proxy2) = start_proxy(backend, Some(front)).await;
    channel.reconnect_now();

    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            state.changed().await.unwrap();
            if *state.borrow() == ConnState::Connected {
                break;
            }
        }
    })
    .await
    .expect("kick must reconnect fast despite 600s backoff");

    // и RPC снова работает
    let resp = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        client.unary(Request::new(Msg { payload: Bytes::from_static(b"2") })),
    )
    .await
    .expect("rpc after kick")
    .unwrap();
    assert_eq!(resp.into_inner().payload.as_ref(), b"2");
}

#[tokio::test]
async fn middleware_stack_works_end_to_end() {
    use slozhn::middleware::{RetryLayer, TraceLayer};
    #[allow(unused_imports)]

    // server with TraceLayer::server() wrapped around the tonic routes
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let routes = tonic::service::Routes::new(
            slozhn_proto::testing::v1::echo_server::EchoServer::new(echo_ws::EchoImpl),
        );
        let traced = slozhn::middleware::trace_server(routes);
        let manager = slozhn::server::SessionManager::new(Default::default());
        let app = axum::Router::new()
            .route("/rpc", slozhn::server::grpc_ws_session(traced, manager));
        let _ = axum::serve(listener, app).await;
    });

    // client with Trace + Retry layers on top of the channel
    let channel = slozhn::client::builder(format!("ws://{addr}/rpc"))
        .resume()
        .build();
    let stack = tower::ServiceBuilder::new()
        .layer(TraceLayer::client())
        .layer(RetryLayer::default())
        .service(channel);
    let mut client = EchoClient::new(stack);

    let resp = client
        .unary(Request::new(Msg { payload: Bytes::from_static(b"layered") }))
        .await
        .unwrap();
    assert_eq!(resp.into_inner().payload.as_ref(), b"layered");
}

#[tokio::test]
async fn retry_layer_recovers_unary_over_network_break() {
    use slozhn::middleware::RetryLayer;

    // plain (non-session) server — клиент тоже без resume
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, echo_ws::router()).await;
    });
    let (front, proxy) = start_proxy(backend, None).await;

    let channel = slozhn::client::builder(format!("ws://{front}/rpc"))
        .reconnect_config(slozhn_client::reconnect::AutoConfig {
            initial_backoff: std::time::Duration::from_millis(50),
            max_backoff: std::time::Duration::from_millis(200),
        })
        .build();
    let stack = tower::ServiceBuilder::new()
        .layer(RetryLayer {
            max_attempts: 5,
            base_backoff: std::time::Duration::from_millis(200),
            ..Default::default()
        })
        .service(channel);
    let mut client = EchoClient::new(stack);

    client
        .unary(Request::new(Msg { payload: Bytes::from_static(b"1") }))
        .await
        .unwrap();

    // рвём сеть и тут же возвращаем: первый attempt попадает на мёртвое
    // соединение (UNAVAILABLE), ретрай после реконнекта должен пройти
    drop(proxy);
    let (_f2, _p2) = start_proxy(backend, Some(front)).await;

    let resp = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        client.unary(Request::new(Msg { payload: Bytes::from_static(b"2") })),
    )
    .await
    .expect("in time")
    .expect("retry must recover the unary call");
    assert_eq!(resp.into_inner().payload.as_ref(), b"2");
}

#[tokio::test]
async fn drain_lets_active_streams_finish_and_rejects_new_ones() {
    use tokio_stream::StreamExt as _;

    let registry = slozhn::server::ConnectionRegistry::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    {
        let registry = registry.clone();
        tokio::spawn(async move {
            let routes = tonic::service::Routes::new(
                slozhn_proto::testing::v1::echo_server::EchoServer::new(echo_ws::EchoImpl),
            );
            let manager = slozhn::server::SessionManager::new(Default::default());
            let app = axum::Router::new().route(
                "/rpc",
                slozhn::server::grpc_ws_session_with_registry(routes, manager, registry),
            );
            let _ = axum::serve(listener, app).await;
        });
    }

    let channel = slozhn::client::builder(format!("ws://{addr}/rpc")).resume().build();
    let mut client = EchoClient::new(channel);

    // живой bidi до дренажа
    let (tx, rx) = tokio::sync::mpsc::channel::<Msg>(1);
    let mut inbound = client
        .bidi(Request::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
        .await
        .unwrap()
        .into_inner();
    tx.send(Msg { payload: Bytes::copy_from_slice(&1u64.to_le_bytes()) })
        .await
        .unwrap();
    inbound.next().await.unwrap().unwrap();
    assert_eq!(registry.len(), 1);

    // ДРЕНАЖ
    registry.drain_all(slozhn::frame::error::GoAwayCode::Graceful);

    // новый RPC отклоняется (GoAway → open fails → честный UNAVAILABLE);
    // GoAway летит асинхронно — даём ему дойти
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let err = client
            .unary(Request::new(Msg { payload: Bytes::from_static(b"x") }))
            .await
            .err();
        match err {
            Some(status) if status.code() == tonic::Code::Unavailable => break,
            Some(other) => panic!("unexpected status: {other:?}"),
            None if std::time::Instant::now() < deadline => {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            None => panic!("new RPCs must be rejected after drain"),
        }
    }

    // а активный bidi ДОРАБАТЫВАЕТ сквозь дренаж
    for i in 2u64..=4 {
        tx.send(Msg { payload: Bytes::copy_from_slice(&i.to_le_bytes()) })
            .await
            .unwrap();
        let m = inbound.next().await.unwrap().unwrap();
        assert_eq!(u64::from_le_bytes(m.payload.as_ref().try_into().unwrap()), i * 2);
    }
    drop(tx);
    assert!(inbound.next().await.is_none());
}

#[tokio::test]
async fn descriptor_driven_retry_and_idempotency_key() {
    use slozhn::middleware::{IdempotencyIndex, IdempotencyKeyLayer, IdempotencyLevel, RetryLayer};
    use std::sync::Arc;

    // настоящий дескриптор из генерённого крейта: Unary помечен IDEMPOTENT
    let fds = slozhn_proto::testing::v1::FILE_DESCRIPTOR_SET;
    let idx = Arc::new(IdempotencyIndex::from_descriptor_set(fds).unwrap());
    assert_eq!(idx.level("/testing.v1.Echo/Unary"), IdempotencyLevel::Idempotent);
    assert_eq!(idx.level("/testing.v1.Echo/Bidi"), IdempotencyLevel::Unknown);

    let retry = RetryLayer::from_file_descriptor_set(fds).unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, echo_ws::router()).await;
    });

    let channel = slozhn::client::builder(format!("ws://{addr}/rpc")).build();
    let stack = tower::ServiceBuilder::new()
        .layer(IdempotencyKeyLayer::new(idx))
        .layer(retry)
        .service(channel);
    let mut client = EchoClient::new(stack);

    let resp = client
        .unary(Request::new(Msg { payload: Bytes::from_static(b"marked") }))
        .await
        .unwrap();
    assert_eq!(resp.into_inner().payload.as_ref(), b"marked");
}
