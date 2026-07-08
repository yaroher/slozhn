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
    use tower::Layer as _;

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
