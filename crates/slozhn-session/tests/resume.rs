//! Deterministic check of the client SessionTransport: the "server" is
//! played by hand on the other end of loopback pipes.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use slozhn_frame::loopback::{self, FramePipe};
use slozhn_frame::proto::v1::{frame, Frame, Hello, Message};
use slozhn_session::client::{connect_session, BoxFrameTransport, Factory};
use slozhn_session::SessionConfig;

fn msg(stream_id: u64, tag: u8) -> Frame {
    Frame {
        stream_id,
        seq: 0,
        kind: Some(frame::Kind::Message(Message {
            payload: Bytes::from(vec![tag]),
            compressed: false,
        })),
    }
}

fn hello(session_id: &[u8], token: &[u8], last_recv_seq: u64, rejected: bool) -> Frame {
    Frame {
        stream_id: 0,
        seq: 0,
        kind: Some(frame::Kind::Hello(Hello {
            version: 1,
            initial_stream_window: 65536,
            initial_connection_window: 65536,
            session_id: session_id.to_vec().into(),
            resume_token: token.to_vec().into(),
            last_recv_seq,
            resume_rejected: rejected,
        })),
    }
}

/// Every factory call yields a fresh pair; the server ends go to the test.
fn make_factory() -> (Factory, tokio::sync::mpsc::UnboundedReceiver<FramePipe>) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let factory: Factory = Arc::new(move || {
        let tx = tx.clone();
        Box::pin(async move {
            let (a, b) = loopback::pair();
            tx.send(b).map_err(|_| "test dropped".to_string())?;
            Ok(Box::pin(a) as BoxFrameTransport)
        })
    });
    (factory, rx)
}

fn quiet_session_config() -> SessionConfig {
    SessionConfig {
        replay_buffer_bytes: 4096,
        initial_backoff: std::time::Duration::from_millis(50),
        max_backoff: std::time::Duration::from_millis(100),
        ack_every: 1000,                      // acks don't interfere with the scenario
        ack_delay: Duration::from_secs(600),  // timer won't fire
    }
}

/// Drive the transport state machine: poll_next until Pending (timeout).
async fn pump(t: &mut slozhn_session::client::SessionTransport) {
    let _ = tokio::time::timeout(Duration::from_millis(100), t.next()).await;
}

#[tokio::test]
async fn stream_survives_kill_with_exact_replay() {
    let (factory, mut srv_rx) = make_factory();

    let connect = tokio::spawn(connect_session(
        factory,
        slozhn_frame::connection::Config::default(),
        quiet_session_config(),
    ));

    // first physical connect: new session
    let mut srv1 = srv_rx.recv().await.expect("first transport");
    let client_hello = srv1.next().await.expect("client hello");
    match &client_hello.kind {
        Some(frame::Kind::Hello(h)) => assert!(h.session_id.is_empty()),
        other => panic!("expected hello, got {other:?}"),
    }
    srv1.send(hello(b"s1", b"t1", 0, false)).await.unwrap();

    let (mut transport, peer) = connect.await.unwrap().expect("connected");
    assert_eq!(peer.session_id.as_ref(), b"s1");

    // 3 frames before the disconnect
    for tag in 1..=3u8 {
        transport.send(msg(1, tag)).await.unwrap();
    }
    for expected_seq in 1..=3u64 {
        let f = srv1.next().await.expect("frame");
        assert_eq!(f.seq, expected_seq);
    }

    // DISCONNECT
    drop(srv1);

    // 2 more frames during the gap — they go into the replay buffer
    transport.send(msg(1, 4)).await.unwrap();
    transport.send(msg(1, 5)).await.unwrap();

    // pump: the transport notices the disconnect and reconnects
    pump(&mut transport).await;

    // second physical connect: resume
    let mut srv2 = srv_rx.recv().await.expect("second transport");
    let resume = srv2.next().await.expect("resume hello");
    match &resume.kind {
        Some(frame::Kind::Hello(h)) => {
            assert_eq!(h.session_id.as_ref(), b"s1");
            assert_eq!(h.resume_token.as_ref(), b"t1");
        }
        other => panic!("expected resume hello, got {other:?}"),
    }
    // the server confirms: received everything up to seq 3
    srv2.send(hello(b"s1", b"t1", 3, false)).await.unwrap();

    pump(&mut transport).await;

    // replay: exactly 4 and 5, no duplicates of 1-3
    let f4 = srv2.next().await.expect("replayed 4");
    assert_eq!(f4.seq, 4);
    let f5 = srv2.next().await.expect("replayed 5");
    assert_eq!(f5.seq, 5);

    // the reverse direction works after resume
    let mut downstream = msg(2, 42);
    downstream.seq = 1;
    srv2.send(downstream).await.unwrap();
    let got = tokio::time::timeout(Duration::from_secs(1), transport.next())
        .await
        .expect("inbound")
        .expect("frame");
    assert_eq!(got.stream_id, 2);
    assert_eq!(got.seq, 1);
}

#[tokio::test]
async fn resume_rejected_ends_transport() {
    let (factory, mut srv_rx) = make_factory();

    let connect = tokio::spawn(connect_session(
        factory,
        slozhn_frame::connection::Config::default(),
        quiet_session_config(),
    ));
    let mut srv1 = srv_rx.recv().await.expect("first transport");
    let _client_hello = srv1.next().await.expect("client hello");
    srv1.send(hello(b"s1", b"t1", 0, false)).await.unwrap();
    let (mut transport, _peer) = connect.await.unwrap().expect("connected");

    drop(srv1); // disconnect
    pump(&mut transport).await;

    let mut srv2 = srv_rx.recv().await.expect("second transport");
    let _resume = srv2.next().await.expect("resume hello");
    srv2.send(hello(b"", b"", 0, true)).await.unwrap(); // REJECTED

    // the transport dies honestly
    let end = tokio::time::timeout(Duration::from_secs(1), transport.next())
        .await
        .expect("must finish");
    assert!(end.is_none());
}

#[tokio::test]
async fn kick_breaks_session_backoff() {
    use slozhn_frame::transport::{ConnState, ReconnectHooks};
    use slozhn_session::client::connect_session_hooked;

    // factory: 1-й вызов — живой пайп, дальше всегда ошибка (сервера «нет»)
    let (pipe_tx, pipe_rx) = tokio::sync::mpsc::unbounded_channel::<FramePipe>();
    let first = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let factory: slozhn_session::client::Factory = std::sync::Arc::new(move || {
        let first = first.clone();
        let pipe_tx = pipe_tx.clone();
        Box::pin(async move {
            if first.swap(false, std::sync::atomic::Ordering::SeqCst) {
                let (a, b) = loopback::pair();
                pipe_tx.send(b).map_err(|_| "test dropped".to_string())?;
                Ok(Box::pin(a) as slozhn_session::client::BoxFrameTransport)
            } else {
                Err("server down".to_string())
            }
        })
    });
    let (hooks, state) = ReconnectHooks::new();

    let connect = tokio::spawn(connect_session_hooked(
        factory,
        slozhn_frame::connection::Config::default(),
        quiet_session_config(),
        hooks.clone(),
    ));
    let mut srv_rx = pipe_rx;
    let mut srv1 = srv_rx.recv().await.expect("first transport");
    let _client_hello = srv1.next().await.expect("client hello");
    srv1.send(hello(b"s1", b"t1", 0, false)).await.unwrap();
    let (mut transport, _peer) = connect.await.unwrap().expect("connected");
    assert_eq!(*state.borrow(), ConnState::Connected);

    drop(srv1); // разрыв; все реконнекты падают → backoff-и

    // прокачиваем машину, ждём Backoff{attempt >= 1}
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        pump(&mut transport).await;
        if matches!(*state.borrow(), ConnState::Backoff { .. }) {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "no backoff observed");
    }

    // kick: следующий poll должен немедленно уйти в Connecting
    hooks.kick.notify_waiters();
    pump(&mut transport).await;
    let cur = state.borrow().clone();
    assert!(
        matches!(cur, ConnState::Connecting | ConnState::Backoff { attempt: 2.., .. }),
        "kick must force an immediate attempt, got {cur:?}"
    );
}
