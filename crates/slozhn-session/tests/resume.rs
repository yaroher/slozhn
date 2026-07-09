//! Deterministic check of the client SessionTransport: the "server" is
//! played by hand on the other end of loopback pipes.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use slozhn_frame::loopback::{self, FramePipe};
use slozhn_frame::proto::v1::{Frame, Hello, Message, frame};
use slozhn_session::SessionConfig;
use slozhn_session::client::{BoxFrameTransport, Factory, connect_session};

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
        keepalive_interval: None, // детерминированные тесты — без пингов
        keepalive_timeout: std::time::Duration::from_secs(10),
        ack_every: 1000,                     // acks don't interfere with the scenario
        ack_delay: Duration::from_secs(600), // timer won't fire
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
        matches!(
            cur,
            ConnState::Connecting | ConnState::Backoff { attempt: 2.., .. }
        ),
        "kick must force an immediate attempt, got {cur:?}"
    );
}

#[tokio::test]
async fn keepalive_detects_silent_peer_and_reconnects() {
    use slozhn_frame::transport::ReconnectHooks;
    use slozhn_session::client::connect_session_hooked;

    let (factory, mut srv_rx) = make_factory();
    let (hooks, _state) = ReconnectHooks::new();

    let cfg = SessionConfig {
        keepalive_interval: Some(Duration::from_millis(50)),
        keepalive_timeout: Duration::from_millis(50),
        ..quiet_session_config()
    };
    let connect = tokio::spawn(connect_session_hooked(
        factory,
        slozhn_frame::connection::Config::default(),
        cfg,
        hooks,
    ));
    let mut srv1 = srv_rx.recv().await.expect("first transport");
    let _client_hello = srv1.next().await.expect("client hello");
    srv1.send(hello(b"s1", b"t1", 0, false)).await.unwrap();
    let (mut transport, _peer) = connect.await.unwrap().expect("connected");

    // сервер ЖИВ (pipe не рвём), но МОЛЧИТ: Ping придёт — Pong не отправим.
    // Клиентский keepalive обязан счесть транспорт мёртвым и реконнектнуться.
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    let second = loop {
        pump(&mut transport).await;
        if let Ok(t) =
            tokio::time::timeout(Duration::from_millis(50), srv_rx.recv()).await
        {
            break t.expect("second transport");
        }
        assert!(
            std::time::Instant::now() < deadline,
            "keepalive must trigger a reconnect against a silent peer"
        );
    };
    drop(second);
    // и Ping реально уходил на первый транспорт
    let mut saw_ping = false;
    while let Ok(Some(f)) =
        tokio::time::timeout(Duration::from_millis(100), srv1.next()).await
    {
        if matches!(f.kind, Some(frame::Kind::Ping(_))) {
            saw_ping = true;
            break;
        }
    }
    assert!(saw_ping, "client must have sent a keepalive ping");
}

/// Client-side "fresh hello" frame (mirrors what `connect_session` sends).
fn client_hello() -> Frame {
    Frame {
        stream_id: 0,
        seq: 0,
        kind: Some(frame::Kind::Hello(Hello {
            version: 1,
            initial_stream_window: 65536,
            initial_connection_window: 65536,
            session_id: Bytes::new(),
            resume_token: Bytes::new(),
            last_recv_seq: 0,
            resume_rejected: false,
        })),
    }
}

#[tokio::test]
async fn max_sessions_limit_rejects_new_session() {
    use slozhn_session::server::{ServerSessionConfig, SessionManager};

    let manager = SessionManager::new(ServerSessionConfig {
        max_sessions: 1,
        ..Default::default()
    });

    // first accept: under the limit, becomes a live session
    let (client_end1, server_end1) = loopback::pair();
    let mut client_end1: BoxFrameTransport = Box::pin(client_end1);
    client_end1.send(client_hello()).await.unwrap();
    let accepted1 = manager
        .accept(Box::pin(server_end1))
        .await
        .expect("accept ok");
    assert!(accepted1.is_some(), "first session must be accepted");
    let hello1 = client_end1.next().await.expect("hello reply");
    match hello1.kind {
        Some(frame::Kind::Hello(h)) => assert!(!h.resume_rejected),
        other => panic!("expected hello, got {other:?}"),
    }

    // second accept: over the limit, must be rejected with resume_rejected hello
    let (client_end2, server_end2) = loopback::pair();
    let mut client_end2: BoxFrameTransport = Box::pin(client_end2);
    client_end2.send(client_hello()).await.unwrap();
    let accepted2 = manager
        .accept(Box::pin(server_end2))
        .await
        .expect("accept ok");
    assert!(
        accepted2.is_none(),
        "second session must be rejected due to max_sessions"
    );
    let hello2 = client_end2.next().await.expect("rejected hello reply");
    match hello2.kind {
        Some(frame::Kind::Hello(h)) => {
            assert!(h.resume_rejected, "must be rejected");
            assert!(h.session_id.is_empty());
            assert!(h.resume_token.is_empty());
        }
        other => panic!("expected hello, got {other:?}"),
    }
}

/// Sink backpressure must stall the caller instead of killing the session
/// when the replay buffer fills up during an outage, and must release once
/// the peer acks the backlog after a resume.
#[tokio::test]
async fn sink_backpressure_stalls_then_releases_after_resume_ack() {
    use slozhn_frame::proto::v1::Ack;

    let (factory, mut srv_rx) = make_factory();

    let cfg = SessionConfig {
        replay_buffer_bytes: 256, // tiny — a handful of frames fills it
        keepalive_interval: None,
        ..quiet_session_config()
    };

    let connect = tokio::spawn(connect_session(
        factory,
        slozhn_frame::connection::Config::default(),
        cfg,
    ));

    let mut srv1 = srv_rx.recv().await.expect("first transport");
    let _client_hello = srv1.next().await.expect("client hello");
    srv1.send(hello(b"s1", b"t1", 0, false)).await.unwrap();
    let (transport, _peer) = connect.await.unwrap().expect("connected");

    // kill the server side of the physical connection
    drop(srv1);

    // spawned task: keeps sending; reports each successful send's index
    let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel::<u32>();
    let mut transport = transport;
    let sender = tokio::spawn(async move {
        for i in 0..500u32 {
            if transport.send(msg(1, (i % 256) as u8)).await.is_err() {
                break;
            }
            let _ = progress_tx.send(i);
        }
    });

    // some sends complete (buffer has room initially)…
    let mut last_seen = None;
    while let Ok(Some(i)) =
        tokio::time::timeout(Duration::from_millis(200), progress_rx.recv()).await
    {
        last_seen = Some(i);
    }
    assert!(
        last_seen.is_some(),
        "expected at least one send to succeed before the buffer fills"
    );

    // …then it must get stuck: no further progress within 200ms — the sink
    // is applying backpressure instead of overflowing and killing the session
    let stuck = tokio::time::timeout(Duration::from_millis(200), progress_rx.recv()).await;
    assert!(
        stuck.is_err(),
        "sender must be stalled by backpressure, not still progressing"
    );
    assert!(!sender.is_finished(), "session must not have died");

    // reconnect: the factory hands us a fresh pipe automatically once the
    // transport notices the old one is gone and retries
    let mut srv2 = tokio::time::timeout(Duration::from_secs(2), srv_rx.recv())
        .await
        .expect("reconnect must happen")
        .expect("second transport");
    let resume = tokio::time::timeout(Duration::from_secs(1), srv2.next())
        .await
        .expect("resume hello must arrive")
        .expect("resume hello frame");
    match &resume.kind {
        Some(frame::Kind::Hello(h)) => {
            assert_eq!(h.session_id.as_ref(), b"s1");
            assert_eq!(h.resume_token.as_ref(), b"t1");
        }
        other => panic!("expected resume hello, got {other:?}"),
    }
    // accept the resume, then ack everything the client could have sent —
    // this trims the replay buffer and must unstick the sender
    srv2.send(hello(b"s1", b"t1", 0, false)).await.unwrap();
    srv2.send(Frame {
        stream_id: 0,
        seq: 0,
        kind: Some(frame::Kind::Ack(Ack {
            last_seq: 1_000_000,
        })),
    })
    .await
    .unwrap();

    // the stuck send must now resolve and progress must resume
    let resumed = tokio::time::timeout(Duration::from_secs(2), progress_rx.recv())
        .await
        .expect("send must unstick after the resume ack")
        .expect("progress channel still open");
    assert!(
        resumed > last_seen.expect("checked above"),
        "progress must have advanced past the stall point"
    );

    sender.abort();
}

/// Server-side counterpart of `sink_backpressure_stalls_then_releases_after_resume_ack`:
/// `ServerSessionTransport` must apply the same 80%-of-cap Sink backpressure
/// as the client instead of racing straight into `BufferOverflow` (which
/// kills the whole session via `die()`), and a parked `poll_ready` must
/// resolve once an incoming Ack trims the replay buffer.
#[tokio::test]
async fn server_sink_backpressure_stalls_then_releases_after_ack() {
    use slozhn_frame::proto::v1::Ack;
    use slozhn_session::server::{ServerSessionConfig, SessionManager};

    let manager = SessionManager::new(ServerSessionConfig {
        session: SessionConfig {
            replay_buffer_bytes: 256, // tiny — a handful of frames fills it
            keepalive_interval: None,
            ack_every: 1000,                     // acks don't interfere with the scenario
            ack_delay: Duration::from_secs(600), // timer won't fire
            ..Default::default()
        },
        ..Default::default()
    });

    let (client_end, server_end) = loopback::pair();
    let mut client_end: BoxFrameTransport = Box::pin(client_end);
    client_end.send(client_hello()).await.unwrap();
    let (mut server_transport, _hello) = manager
        .accept(Box::pin(server_end))
        .await
        .expect("accept ok")
        .expect("first session must be accepted");
    let _server_hello_reply = client_end.next().await.expect("hello reply");

    // Fill the replay buffer from the server app side: keep sending until
    // poll_ready stalls (Pending) instead of a send erroring out with
    // BufferOverflow.
    let mut sent = 0u32;
    loop {
        match tokio::time::timeout(
            Duration::from_millis(50),
            server_transport.send(msg(1, (sent % 256) as u8)),
        )
        .await
        {
            Ok(Ok(())) => {
                sent += 1;
                assert!(sent <= 200, "buffer never filled — expected backpressure");
            }
            Ok(Err(e)) => panic!("send failed unexpectedly (session must not die): {e:?}"),
            Err(_) => break, // stalled — backpressure engaged, not overflowed
        }
    }
    assert!(sent > 0, "expected at least one send before stalling");

    // Ack everything sent so far, over the still-Active physical connection
    // — this must trim the server's replay buffer.
    client_end
        .send(Frame {
            stream_id: 0,
            seq: 0,
            kind: Some(frame::Kind::Ack(Ack {
                last_seq: sent as u64,
            })),
        })
        .await
        .unwrap();

    // Let the server transport observe the Ack on its Stream side — this is
    // what processes the trim and wakes any parked poll_ready.
    let _ = tokio::time::timeout(Duration::from_millis(200), server_transport.next()).await;

    // A further send must now resolve promptly instead of staying stuck.
    let resumed = tokio::time::timeout(
        Duration::from_millis(200),
        server_transport.send(msg(1, 99)),
    )
    .await;
    assert!(
        matches!(resumed, Ok(Ok(()))),
        "server send must unstick after the Ack trims the replay buffer"
    );
}
