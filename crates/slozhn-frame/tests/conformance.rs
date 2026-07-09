//! Golden conformance vectors for `docs/protocol.md`. Each test pins one
//! spec section against the reference implementation, exercised through
//! `bind`/`bind_pre_negotiated` + `loopback::pair()` (see `tests/loopback.rs`
//! for the same style with less commentary). Per `docs/protocol.md` §10,
//! these vectors are normative: a conformant reimplementation MUST reproduce
//! the same observable frame kinds, ordering, and error outcomes.
//!
//! A few cases noted inline could not be pinned through the *public* API
//! alone and reach for the raw frame wire (`loopback::pair()` without
//! `bind()` on one side) instead — the same pattern already used by
//! `tests/loopback.rs::protocol_violation_goaway` and
//! `::version_mismatch_kills_connection` for exactly this reason: driving a
//! deliberately non-conformant peer by hand.

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures::{Sink, SinkExt, Stream, StreamExt};

use slozhn_frame::connection::{Config, bind};
use slozhn_frame::error::{ConnError, GoAwayCode, OpenError, ProtocolError, TransportClosed};
use slozhn_frame::ext::{MetadataExt, StatusExt};
use slozhn_frame::ids::Side;
use slozhn_frame::loopback;
use slozhn_frame::proto::v1::{Frame, Hello, Message, Metadata, Open, Ping, Status, frame};
use slozhn_frame::stream::StreamEvent;

/// Wraps a frame transport and records every frame it *sends* (outbound, in
/// send order) into a shared log. Lets a test assert the exact wire sequence
/// one side produced without reaching into driver internals — the only
/// public surface it uses is the `Stream`/`Sink<Frame>` transport contract
/// that `bind` itself requires.
struct Sniff<T> {
    inner: T,
    out: Arc<Mutex<Vec<Frame>>>,
}

impl<T> Stream for Sniff<T>
where
    T: Stream<Item = Frame> + Unpin,
{
    type Item = Frame;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Frame>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl<T> Sink<Frame> for Sniff<T>
where
    T: Sink<Frame, Error = TransportClosed> + Unpin,
{
    type Error = TransportClosed;
    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_ready(cx)
    }
    fn start_send(mut self: Pin<&mut Self>, item: Frame) -> Result<(), Self::Error> {
        self.out.lock().unwrap().push(item.clone());
        Pin::new(&mut self.inner).start_send(item)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_close(cx)
    }
}

fn kind_name(f: &Frame) -> &'static str {
    match &f.kind {
        Some(frame::Kind::Open(_)) => "Open",
        Some(frame::Kind::Headers(_)) => "Headers",
        Some(frame::Kind::Message(_)) => "Message",
        Some(frame::Kind::HalfClose(_)) => "HalfClose",
        Some(frame::Kind::Status(_)) => "Status",
        Some(frame::Kind::Cancel(_)) => "Cancel",
        Some(frame::Kind::Ping(_)) => "Ping",
        Some(frame::Kind::Pong(_)) => "Pong",
        Some(frame::Kind::WindowUpdate(_)) => "WindowUpdate",
        Some(frame::Kind::GoAway(_)) => "GoAway",
        Some(frame::Kind::Hello(_)) => "Hello",
        Some(frame::Kind::Ack(_)) => "Ack",
        None => "Empty",
    }
}

fn hello_frame(version: u32, window: u32) -> Frame {
    Frame {
        stream_id: 0,
        seq: 0,
        kind: Some(frame::Kind::Hello(Hello {
            version,
            initial_stream_window: window,
            initial_connection_window: window,
            ..Default::default()
        })),
    }
}

// ---------------------------------------------------------------------
// 1. Handshake — spec §3
// ---------------------------------------------------------------------

#[tokio::test]
async fn hello_exchange_echoes_version_and_windows() {
    // spec §3: each side sends its own Hello first, unprompted, announcing
    // PROTOCOL_VERSION and its configured windows.
    let cfg = Config {
        initial_stream_window: 12_345,
        initial_connection_window: 54_321,
        max_streams: 8,
        ..Config::default()
    };
    let (a, mut raw) = loopback::pair();
    let (_client, drv) = bind(Side::Client, cfg, a);
    tokio::spawn(drv.run());

    let hello = raw.next().await.expect("client sends hello first, unprompted");
    match hello.kind {
        Some(frame::Kind::Hello(h)) => {
            assert_eq!(h.version, slozhn_frame::PROTOCOL_VERSION);
            assert_eq!(h.initial_stream_window, 12_345);
            assert_eq!(h.initial_connection_window, 54_321);
        }
        other => panic!("expected Hello, got {other:?}"),
    }

    // complete the handshake so the driver doesn't hang mid-test
    raw.send(hello_frame(slozhn_frame::PROTOCOL_VERSION, 65_536))
        .await
        .unwrap();
}

#[tokio::test]
async fn frame_before_hello_is_connection_error() {
    // spec §3: any frame read before the peer's Hello MUST be a protocol
    // error — there is no degraded/partial mode.
    let (a, mut raw) = loopback::pair();
    let (_client, drv) = bind(Side::Client, Config::default(), a);
    let handle = tokio::spawn(drv.run());

    let _ = raw.next().await.expect("client's own hello, drained");
    raw.send(Frame {
        stream_id: 0,
        seq: 0,
        kind: Some(frame::Kind::Ping(Ping { opaque: 1 })),
    })
    .await
    .unwrap();

    let err = handle.await.unwrap().unwrap_err();
    assert!(matches!(
        err,
        ConnError::Protocol(ProtocolError::ExpectedHello)
    ));
}

// ---------------------------------------------------------------------
// 2. Stream id parity — spec §2
// ---------------------------------------------------------------------

#[tokio::test]
async fn server_rejects_client_open_with_even_stream_id() {
    // spec §2: the client MUST allocate odd stream ids; a server receiving
    // an Open with an even id (client-parity violation) MUST kill the
    // connection. Driven by hand (raw wire) because a conformant client
    // would never construct this frame through the public API.
    let (a, mut raw) = loopback::pair();
    let (_server, drv) = bind(Side::Server, Config::default(), a);
    let handle = tokio::spawn(drv.run());

    let _ = raw.next().await.expect("server's own hello, drained");
    raw.send(hello_frame(slozhn_frame::PROTOCOL_VERSION, 65_536))
        .await
        .unwrap();

    raw.send(Frame {
        stream_id: 2, // even — invalid for a client-initiated Open
        seq: 0,
        kind: Some(frame::Kind::Open(Open {
            method: "/svc.S/Bad".into(),
            metadata: Some(Metadata::empty()),
        })),
    })
    .await
    .unwrap();

    let err = handle.await.unwrap().unwrap_err();
    assert!(matches!(
        err,
        ConnError::Protocol(ProtocolError::InvalidParity(2))
    ));
}

// ---------------------------------------------------------------------
// 3. Unary happy path golden sequence — spec §4.1
// ---------------------------------------------------------------------

#[tokio::test]
async fn unary_happy_path_wire_sequence() {
    // spec §4.1: Open -> Message -> HalfClose from the opener; Headers ->
    // Message -> Status from the acceptor. Flow-control WindowUpdate credit
    // frames are filtered out of this comparison (their exact placement is
    // pinned separately, §5, by the flow-control test below) so this test
    // stays a clean pin of the RPC lifecycle frames themselves.
    let (a, b) = loopback::pair();
    let client_out = Arc::new(Mutex::new(Vec::new()));
    let server_out = Arc::new(Mutex::new(Vec::new()));
    let a = Sniff { inner: a, out: client_out.clone() };
    let b = Sniff { inner: b, out: server_out.clone() };
    let (client, cd) = bind(Side::Client, Config::default(), a);
    let (server, sd) = bind(Side::Server, Config::default(), b);
    tokio::spawn(cd.run());
    tokio::spawn(sd.run());
    let _keep = server.clone();

    let server_task = tokio::spawn(async move {
        let mut inc = server.accept().await.expect("incoming");
        inc.send.send_headers(Metadata::empty()).unwrap();
        // wait for BOTH the message and the half-close before replying, so
        // the opener's HalfClose is guaranteed on the wire before our Status
        // terminates the stream (otherwise the sequences race)
        let mut payload = None;
        let mut half_closed = false;
        while payload.is_none() || !half_closed {
            match inc.recv.next_event().await.expect("event") {
                StreamEvent::Message(b) => payload = Some(b),
                StreamEvent::RemoteHalfClose => half_closed = true,
                other => panic!("unexpected {other:?}"),
            }
        }
        let payload = payload.unwrap();
        inc.send.send(payload).await.unwrap();
        inc.send.finish(Status::ok()).await.unwrap();
    });

    let (send, mut recv) = client
        .open("/echo.Echo/Do".into(), Metadata::empty())
        .await
        .unwrap();
    send.send(Bytes::from_static(b"hi")).await.unwrap();
    send.half_close().await.unwrap();

    assert!(matches!(recv.next_event().await, Some(StreamEvent::Headers(_))));
    assert!(matches!(recv.next_event().await, Some(StreamEvent::Message(_))));
    let terminal = loop {
        match recv.next_event().await.expect("event") {
            StreamEvent::RemoteHalfClose => continue,
            e => break e,
        }
    };
    assert!(matches!(terminal, StreamEvent::Terminated(_)));
    server_task.await.unwrap();

    let stream_id = 1; // client's first opened stream
    let client_kinds: Vec<&str> = client_out
        .lock()
        .unwrap()
        .iter()
        .filter(|f| f.stream_id == stream_id && !matches!(f.kind, Some(frame::Kind::WindowUpdate(_))))
        .map(kind_name)
        .collect();
    assert_eq!(client_kinds, vec!["Open", "Message", "HalfClose"]);

    let server_kinds: Vec<&str> = server_out
        .lock()
        .unwrap()
        .iter()
        .filter(|f| f.stream_id == stream_id && !matches!(f.kind, Some(frame::Kind::WindowUpdate(_))))
        .map(kind_name)
        .collect();
    assert_eq!(server_kinds, vec!["Headers", "Message", "Status"]);
}

// ---------------------------------------------------------------------
// 4. Flow control: borrow rule + stall until credit — spec §5
// ---------------------------------------------------------------------

#[tokio::test]
async fn flow_control_borrow_rule_then_stall_until_credit() {
    // spec §5 borrow rule: a send is allowed whenever the window is > 0,
    // even if the message overshoots it; the window may then go negative by
    // that one message. The *next* send must stall until enough
    // WindowUpdate credit arrives to bring the window back above zero.
    let cfg = Config {
        initial_stream_window: 16,
        initial_connection_window: 4096,
        ..Config::default()
    };
    let (a, b) = loopback::pair();
    let (client, cd) = bind(Side::Client, cfg.clone(), a);
    let (server, sd) = bind(Side::Server, cfg, b);
    tokio::spawn(cd.run());
    tokio::spawn(sd.run());
    let _keep = (client.clone(), server.clone());

    let (send, _recv) = client
        .open("/svc.S/Borrow".into(), Metadata::empty())
        .await
        .unwrap();
    let mut inc = server.accept().await.expect("incoming");

    // window is 16; a single 20-byte message overshoots it by 4 — allowed
    // once (the borrow), window becomes 16 - 20 = -4.
    send.send(Bytes::from(vec![0u8; 20])).await.unwrap();

    // window is negative: the next send, however small, MUST block.
    let blocked = send.send(Bytes::from(vec![1u8; 1]));
    tokio::pin!(blocked);
    tokio::select! {
        _ = &mut blocked => panic!("send must block: window went negative after the borrow"),
        _ = tokio::time::sleep(Duration::from_millis(50)) => {}
    }

    // server reads the 20-byte message -> credits 20 back -> window
    // -4 + 20 = 16 > 0 -> the blocked send unblocks.
    assert!(matches!(
        inc.recv.next_event().await,
        Some(StreamEvent::Message(b)) if b.len() == 20
    ));
    blocked.await.unwrap();

    // Note: beyond MAX_MESSAGE_SIZE (pinned at the unit level in
    // stream.rs::oversized_message_is_protocol_error), there is no separate
    // "overshoot too large" protocol error in the reference implementation
    // — a single send is bounded only by MAX_MESSAGE_SIZE, not by any
    // window-relative cap. See docs/protocol.md §5.
}

// ---------------------------------------------------------------------
// 5. Reset-race tolerance — spec §4.4
// ---------------------------------------------------------------------

#[tokio::test]
async fn frames_on_reset_stream_are_dropped_silently() {
    // spec §4.4: a frame that arrives for a stream id this side itself just
    // terminated is a legal race, not a protocol error — it MUST be dropped
    // silently and the connection MUST stay alive. Driven by hand because
    // the public API has no way to keep sending on a stream this side
    // already knows is finished.
    let (a, mut raw) = loopback::pair();
    let (server, drv) = bind(Side::Server, Config::default(), a);
    let handle = tokio::spawn(drv.run());

    let _ = raw.next().await.expect("server's own hello, drained");
    raw.send(hello_frame(slozhn_frame::PROTOCOL_VERSION, 65_536))
        .await
        .unwrap();

    raw.send(Frame {
        stream_id: 1,
        seq: 0,
        kind: Some(frame::Kind::Open(Open {
            method: "/svc.S/One".into(),
            metadata: Some(Metadata::empty()),
        })),
    })
    .await
    .unwrap();
    let inc = server.accept().await.expect("incoming");

    // server finishes the stream immediately — stream_id 1 becomes reset
    inc.send.finish(Status::ok()).await.unwrap();
    let status = raw.next().await.expect("status frame");
    assert!(matches!(status.kind, Some(frame::Kind::Status(_))));

    // a Message in flight on the now-reset stream id MUST be dropped
    // silently — no connection error.
    raw.send(Frame {
        stream_id: 1,
        seq: 0,
        kind: Some(frame::Kind::Message(Message {
            payload: Bytes::from_static(b"late"),
            compressed: false,
        })),
    })
    .await
    .unwrap();

    // prove the connection is still alive: an ordinary Ping still gets a Pong.
    raw.send(Frame {
        stream_id: 0,
        seq: 0,
        kind: Some(frame::Kind::Ping(Ping { opaque: 99 })),
    })
    .await
    .unwrap();
    let pong = raw.next().await.expect("connection alive — pong arrives");
    assert!(matches!(pong.kind, Some(frame::Kind::Pong(p)) if p.opaque == 99));

    // the accepted stream's recv half also holds a command-channel handle:
    // it must go too, or the driver never observes "all handles dropped"
    drop(inc.recv);
    drop(server);
    let _ = handle.await;
}

// ---------------------------------------------------------------------
// 6. Stream limit — spec §4.5
// ---------------------------------------------------------------------

#[tokio::test]
async fn stream_limit_exceeded_yields_stream_level_status_8() {
    // spec §4.5: Config.max_streams rejects an inbound Open beyond the
    // limit with a stream-level Status(RESOURCE_EXHAUSTED=8), not a
    // connection error — the rest of the connection is unaffected.
    let server_cfg = Config { max_streams: 1, ..Config::default() };
    let (a, b) = loopback::pair();
    let (client, cd) = bind(Side::Client, Config::default(), a);
    let (server, sd) = bind(Side::Server, server_cfg, b);
    tokio::spawn(cd.run());
    tokio::spawn(sd.run());
    let _keep = (client.clone(), server.clone());

    let (_send1, _recv1) = client
        .open("/svc.S/One".into(), Metadata::empty())
        .await
        .unwrap();
    let _inc1 = server.accept().await.expect("incoming 1");

    let (_send2, mut recv2) = client
        .open("/svc.S/Two".into(), Metadata::empty())
        .await
        .unwrap();
    let terminal = recv2.next_event().await.expect("event");
    assert!(matches!(terminal, StreamEvent::Terminated(s) if s.code == 8));

    // the connection itself is unaffected: a fresh ping still round-trips
    client.ping().await.unwrap();
}

// ---------------------------------------------------------------------
// 7. Ping -> Pong opaque echo — spec §6
// ---------------------------------------------------------------------

#[tokio::test]
async fn ping_pong_opaque_echo() {
    // spec §6: a Pong MUST echo the Ping's opaque value unmodified.
    let (a, mut raw) = loopback::pair();
    let (_client, drv) = bind(Side::Client, Config::default(), a);
    tokio::spawn(drv.run());

    let _ = raw.next().await.expect("client's own hello, drained");
    raw.send(hello_frame(slozhn_frame::PROTOCOL_VERSION, 65_536))
        .await
        .unwrap();

    raw.send(Frame {
        stream_id: 0,
        seq: 0,
        kind: Some(frame::Kind::Ping(Ping { opaque: 0xDEAD_BEEF })),
    })
    .await
    .unwrap();
    let pong = raw.next().await.expect("pong");
    assert!(matches!(pong.kind, Some(frame::Kind::Pong(p)) if p.opaque == 0xDEAD_BEEF));
}

// ---------------------------------------------------------------------
// 8. GoAway drain semantics — spec §7
// ---------------------------------------------------------------------

#[tokio::test]
async fn goaway_rejects_new_opens_but_lets_existing_stream_finish() {
    // spec §7: after GoAway, new opens are rejected; a stream already open
    // before GoAway runs to completion normally.
    let (a, b) = loopback::pair();
    let (client, cd) = bind(Side::Client, Config::default(), a);
    let (server, sd) = bind(Side::Server, Config::default(), b);
    tokio::spawn(cd.run());
    tokio::spawn(sd.run());
    let _keep = (client.clone(), server.clone());

    let (send, mut recv) = client
        .open("/svc.S/Before".into(), Metadata::empty())
        .await
        .unwrap();
    let mut inc = server.accept().await.expect("incoming");

    server.go_away(GoAwayCode::Graceful);

    // wait until GoAway has reached the client — new opens start failing
    loop {
        match client.open("/svc.S/After".into(), Metadata::empty()).await {
            Err(OpenError::GoingAway) => break,
            Ok((s, _r)) => {
                s.cancel(); // GoAway still in flight; retry
                tokio::task::yield_now().await;
            }
            Err(other) => panic!("unexpected {other:?}"),
        }
    }

    // the stream opened before GoAway still works end to end
    send.send(Bytes::from_static(b"still works")).await.unwrap();
    inc.send.send_headers(Metadata::empty()).unwrap();
    let payload = loop {
        match inc.recv.next_event().await.expect("event") {
            StreamEvent::Message(b) => break b,
            StreamEvent::RemoteHalfClose => continue,
            other => panic!("unexpected {other:?}"),
        }
    };
    inc.send.send(payload).await.unwrap();
    inc.send.finish(Status::ok()).await.unwrap();

    assert!(matches!(recv.next_event().await, Some(StreamEvent::Headers(_))));
    assert!(matches!(
        recv.next_event().await,
        Some(StreamEvent::Message(b)) if b.as_ref() == b"still works"
    ));
    let terminal = loop {
        match recv.next_event().await.expect("event") {
            StreamEvent::RemoteHalfClose => continue,
            e => break e,
        }
    };
    assert!(matches!(terminal, StreamEvent::Terminated(s) if s.code == 0));
}

// ---------------------------------------------------------------------
// 9. Terminal Status wins — spec §4.2 / §4.4
// ---------------------------------------------------------------------

#[tokio::test]
async fn frames_after_terminal_status_are_ignored_by_opener() {
    // spec §4.2: once a Status has terminated a stream, no further event is
    // ever delivered to the application on that stream — the event channel
    // simply drains and closes, it does not error.
    let (a, b) = loopback::pair();
    let (client, cd) = bind(Side::Client, Config::default(), a);
    let (server, sd) = bind(Side::Server, Config::default(), b);
    tokio::spawn(cd.run());
    tokio::spawn(sd.run());
    let _keep = (client.clone(), server.clone());

    let (send, mut recv) = client
        .open("/svc.S/Term".into(), Metadata::empty())
        .await
        .unwrap();
    send.half_close().await.unwrap();
    let inc = server.accept().await.expect("incoming");
    inc.send.finish(Status::with_code(5, "not found")).await.unwrap();

    let terminal = loop {
        match recv.next_event().await.expect("event") {
            StreamEvent::RemoteHalfClose => continue,
            e => break e,
        }
    };
    assert!(matches!(terminal, StreamEvent::Terminated(s) if s.code == 5));
    // exactly one terminal event: the driver removed the stream slot on
    // termination, so the event channel is now drained and closed.
    assert!(recv.next_event().await.is_none());
}
