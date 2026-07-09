//! Regression tests for a batch of hardening fixes:
//! 1. receive-side flow control is now enforced (stream.rs::on_message).
//! 3. SendHalf now has a Drop impl so an abandoned stream doesn't leak its
//!    slot forever.
//! 4. outbound Message payloads are checked against MAX_MESSAGE_SIZE before
//!    ever reaching the wire.
//! 5. the pre-Hello handshake read now has a deadline (slowloris guard).
//! 6. connection recv credit for buffered-but-unread messages is refunded
//!    on every stream teardown path, so early-cancelled streams don't
//!    permanently shrink the peer's connection send window.
//! 7. Open/Headers metadata is capped (bytes + entry count) — a peer can no
//!    longer smuggle unbounded metadata through the stream-limit guard.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};

use slozhn_frame::connection::{Config, bind};
use slozhn_frame::error::{ConnError, ProtocolError, StreamError};
use slozhn_frame::ext::{MetadataExt, StatusExt};
use slozhn_frame::ids::Side;
use slozhn_frame::loopback;
use slozhn_frame::proto::v1::{
    Frame, Hello, Message, Metadata, MetadataEntry, Open, Status, frame, metadata_entry,
};
use slozhn_frame::stream::StreamEvent;

/// Wraps a frame transport and records every frame it *sends* (outbound, in
/// send order) into a shared log — lets a test assert on the exact wire
/// output (e.g. a connection-level WindowUpdate) without reaching into
/// driver internals. Mirrors `tests/conformance.rs::Sniff`.
struct Sniff<T> {
    inner: T,
    out: Arc<Mutex<Vec<Frame>>>,
}

impl<T> futures::Stream for Sniff<T>
where
    T: futures::Stream<Item = Frame> + Unpin,
{
    type Item = Frame;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Frame>> {
        std::pin::Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl<T> futures::Sink<Frame> for Sniff<T>
where
    T: futures::Sink<Frame, Error = slozhn_frame::error::TransportClosed> + Unpin,
{
    type Error = slozhn_frame::error::TransportClosed;
    fn poll_ready(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::pin::Pin::new(&mut self.inner).poll_ready(cx)
    }
    fn start_send(mut self: std::pin::Pin<&mut Self>, item: Frame) -> Result<(), Self::Error> {
        self.out.lock().unwrap().push(item.clone());
        std::pin::Pin::new(&mut self.inner).start_send(item)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::pin::Pin::new(&mut self.inner).poll_close(cx)
    }
}

// ---------------------------------------------------------------------
// 1. Receive-side flow control is enforced
// ---------------------------------------------------------------------

#[tokio::test]
async fn flow_control_violation_kills_connection() {
    // Driven by hand (raw wire) because a conformant peer, going through the
    // public API, would never ignore its own granted window.
    let cfg = Config {
        initial_stream_window: 16,
        initial_connection_window: 4096,
        ..Config::default()
    };
    let (a, mut raw) = loopback::pair();
    let (server, drv) = bind(Side::Server, cfg, a);
    let handle = tokio::spawn(drv.run());

    let _ = raw.next().await.expect("server's own hello, drained");
    raw.send(Frame {
        stream_id: 0,
        seq: 0,
        kind: Some(frame::Kind::Hello(Hello {
            version: 1,
            initial_stream_window: 65536,
            initial_connection_window: 65536,
            ..Default::default()
        })),
    })
    .await
    .unwrap();

    // Stream 3: a normal, fully in-window message still passes and is
    // delivered to the application — the fix must not reject legitimate
    // traffic.
    raw.send(Frame {
        stream_id: 3,
        seq: 0,
        kind: Some(frame::Kind::Open(Open {
            method: "/svc.S/Normal".into(),
            metadata: Some(Metadata::empty()),
        })),
    })
    .await
    .unwrap();
    let mut inc_normal = server.accept().await.expect("incoming normal stream");
    raw.send(Frame {
        stream_id: 3,
        seq: 0,
        kind: Some(frame::Kind::Message(Message {
            payload: Bytes::from(vec![0u8; 4]),
            compressed: false,
        })),
    })
    .await
    .unwrap();
    assert!(matches!(
        inc_normal.recv.next_event().await,
        Some(StreamEvent::Message(b)) if b.len() == 4
    ));
    // reading the message auto-credits the window back to the peer — drain
    // that WindowUpdate off the wire so it doesn't get mistaken for the
    // GoAway frame later.
    let credit = raw.next().await.expect("credit for the normal message");
    assert!(matches!(credit.kind, Some(frame::Kind::WindowUpdate(_))));

    // Stream 1: the borrow rule allows one message to overshoot a positive
    // window (16 -> -4), but a peer that keeps sending once the window is
    // already <= 0 is a flow-control violation.
    raw.send(Frame {
        stream_id: 1,
        seq: 0,
        kind: Some(frame::Kind::Open(Open {
            method: "/svc.S/Flood".into(),
            metadata: Some(Metadata::empty()),
        })),
    })
    .await
    .unwrap();
    let _inc_flood = server.accept().await.expect("incoming flood stream");

    raw.send(Frame {
        stream_id: 1,
        seq: 0,
        kind: Some(frame::Kind::Message(Message {
            payload: Bytes::from(vec![0u8; 20]), // window 16 -> -4, allowed (borrow)
            compressed: false,
        })),
    })
    .await
    .unwrap();
    raw.send(Frame {
        stream_id: 1,
        seq: 0,
        kind: Some(frame::Kind::Message(Message {
            payload: Bytes::from_static(b"x"), // window still <= 0 — violation
            compressed: false,
        })),
    })
    .await
    .unwrap();

    let err = handle.await.unwrap().unwrap_err();
    assert!(matches!(
        err,
        ConnError::Protocol(ProtocolError::FlowControlViolation(1))
    ));

    // connection-fatal: the last frame on the wire is GoAway with the
    // ProtocolError code, same as any other protocol violation.
    let last = raw.next().await.expect("goaway frame");
    match last.kind {
        Some(frame::Kind::GoAway(ga)) => assert_eq!(ga.code, 1),
        other => panic!("expected GoAway, got {other:?}"),
    }
}

// ---------------------------------------------------------------------
// 3. SendHalf Drop reclaims the slot and notifies the peer
// ---------------------------------------------------------------------

#[tokio::test]
async fn dropped_send_half_opener_cancels_and_frees_slot() {
    let client_cfg = Config {
        max_streams: 1,
        ..Config::default()
    };
    let (a, b) = loopback::pair();
    let (client, cd) = bind(Side::Client, client_cfg, a);
    let (server, sd) = bind(Side::Server, Config::default(), b);
    tokio::spawn(cd.run());
    tokio::spawn(sd.run());
    let _keep = (client.clone(), server.clone());

    let (send, recv) = client
        .open("/svc.S/Abandoned".into(), Metadata::empty())
        .await
        .unwrap();
    let mut inc = server.accept().await.expect("incoming");

    // the handler drops SendHalf without ever calling half_close/finish/cancel
    drop(send);

    // the peer learns the stream ended
    assert!(matches!(
        inc.recv.next_event().await,
        Some(StreamEvent::Cancelled)
    ));

    drop(recv);
    // the client's slot (max_streams=1) was reclaimed — a new open succeeds
    let (_send2, _recv2) = client
        .open("/svc.S/After".into(), Metadata::empty())
        .await
        .unwrap();
}

#[tokio::test]
async fn dropped_send_half_acceptor_emits_status_and_frees_slot() {
    let server_cfg = Config {
        max_streams: 1,
        ..Config::default()
    };
    let (a, b) = loopback::pair();
    let (client, cd) = bind(Side::Client, Config::default(), a);
    let (server, sd) = bind(Side::Server, server_cfg, b);
    tokio::spawn(cd.run());
    tokio::spawn(sd.run());
    let _keep = (client.clone(), server.clone());

    let (_send, mut recv) = client
        .open("/svc.S/Abandoned".into(), Metadata::empty())
        .await
        .unwrap();
    let inc = server.accept().await.expect("incoming");

    // the acceptor's handler drops SendHalf without finish/cancel
    drop(inc.send);

    // the opener sees a definite terminal, not a silent hang
    let terminal = loop {
        match recv.next_event().await.expect("event") {
            StreamEvent::RemoteHalfClose => continue,
            e => break e,
        }
    };
    assert!(matches!(terminal, StreamEvent::Terminated(_)));

    drop(inc.recv);
    // the server's slot (max_streams=1) was reclaimed — a new inbound open succeeds
    let (_send2, mut recv2) = client
        .open("/svc.S/After".into(), Metadata::empty())
        .await
        .unwrap();
    let inc2 = server.accept().await.expect("incoming after reclaim");
    inc2.send.send_headers(Metadata::empty()).unwrap();
    assert!(matches!(
        recv2.next_event().await,
        Some(StreamEvent::Headers(_))
    ));
}

// ---------------------------------------------------------------------
// 4. Outbound Message is checked against MAX_MESSAGE_SIZE
// ---------------------------------------------------------------------

#[tokio::test]
async fn oversized_send_fails_locally_without_touching_wire() {
    let (a, b) = loopback::pair();
    let (client, cd) = bind(Side::Client, Config::default(), a);
    let (server, sd) = bind(Side::Server, Config::default(), b);
    tokio::spawn(cd.run());
    tokio::spawn(sd.run());
    let _keep = (client.clone(), server.clone());

    let (send, _recv) = client
        .open("/svc.S/Big".into(), Metadata::empty())
        .await
        .unwrap();
    let _inc = server.accept().await.expect("incoming");

    let oversized = Bytes::from(vec![0u8; slozhn_frame::MAX_MESSAGE_SIZE + 1]);
    let err = send.send(oversized).await.unwrap_err();
    assert!(matches!(err, StreamError::Connection(_)));

    // the connection itself is unaffected: a fresh ping still round-trips
    client.ping().await.unwrap();
}

// ---------------------------------------------------------------------
// 5. Hello handshake timeout (slowloris guard)
// ---------------------------------------------------------------------

#[tokio::test]
async fn handshake_times_out_when_peer_never_sends_hello() {
    let cfg = Config {
        handshake_timeout: Duration::from_millis(100),
        ..Config::default()
    };
    let (a, mut raw) = loopback::pair();
    let (_client, drv) = bind(Side::Client, cfg, a);
    let handle = tokio::spawn(drv.run());

    // drain our own Hello, then go silent — never send one back
    let _ = raw.next().await.expect("client's own hello, drained");

    let err = tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("run() must not hang past the handshake timeout")
        .unwrap()
        .unwrap_err();
    assert!(matches!(err, ConnError::HandshakeTimeout));
}

// ---------------------------------------------------------------------
// 6. Connection recv credit for unread buffered messages is refunded on
//    stream teardown (early cancel), not leaked forever.
// ---------------------------------------------------------------------

#[tokio::test]
async fn early_cancel_refunds_connection_recv_credit_and_avoids_stall() {
    // client_cfg.initial_connection_window is the window the CLIENT grants
    // the peer (server) to send on this connection. Keep it small and the
    // per-stream window generous, so the connection-level credit is the
    // only thing that can ever make a server send stall.
    let client_cfg = Config {
        initial_stream_window: 1_000_000,
        initial_connection_window: 2048,
        ..Config::default()
    };
    let (a, b) = loopback::pair();
    let out = Arc::new(Mutex::new(Vec::new()));
    let a = Sniff { inner: a, out: out.clone() };
    let (client, cd) = bind(Side::Client, client_cfg, a);
    let (server, sd) = bind(Side::Server, Config::default(), b);
    tokio::spawn(cd.run());
    tokio::spawn(sd.run());
    let _keep = (client.clone(), server.clone());

    let msg_len = 512usize;
    // 12 * 512 = 6144 bytes, ~3x the 2048-byte connection window: without a
    // credit refund on early cancel, the server's 5th send onward would
    // stall forever waiting for a WindowUpdate that never comes.
    let cycles = 12;

    async fn assert_send_completes(send: &slozhn_frame::connection::SendHalf, payload: Bytes) {
        tokio::time::timeout(Duration::from_secs(2), send.send(payload))
            .await
            .expect(
                "send stalled — connection recv credit for unread messages was not \
                 refunded on stream teardown",
            )
            .unwrap();
    }

    for i in 0..cycles {
        let (send, recv) = client
            .open(format!("/svc.S/Early{i}"), Metadata::empty())
            .await
            .unwrap();
        let mut inc = server.accept().await.expect("incoming");

        // server streams one large message the app will never read
        assert_send_completes(&inc.send, Bytes::from(vec![0u8; msg_len])).await;

        // app abandons the RPC without ever reading the message — a normal
        // early-cancel of a server-streaming RPC. The opener side's
        // RecvHalf::Drop sends a real Cancel (cancel_on_drop = true).
        drop(recv);
        drop(send);

        // server observes the cancel (its own teardown path is exercised
        // too, but this test targets the client's conn_recv_credit)
        let saw_cancel = loop {
            match inc.recv.next_event().await {
                Some(StreamEvent::Cancelled) => break true,
                Some(StreamEvent::RemoteHalfClose) => continue,
                Some(other) => panic!("unexpected event: {other:?}"),
                None => break false,
            }
        };
        assert!(saw_cancel, "server must observe the client's early cancel");
    }

    // The connection must still be able to send after many such cycles:
    // open one more stream and stream several more large messages through
    // cleanly, each within a bounded timeout.
    let (_send, mut recv) = client
        .open("/svc.S/After".into(), Metadata::empty())
        .await
        .unwrap();
    let inc = server.accept().await.expect("incoming after cycles");
    for _ in 0..4 {
        assert_send_completes(&inc.send, Bytes::from(vec![9u8; msg_len])).await;
    }
    inc.send.finish(Status::ok()).await.unwrap();

    let mut received = 0;
    loop {
        match recv.next_event().await.expect("event") {
            StreamEvent::Message(b) => {
                assert_eq!(b.len(), msg_len);
                received += 1;
            }
            StreamEvent::Terminated(_) => break,
            StreamEvent::RemoteHalfClose => continue,
            other => panic!("unexpected event: {other:?}"),
        }
    }
    assert_eq!(received, 4);

    // And directly on the wire: the fix must have produced at least one
    // connection-level (stream_id 0) WindowUpdate refunding the unread
    // bytes from the early-cancelled streams — without it, no such frame
    // would ever be emitted for messages the app never read.
    let conn_credit: u32 = out
        .lock()
        .unwrap()
        .iter()
        .filter_map(|f| match &f.kind {
            Some(frame::Kind::WindowUpdate(wu)) if f.stream_id == 0 => Some(wu.increment),
            _ => None,
        })
        .sum();
    assert!(
        conn_credit > 0,
        "expected at least one connection-level WindowUpdate refunding unread message bytes"
    );
    // upper bound: never refund more than was actually consumed — the
    // early-cancelled cycles' unread bytes, plus the 4 final messages the
    // app *did* read normally (which also credit the connection window via
    // the existing read-path, Command::Credit) — no double-crediting.
    assert!(conn_credit as usize <= cycles * msg_len + 4 * msg_len);
}

// ---------------------------------------------------------------------
// 7. Open/Headers metadata size + entry-count cap
// ---------------------------------------------------------------------

#[tokio::test]
async fn open_with_oversized_metadata_is_rejected_stream_level() {
    let (a, mut raw) = loopback::pair();
    let (server, drv) = bind(Side::Server, Config::default(), a);
    let handle = tokio::spawn(drv.run());

    let _ = raw.next().await.expect("server's own hello, drained");
    raw.send(Frame {
        stream_id: 0,
        seq: 0,
        kind: Some(frame::Kind::Hello(Hello {
            version: 1,
            initial_stream_window: 65536,
            initial_connection_window: 65536,
            ..Default::default()
        })),
    })
    .await
    .unwrap();

    // an Open whose metadata exceeds the default byte cap (16 KiB) must be
    // rejected with a stream-level Status(RESOURCE_EXHAUSTED=8) — not a
    // connection error — and must not create a slot the app ever sees.
    let big_value = "x".repeat(32 * 1024);
    raw.send(Frame {
        stream_id: 1,
        seq: 0,
        kind: Some(frame::Kind::Open(Open {
            method: "/svc.S/BigMetadata".into(),
            metadata: Some(Metadata {
                entries: vec![MetadataEntry {
                    key: "k".into(),
                    value: Some(metadata_entry::Value::Ascii(big_value)),
                }],
            }),
        })),
    })
    .await
    .unwrap();

    let status = raw.next().await.expect("status frame for oversized metadata");
    match status.kind {
        Some(frame::Kind::Status(s)) => assert_eq!(s.code, 8),
        other => panic!("expected Status, got {other:?}"),
    }

    // an Open exceeding the entry-count cap (128) is rejected the same way,
    // even though every individual entry is tiny.
    let many_entries: Vec<MetadataEntry> = (0..200)
        .map(|i| MetadataEntry {
            key: format!("k{i}"),
            value: Some(metadata_entry::Value::Ascii("v".into())),
        })
        .collect();
    raw.send(Frame {
        stream_id: 3,
        seq: 0,
        kind: Some(frame::Kind::Open(Open {
            method: "/svc.S/TooManyEntries".into(),
            metadata: Some(Metadata { entries: many_entries }),
        })),
    })
    .await
    .unwrap();
    let status = raw.next().await.expect("status frame for too many entries");
    match status.kind {
        Some(frame::Kind::Status(s)) => assert_eq!(s.code, 8),
        other => panic!("expected Status, got {other:?}"),
    }

    // the connection itself is unaffected, and neither oversized-metadata
    // stream was ever handed to the app: prove liveness with a normal,
    // small-metadata Open that works end to end.
    raw.send(Frame {
        stream_id: 5,
        seq: 0,
        kind: Some(frame::Kind::Open(Open {
            method: "/svc.S/Small".into(),
            metadata: Some(Metadata::ascii("k", "v")),
        })),
    })
    .await
    .unwrap();
    let inc = server.accept().await.expect("normal small-metadata open still works");
    assert_eq!(inc.method, "/svc.S/Small");

    drop(inc.recv);
    drop(inc.send);
    drop(server);
    let _ = handle.await;
}

// ---------------------------------------------------------------------
// 8. A late frame on a closed stream whose id was evicted from the
//    bounded reset set is a legal race (id <= highest opened), NOT a
//    fatal UnknownStream — high stream churn must not tear the
//    connection down.
// ---------------------------------------------------------------------

#[tokio::test]
async fn late_frame_on_evicted_reset_id_is_not_fatal() {
    let (a, mut raw) = loopback::pair();
    let (server, drv) = bind(Side::Server, Config::default(), a);
    let handle = tokio::spawn(drv.run());

    let _ = raw.next().await.expect("server's own hello, drained");
    raw.send(Frame {
        stream_id: 0,
        seq: 0,
        kind: Some(frame::Kind::Hello(Hello {
            version: 1,
            initial_stream_window: 65536,
            initial_connection_window: 65536,
            ..Default::default()
        })),
    })
    .await
    .unwrap();

    // Open stream 5 and close it (peer half-closes, server finishes) so the
    // server records highest_remote_open = 5, then the id becomes closed.
    raw.send(Frame {
        stream_id: 5,
        seq: 0,
        kind: Some(frame::Kind::Open(Open {
            method: "/svc.S/Once".into(),
            metadata: Some(Metadata::empty()),
        })),
    })
    .await
    .unwrap();
    let inc = server.accept().await.expect("incoming");
    inc.send.finish(Status::ok()).await.unwrap();
    let status = raw.next().await.expect("status frame");
    assert!(matches!(status.kind, Some(frame::Kind::Status(_))));

    // A late Message arrives on stream 3 — an id below highest_remote_open (5)
    // that the server never explicitly tracked as reset (simulating an id
    // evicted from the bounded reset set under churn). It must be dropped
    // silently, not treated as a fatal UnknownStream.
    raw.send(Frame {
        stream_id: 3,
        seq: 0,
        kind: Some(frame::Kind::Message(Message {
            payload: Bytes::from_static(b"late"),
            compressed: false,
        })),
    })
    .await
    .unwrap();

    // Prove the connection is still alive: a Ping still gets a Pong.
    raw.send(Frame {
        stream_id: 0,
        seq: 0,
        kind: Some(frame::Kind::Ping(slozhn_frame::proto::v1::Ping { opaque: 7 })),
    })
    .await
    .unwrap();
    let pong = raw.next().await.expect("connection alive — pong arrives");
    assert!(matches!(pong.kind, Some(frame::Kind::Pong(p)) if p.opaque == 7));

    drop(inc.recv);
    drop(server);
    let _ = handle.await;
}
