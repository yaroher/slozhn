//! Protocol fuzzing: random byte soup must never panic the decoder, and
//! random (mostly-invalid) frame sequences must never panic the connection
//! actor — only ever return `Err(ConnError)` or keep the driver running.

use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use proptest::prelude::*;

use slozhn_frame::connection::{Config, bind};
use slozhn_frame::ids::Side;
use slozhn_frame::loopback;
use slozhn_frame::proto::v1::{
    Ack, Cancel, Frame, GoAway, HalfClose, Hello, Message, Metadata, Open, Ping, Pong, Status,
    WindowUpdate, frame,
};

/// A handful of small, cheap-to-generate frame kinds covering every variant
/// of `frame::Kind` except `Hello` (sent once, up front, to complete the
/// handshake) — fuzzing a second Hello is covered by `arb_kind` including it
/// too, since a duplicate Hello is itself an interesting protocol violation.
fn arb_kind() -> impl Strategy<Value = frame::Kind> {
    prop_oneof![
        Just(frame::Kind::Hello(Hello {
            version: 1,
            initial_stream_window: 65536,
            initial_connection_window: 65536,
            ..Default::default()
        })),
        any::<u64>().prop_map(|opaque| frame::Kind::Ping(Ping { opaque })),
        any::<u64>().prop_map(|opaque| frame::Kind::Pong(Pong { opaque })),
        Just(frame::Kind::Open(Open {
            method: "/fuzz.Svc/Do".into(),
            metadata: Some(Metadata::default()),
        })),
        proptest::collection::vec(any::<u8>(), 0..32).prop_map(|payload| {
            frame::Kind::Message(Message {
                payload: Bytes::from(payload),
                compressed: false,
            })
        }),
        Just(frame::Kind::HalfClose(HalfClose {})),
        (any::<u32>(), ".{0,16}").prop_map(|(code, message)| {
            frame::Kind::Status(Status {
                code,
                message,
                trailers: Some(Metadata::default()),
            })
        }),
        Just(frame::Kind::Cancel(Cancel {})),
        any::<u32>().prop_map(|increment| frame::Kind::WindowUpdate(WindowUpdate { increment })),
        (any::<u64>(), any::<u32>()).prop_map(|(last_stream_id, code)| {
            frame::Kind::GoAway(GoAway {
                last_stream_id,
                code,
                message: String::new(),
            })
        }),
        any::<u64>().prop_map(|last_seq| frame::Kind::Ack(Ack { last_seq })),
    ]
}

fn arb_frame() -> impl Strategy<Value = Frame> {
    (0..8u64, arb_kind()).prop_map(|(stream_id, kind)| Frame {
        stream_id,
        seq: 0,
        kind: Some(kind),
    })
}

/// Feed `frames` into the raw end of a bound-but-otherwise-untouched
/// connection driver and make sure it never panics. The driver either keeps
/// running (still reading, possibly having ignored garbage) or terminates
/// with `Err(ConnError)` — both are fine; a panic is the only failure.
async fn run_case(frames: Vec<Frame>) {
    let (a, mut raw) = loopback::pair();
    let (_client, drv) = bind(Side::Client, Config::default(), a);
    let handle = tokio::spawn(drv.run());

    // A valid Hello first, so the driver gets past the handshake and into
    // the part of the state machine we actually want to fuzz.
    let hello = Frame {
        stream_id: 0,
        seq: 0,
        kind: Some(frame::Kind::Hello(Hello {
            version: slozhn_frame::PROTOCOL_VERSION,
            initial_stream_window: 65536,
            initial_connection_window: 65536,
            ..Default::default()
        })),
    };
    if raw.send(hello).await.is_ok() {
        // drain the driver's own Hello reply, if it still shows up in time
        let _ = tokio::time::timeout(Duration::from_millis(200), raw.next()).await;
    }

    for f in frames {
        // A per-frame timeout: if the driver stopped consuming (errored out
        // and exited, or is backpressured on a full outbound channel), stop
        // feeding rather than hang the whole fuzz case.
        match tokio::time::timeout(Duration::from_millis(50), raw.send(f)).await {
            Ok(Ok(())) => {}
            _ => break,
        }
    }
    drop(raw);

    // The property under test: no panic. `JoinHandle::await` surfaces a
    // panic inside the driver as `JoinError::is_panic()` — resurface it so
    // the test harness reports it as a failure with its original message.
    match tokio::time::timeout(Duration::from_millis(500), handle).await {
        Ok(Ok(_conn_result)) => {}
        Ok(Err(join_err)) => {
            if join_err.is_panic() {
                std::panic::resume_unwind(join_err.into_panic());
            }
        }
        Err(_elapsed) => {} // driver still running — acceptable outcome
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// `Frame::decode` must never panic on arbitrary bytes, valid protobuf
    /// or not.
    #[test]
    fn decode_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..2048)) {
        use prost::Message as _;
        let _ = Frame::decode(bytes.as_slice());
    }

    /// The connection actor must never panic on an arbitrary sequence of
    /// (mostly protocol-violating) frames.
    #[test]
    fn driver_survives_random_frames(frames in proptest::collection::vec(arb_frame(), 0..40)) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build current-thread runtime");
        rt.block_on(run_case(frames));
    }
}
