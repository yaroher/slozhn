//! Criterion benchmarks for slozhn-frame: pure codec cost, a full loopback
//! echo round-trip, and stream throughput under flow control.
//!
//! `cargo bench -p slozhn-frame` (a couple minutes with the sample sizes /
//! measurement times configured below).
//! `cargo bench -p slozhn-frame -- --test` runs everything once in test mode
//! (fast smoke check, no statistics).

use std::hint::black_box;
use std::time::Duration;

use bytes::Bytes;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use prost::Message as _;

use slozhn_frame::connection::{Config, bind};
use slozhn_frame::ext::{MetadataExt, StatusExt};
use slozhn_frame::ids::Side;
use slozhn_frame::loopback;
use slozhn_frame::proto::v1::{Frame, Message, Metadata, Status, frame};
use slozhn_frame::stream::StreamEvent;

const ONE_KIB: usize = 1024;
const FOUR_KIB: usize = 4096;
const BURST_MESSAGES: usize = 100;

fn bench_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(3));
    group.throughput(Throughput::Bytes(ONE_KIB as u64));

    let frame = Frame {
        stream_id: 1,
        seq: 0,
        kind: Some(frame::Kind::Message(Message {
            payload: Bytes::from(vec![0x42u8; ONE_KIB]),
            compressed: false,
        })),
    };

    group.bench_function("message_1kib_encode_decode_roundtrip", |b| {
        b.iter(|| {
            let bytes = frame.encode_to_vec();
            let decoded = Frame::decode(bytes.as_slice()).expect("valid frame");
            black_box(decoded);
        });
    });

    group.finish();
}

/// A tokio current-thread runtime + a bound client/server pair over
/// `loopback::pair()`, with a persistent accept-and-echo task on the server
/// side. Set up once outside the timed loop; each benchmark iteration only
/// pays for open + send + echo + recv.
fn setup_echo_pair() -> (tokio::runtime::Runtime, slozhn_frame::Connection) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build current-thread runtime");

    let client = rt.block_on(async {
        let (a, b) = loopback::pair();
        let (client, client_drv) = bind(Side::Client, Config::default(), a);
        let (server, server_drv) = bind(Side::Server, Config::default(), b);
        tokio::spawn(client_drv.run());
        tokio::spawn(server_drv.run());

        // persistent accept loop: echoes every message it receives back to
        // the opener, one worker task per accepted stream.
        tokio::spawn(async move {
            loop {
                let Some(mut inc) = server.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let _ = inc.send.send_headers(Metadata::empty());
                    loop {
                        match inc.recv.next_event().await {
                            Some(StreamEvent::Message(payload)) => {
                                if inc.send.send(payload).await.is_err() {
                                    break;
                                }
                            }
                            Some(StreamEvent::RemoteHalfClose) => break,
                            _ => break,
                        }
                    }
                    let _ = inc.send.finish(Status::ok()).await;
                });
            }
        });

        client
    });

    (rt, client)
}

fn bench_loopback_echo(c: &mut Criterion) {
    let (rt, client) = setup_echo_pair();
    let payload = Bytes::from(vec![0x7Au8; ONE_KIB]);

    let mut group = c.benchmark_group("loopback");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(3));
    group.throughput(Throughput::Bytes(ONE_KIB as u64));

    group.bench_function("echo_1kib_message", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (send, mut recv) = client
                    .open("/bench.Bench/Echo".into(), Metadata::empty())
                    .await
                    .expect("open stream");
                send.send(payload.clone()).await.expect("send message");
                send.half_close().await.expect("half close");

                // Headers, then the echoed message.
                loop {
                    match recv.next_event().await.expect("event") {
                        StreamEvent::Headers(_) => break,
                        StreamEvent::RemoteHalfClose => continue,
                        other => panic!("unexpected {other:?}"),
                    }
                }
                loop {
                    match recv.next_event().await.expect("event") {
                        StreamEvent::Message(b) => {
                            black_box(b);
                            break;
                        }
                        StreamEvent::RemoteHalfClose => continue,
                        other => panic!("unexpected {other:?}"),
                    }
                }
            });
        });
    });

    group.finish();
}

/// Same persistent-connection pattern as the echo benchmark, but the server
/// side just drains a burst of messages (crediting the flow-control window
/// back as it reads them) instead of echoing — this exercises the
/// window/credit machinery under sustained load rather than round-trip
/// latency.
fn setup_drain_pair() -> (tokio::runtime::Runtime, slozhn_frame::Connection) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build current-thread runtime");

    let client = rt.block_on(async {
        let (a, b) = loopback::pair();
        let (client, client_drv) = bind(Side::Client, Config::default(), a);
        let (server, server_drv) = bind(Side::Server, Config::default(), b);
        tokio::spawn(client_drv.run());
        tokio::spawn(server_drv.run());

        tokio::spawn(async move {
            loop {
                let Some(mut inc) = server.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    loop {
                        match inc.recv.next_event().await {
                            Some(StreamEvent::Message(_)) => {}
                            Some(StreamEvent::RemoteHalfClose) => break,
                            _ => break,
                        }
                    }
                    let _ = inc.send.finish(Status::ok()).await;
                });
            }
        });

        client
    });

    (rt, client)
}

fn bench_throughput(c: &mut Criterion) {
    let (rt, client) = setup_drain_pair();
    let payload = Bytes::from(vec![0xCDu8; FOUR_KIB]);

    let mut group = c.benchmark_group("throughput");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(3));
    group.throughput(Throughput::Bytes((BURST_MESSAGES * FOUR_KIB) as u64));

    group.bench_function("100x4kib_stream_burst", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (send, mut recv) = client
                    .open("/bench.Bench/Burst".into(), Metadata::empty())
                    .await
                    .expect("open stream");
                for _ in 0..BURST_MESSAGES {
                    send.send(payload.clone()).await.expect("send message");
                }
                send.half_close().await.expect("half close");

                loop {
                    match recv.next_event().await.expect("event") {
                        StreamEvent::Terminated(_) => break,
                        StreamEvent::RemoteHalfClose | StreamEvent::Headers(_) => continue,
                        other => panic!("unexpected {other:?}"),
                    }
                }
            });
        });
    });

    group.finish();
}

criterion_group!(benches, bench_codec, bench_loopback_echo, bench_throughput);
criterion_main!(benches);
