use slozhn_frame::connection::{Config, bind};
use slozhn_frame::ids::Side;
use slozhn_frame::loopback;

#[tokio::test]
async fn handshake_and_ping() {
    let (a, b) = loopback::pair();
    let (client, client_drv) = bind(Side::Client, Config::default(), a);
    let (server, server_drv) = bind(Side::Server, Config::default(), b);
    let ch = tokio::spawn(client_drv.run());
    let sh = tokio::spawn(server_drv.run());

    client.ping().await.unwrap();
    server.ping().await.unwrap();

    drop(client);
    drop(server);
    // after all handles are dropped the drivers exit (Ok) or see the other
    // side's transport close (Err(TransportClosed)) — both outcomes are
    // correct; what matters is that both finished
    let _ = ch.await.unwrap();
    let _ = sh.await.unwrap();
}

#[tokio::test]
async fn unary_echo() {
    use bytes::Bytes;
    use slozhn_frame::ext::{MetadataExt, StatusExt};
    use slozhn_frame::proto::v1::{Metadata, Status};
    use slozhn_frame::stream::StreamEvent;

    let (a, b) = loopback::pair();
    let (client, cd) = bind(Side::Client, Config::default(), a);
    let (server, sd) = bind(Side::Server, Config::default(), b);
    tokio::spawn(cd.run());
    tokio::spawn(sd.run());

    // keep the server connection alive until the end of the test: dropping
    // all handles = connection close (FIN), and server_task finishes before
    // the client
    let _server_keepalive = server.clone();

    let server_task = tokio::spawn(async move {
        let mut inc = server.accept().await.expect("incoming stream");
        assert_eq!(inc.method, "/echo.Echo/Do");
        inc.send.send_headers(Metadata::empty()).unwrap();
        // read the single message
        let payload = loop {
            match inc.recv.next_event().await.expect("event") {
                StreamEvent::Message(b) => break b,
                StreamEvent::RemoteHalfClose => continue,
                other => panic!("unexpected {other:?}"),
            }
        };
        inc.send.send(payload).await.unwrap();
        inc.send.finish(Status::ok()).await.unwrap();
    });

    let (send, mut recv) = client
        .open("/echo.Echo/Do".into(), Metadata::empty())
        .await
        .unwrap();
    send.send(Bytes::from_static(b"hello")).await.unwrap();
    send.half_close().await.unwrap();

    assert!(matches!(
        recv.next_event().await,
        Some(StreamEvent::Headers(_))
    ));
    assert!(matches!(
        recv.next_event().await,
        Some(StreamEvent::Message(b)) if b.as_ref() == b"hello"
    ));
    // allow RemoteHalfClose before Terminated
    let terminal = loop {
        match recv.next_event().await.expect("event") {
            StreamEvent::RemoteHalfClose => continue,
            e => break e,
        }
    };
    assert!(matches!(terminal, StreamEvent::Terminated(s) if s.code == 0));
    server_task.await.unwrap();
}

#[tokio::test]
async fn server_streaming() {
    use bytes::Bytes;
    use slozhn_frame::ext::{MetadataExt, StatusExt};
    use slozhn_frame::proto::v1::{Metadata, Status};
    use slozhn_frame::stream::StreamEvent;

    let (a, b) = loopback::pair();
    let (client, cd) = bind(Side::Client, Config::default(), a);
    let (server, sd) = bind(Side::Server, Config::default(), b);
    tokio::spawn(cd.run());
    tokio::spawn(sd.run());
    let _keep = server.clone();

    let server_task = tokio::spawn(async move {
        let mut inc = server.accept().await.expect("incoming");
        inc.send.send_headers(Metadata::empty()).unwrap();
        // drain the request to the end, then reply with a stream
        loop {
            match inc.recv.next_event().await.expect("event") {
                StreamEvent::Message(b) => assert_eq!(b.as_ref(), b"req"),
                StreamEvent::RemoteHalfClose => break,
                other => panic!("unexpected {other:?}"),
            }
        }
        for i in 0u8..5 {
            inc.send.send(Bytes::copy_from_slice(&[i])).await.unwrap();
        }
        inc.send.finish(Status::ok()).await.unwrap();
    });

    let (send, mut recv) = client
        .open("/stream.Svc/ServerStream".into(), Metadata::empty())
        .await
        .unwrap();
    send.send(Bytes::from_static(b"req")).await.unwrap();
    send.half_close().await.unwrap();

    assert!(matches!(
        recv.next_event().await,
        Some(StreamEvent::Headers(_))
    ));
    for i in 0u8..5 {
        assert!(matches!(
            recv.next_event().await,
            Some(StreamEvent::Message(b)) if b.as_ref() == [i]
        ));
    }
    let terminal = loop {
        match recv.next_event().await.expect("event") {
            StreamEvent::RemoteHalfClose => continue,
            e => break e,
        }
    };
    assert!(matches!(terminal, StreamEvent::Terminated(s) if s.code == 0));
    server_task.await.unwrap();
}

#[tokio::test]
async fn client_streaming() {
    use bytes::Bytes;
    use slozhn_frame::ext::{MetadataExt, StatusExt};
    use slozhn_frame::proto::v1::{Metadata, Status};
    use slozhn_frame::stream::StreamEvent;

    let (a, b) = loopback::pair();
    let (client, cd) = bind(Side::Client, Config::default(), a);
    let (server, sd) = bind(Side::Server, Config::default(), b);
    tokio::spawn(cd.run());
    tokio::spawn(sd.run());
    let _keep = server.clone();

    let server_task = tokio::spawn(async move {
        let mut inc = server.accept().await.expect("incoming");
        inc.send.send_headers(Metadata::empty()).unwrap();
        let mut sum: u64 = 0;
        loop {
            match inc.recv.next_event().await.expect("event") {
                StreamEvent::Message(b) => sum += u64::from(b[0]),
                StreamEvent::RemoteHalfClose => break,
                other => panic!("unexpected {other:?}"),
            }
        }
        inc.send
            .send(Bytes::copy_from_slice(&sum.to_le_bytes()))
            .await
            .unwrap();
        inc.send.finish(Status::ok()).await.unwrap();
    });

    let (send, mut recv) = client
        .open("/stream.Svc/ClientStream".into(), Metadata::empty())
        .await
        .unwrap();
    for i in 1u8..=10 {
        send.send(Bytes::copy_from_slice(&[i])).await.unwrap();
    }
    send.half_close().await.unwrap();

    assert!(matches!(
        recv.next_event().await,
        Some(StreamEvent::Headers(_))
    ));
    let sum = match recv.next_event().await {
        Some(StreamEvent::Message(b)) => u64::from_le_bytes(b.as_ref().try_into().unwrap()),
        other => panic!("unexpected {other:?}"),
    };
    assert_eq!(sum, 55);
    let terminal = loop {
        match recv.next_event().await.expect("event") {
            StreamEvent::RemoteHalfClose => continue,
            e => break e,
        }
    };
    assert!(matches!(terminal, StreamEvent::Terminated(s) if s.code == 0));
    server_task.await.unwrap();
}

#[tokio::test]
async fn bidi_interleaved() {
    use bytes::Bytes;
    use slozhn_frame::ext::{MetadataExt, StatusExt};
    use slozhn_frame::proto::v1::{Metadata, Status};
    use slozhn_frame::stream::StreamEvent;

    let (a, b) = loopback::pair();
    let (client, cd) = bind(Side::Client, Config::default(), a);
    let (server, sd) = bind(Side::Server, Config::default(), b);
    tokio::spawn(cd.run());
    tokio::spawn(sd.run());
    let _keep = server.clone();

    let server_task = tokio::spawn(async move {
        let mut inc = server.accept().await.expect("incoming");
        inc.send.send_headers(Metadata::empty()).unwrap();
        loop {
            match inc.recv.next_event().await.expect("event") {
                StreamEvent::Message(b) => {
                    let n = u64::from_le_bytes(b.as_ref().try_into().unwrap());
                    inc.send
                        .send(Bytes::copy_from_slice(&(n * 2).to_le_bytes()))
                        .await
                        .unwrap();
                }
                StreamEvent::RemoteHalfClose => break,
                other => panic!("unexpected {other:?}"),
            }
        }
        inc.send.finish(Status::ok()).await.unwrap();
    });

    let (send, mut recv) = client
        .open("/stream.Svc/Bidi".into(), Metadata::empty())
        .await
        .unwrap();

    assert!(matches!(
        recv.next_event().await,
        Some(StreamEvent::Headers(_))
    ));
    // strict interleaving: the reply to i arrives BEFORE i+1 is sent —
    // catches ordering bugs
    for i in 1u64..=20 {
        send.send(Bytes::copy_from_slice(&i.to_le_bytes()))
            .await
            .unwrap();
        match recv.next_event().await {
            Some(StreamEvent::Message(b)) => {
                assert_eq!(u64::from_le_bytes(b.as_ref().try_into().unwrap()), i * 2);
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    send.half_close().await.unwrap();

    let terminal = loop {
        match recv.next_event().await.expect("event") {
            StreamEvent::RemoteHalfClose => continue,
            e => break e,
        }
    };
    assert!(matches!(terminal, StreamEvent::Terminated(s) if s.code == 0));
    server_task.await.unwrap();
}

#[tokio::test]
async fn cancel_reaches_peer() {
    use slozhn_frame::ext::MetadataExt;
    use slozhn_frame::proto::v1::Metadata;
    use slozhn_frame::stream::StreamEvent;

    let (a, b) = loopback::pair();
    let (client, cd) = bind(Side::Client, Config::default(), a);
    let (server, sd) = bind(Side::Server, Config::default(), b);
    tokio::spawn(cd.run());
    tokio::spawn(sd.run());
    let _keep = (client.clone(), server.clone());

    let (send, _recv) = client
        .open("/svc.S/Cancelled".into(), Metadata::empty())
        .await
        .unwrap();
    let mut inc = server.accept().await.expect("incoming");
    send.cancel();

    assert!(matches!(
        inc.recv.next_event().await,
        Some(StreamEvent::Cancelled)
    ));
    assert!(inc.recv.next_event().await.is_none());
}

#[tokio::test]
async fn protocol_violation_goaway() {
    use futures::{SinkExt, StreamExt};
    use slozhn_frame::proto::v1::{Frame, Hello, Message, frame};

    let (a, mut raw) = loopback::pair();
    let (_client, drv) = bind(Side::Client, Config::default(), a);
    let handle = tokio::spawn(drv.run());

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
    let _their_hello = raw.next().await.expect("hello");

    // Message on nonexistent stream 42 — protocol violation
    raw.send(Frame {
        stream_id: 42,
        seq: 0,
        kind: Some(frame::Kind::Message(Message {
            payload: bytes::Bytes::from_static(b"x"),
            compressed: false,
        })),
    })
    .await
    .unwrap();

    let err = handle.await.unwrap().unwrap_err();
    assert!(matches!(
        err,
        slozhn_frame::error::ConnError::Protocol(
            slozhn_frame::error::ProtocolError::UnknownStream(42)
        )
    ));
    // the last frame is GoAway with the ProtocolError code
    let last = raw.next().await.expect("goaway frame");
    match last.kind {
        Some(frame::Kind::GoAway(ga)) => assert_eq!(ga.code, 1),
        other => panic!("expected GoAway, got {other:?}"),
    }
}

#[tokio::test]
async fn goaway_blocks_new_opens() {
    use bytes::Bytes;
    use slozhn_frame::error::{GoAwayCode, OpenError};
    use slozhn_frame::ext::{MetadataExt, StatusExt};
    use slozhn_frame::proto::v1::{Metadata, Status};
    use slozhn_frame::stream::StreamEvent;

    let (a, b) = loopback::pair();
    let (client, cd) = bind(Side::Client, Config::default(), a);
    let (server, sd) = bind(Side::Server, Config::default(), b);
    tokio::spawn(cd.run());
    tokio::spawn(sd.run());
    let _keep = (client.clone(), server.clone());

    // a stream opened BEFORE goaway runs to completion
    let (send, mut recv) = client
        .open("/svc.S/BeforeGoAway".into(), Metadata::empty())
        .await
        .unwrap();
    let mut inc = server.accept().await.expect("incoming");

    server.go_away(GoAwayCode::Graceful);

    // wait until GoAway reaches the client: new opens start getting rejected
    loop {
        match client
            .open("/svc.S/AfterGoAway".into(), Metadata::empty())
            .await
        {
            Err(OpenError::GoingAway) => break,
            Ok((s, _r)) => {
                // GoAway still in flight; close the probe stream and retry
                s.cancel();
                tokio::task::yield_now().await;
            }
            Err(other) => panic!("unexpected {other:?}"),
        }
    }

    // an active stream keeps working through goaway
    send.send(Bytes::from_static(b"ping")).await.unwrap();
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

    assert!(matches!(
        recv.next_event().await,
        Some(StreamEvent::Headers(_))
    ));
    assert!(matches!(
        recv.next_event().await,
        Some(StreamEvent::Message(b)) if b.as_ref() == b"ping"
    ));

    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(1), server.accept())
            .await
            .expect("goaway should drain accept loop")
            .is_none()
    );
}

#[tokio::test]
async fn send_blocks_without_window_and_resumes_on_read() {
    use bytes::Bytes;
    use slozhn_frame::ext::MetadataExt;
    use slozhn_frame::proto::v1::Metadata;
    use slozhn_frame::stream::StreamEvent;
    use std::time::Duration;

    let cfg = Config {
        initial_stream_window: 16,
        initial_connection_window: 1024,
    };
    let (a, b) = loopback::pair();
    let (client, cd) = bind(Side::Client, cfg.clone(), a);
    let (server, sd) = bind(Side::Server, cfg, b);
    tokio::spawn(cd.run());
    tokio::spawn(sd.run());
    let _keep = (client.clone(), server.clone());

    let (send, _recv) = client
        .open("/svc.S/Slow".into(), Metadata::empty())
        .await
        .unwrap();
    let mut inc = server.accept().await.expect("incoming");

    // the first message eats the entire stream window (16 → 0)
    send.send(Bytes::from(vec![0u8; 16])).await.unwrap();

    // the second must block until the server reads the first
    let second = send.send(Bytes::from(vec![1u8; 16]));
    tokio::pin!(second);
    tokio::select! {
        _ = &mut second => panic!("send must block: window exhausted"),
        _ = tokio::time::sleep(Duration::from_millis(50)) => {}
    }

    // the server reads → WindowUpdate → the second send completes
    assert!(matches!(
        inc.recv.next_event().await,
        Some(StreamEvent::Message(b)) if b.len() == 16
    ));
    second.await.unwrap();
    assert!(matches!(
        inc.recv.next_event().await,
        Some(StreamEvent::Message(b)) if b.as_ref() == vec![1u8; 16]
    ));
}

#[tokio::test]
async fn connection_window_caps_across_streams() {
    use bytes::Bytes;
    use slozhn_frame::ext::MetadataExt;
    use slozhn_frame::proto::v1::Metadata;
    use slozhn_frame::stream::StreamEvent;
    use std::time::Duration;

    let cfg = Config {
        initial_stream_window: 1024,
        initial_connection_window: 16,
    };
    let (a, b) = loopback::pair();
    let (client, cd) = bind(Side::Client, cfg.clone(), a);
    let (server, sd) = bind(Side::Server, cfg, b);
    tokio::spawn(cd.run());
    tokio::spawn(sd.run());
    let _keep = (client.clone(), server.clone());

    let (send1, _recv1) = client
        .open("/svc.S/One".into(), Metadata::empty())
        .await
        .unwrap();
    let (send2, _recv2) = client
        .open("/svc.S/Two".into(), Metadata::empty())
        .await
        .unwrap();
    let mut inc1 = server.accept().await.expect("incoming 1");
    let _inc2 = server.accept().await.expect("incoming 2");

    // the first stream eats the CONNECTION window (16 → 0)
    send1.send(Bytes::from(vec![0u8; 16])).await.unwrap();

    // the second stream blocks even though its stream window is free
    let blocked = send2.send(Bytes::from(vec![2u8; 4]));
    tokio::pin!(blocked);
    tokio::select! {
        _ = &mut blocked => panic!("send must block: connection window exhausted"),
        _ = tokio::time::sleep(Duration::from_millis(50)) => {}
    }

    // the server drains the first stream → connection WindowUpdate → unblocked
    assert!(matches!(
        inc1.recv.next_event().await,
        Some(StreamEvent::Message(b)) if b.len() == 16
    ));
    blocked.await.unwrap();
}

#[tokio::test]
async fn pre_negotiated_bind_works() {
    use bytes::Bytes;
    use slozhn_frame::connection::bind_pre_negotiated;
    use slozhn_frame::ext::{MetadataExt, StatusExt};
    use slozhn_frame::proto::v1::{Hello, Metadata, Status};
    use slozhn_frame::stream::StreamEvent;

    // "external" handshake: the sides simply agreed on the windows
    let hello = Hello {
        version: 1,
        initial_stream_window: 65536,
        initial_connection_window: 65536,
        ..Default::default()
    };

    let (a, b) = loopback::pair();
    let (client, cd) = bind_pre_negotiated(Side::Client, Config::default(), hello.clone(), a);
    let (server, sd) = bind_pre_negotiated(Side::Server, Config::default(), hello, b);
    tokio::spawn(cd.run());
    tokio::spawn(sd.run());
    let _keep = server.clone();

    let server_task = tokio::spawn(async move {
        let mut inc = server.accept().await.expect("incoming");
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
    });

    let (send, mut recv) = client
        .open("/echo.Echo/Do".into(), Metadata::empty())
        .await
        .unwrap();
    send.send(Bytes::from_static(b"pre-negotiated"))
        .await
        .unwrap();
    send.half_close().await.unwrap();

    assert!(matches!(
        recv.next_event().await,
        Some(StreamEvent::Headers(_))
    ));
    assert!(matches!(
        recv.next_event().await,
        Some(StreamEvent::Message(b)) if b.as_ref() == b"pre-negotiated"
    ));
    server_task.await.unwrap();
}

#[tokio::test]
async fn dropping_recv_half_cancels_stream() {
    use slozhn_frame::ext::MetadataExt;
    use slozhn_frame::proto::v1::Metadata;
    use slozhn_frame::stream::StreamEvent;

    let (a, b) = loopback::pair();
    let (client, cd) = bind(Side::Client, Config::default(), a);
    let (server, sd) = bind(Side::Server, Config::default(), b);
    tokio::spawn(cd.run());
    tokio::spawn(sd.run());
    let _keep = (client.clone(), server.clone());

    let (send, recv) = client
        .open("/svc.S/Dropped".into(), Metadata::empty())
        .await
        .unwrap();
    let mut inc = server.accept().await.expect("incoming");
    drop(recv); // the client abandoned the RPC
    drop(send);

    assert!(matches!(
        inc.recv.next_event().await,
        Some(StreamEvent::Cancelled)
    ));
}

#[tokio::test]
async fn version_mismatch_kills_connection() {
    use futures::{SinkExt, StreamExt};
    use slozhn_frame::proto::v1::{Frame, Hello, frame};

    let (a, mut raw) = loopback::pair();
    let (_client, drv) = bind(Side::Client, Config::default(), a);
    let handle = tokio::spawn(drv.run());

    // manually send a Hello with a foreign version
    raw.send(Frame {
        stream_id: 0,
        seq: 0,
        kind: Some(frame::Kind::Hello(Hello {
            version: 99,
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    let _ = raw.next().await; // its Hello

    let err = handle.await.unwrap().unwrap_err();
    assert!(matches!(
        err,
        slozhn_frame::error::ConnError::Protocol(
            slozhn_frame::error::ProtocolError::VersionMismatch(99)
        )
    ));
}
