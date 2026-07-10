//! The `slozhn::testing` harness: a real tonic client talking to real tonic
//! Routes in one runtime, no sockets. Exercises all three fidelity levels.

use std::pin::Pin;

use bytes::Bytes;
use futures::Stream;
use slozhn_proto::testing::v1::echo_client::EchoClient;
use slozhn_proto::testing::v1::echo_server::{Echo, EchoServer};
use slozhn_proto::testing::v1::{Count, Msg};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, Streaming};

struct EchoImpl;

type MsgStream = Pin<Box<dyn Stream<Item = Result<Msg, Status>> + Send>>;

#[tonic::async_trait]
impl Echo for EchoImpl {
    async fn unary(&self, req: Request<Msg>) -> Result<Response<Msg>, Status> {
        let echo_meta = req.metadata().get("x-echo").cloned();
        let mut resp = Response::new(req.into_inner());
        if let Some(v) = echo_meta {
            resp.metadata_mut().insert("x-echo-back", v);
        }
        Ok(resp)
    }

    type ServerStreamStream = MsgStream;
    async fn server_stream(&self, req: Request<Count>) -> Result<Response<MsgStream>, Status> {
        let n = req.into_inner().n;
        let s = tokio_stream::iter((0..n).map(|i| Ok(Msg { payload: Bytes::from(vec![i as u8]) })));
        Ok(Response::new(Box::pin(s)))
    }

    async fn client_stream(&self, req: Request<Streaming<Msg>>) -> Result<Response<Msg>, Status> {
        let mut inbound = req.into_inner();
        let mut sum: u64 = 0;
        while let Some(m) = inbound.next().await {
            sum += u64::from(m?.payload[0]);
        }
        Ok(Response::new(Msg { payload: Bytes::copy_from_slice(&sum.to_le_bytes()) }))
    }

    type BidiStream = MsgStream;
    async fn bidi(&self, req: Request<Streaming<Msg>>) -> Result<Response<MsgStream>, Status> {
        let mut inbound = req.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            while let Some(Ok(m)) = inbound.next().await {
                let n = u64::from_le_bytes(m.payload.as_ref().try_into().unwrap());
                if tx
                    .send(Ok(Msg { payload: Bytes::copy_from_slice(&(n * 2).to_le_bytes()) }))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn fail(&self, _req: Request<Msg>) -> Result<Response<Msg>, Status> {
        let mut st = Status::invalid_argument("deliberate failure");
        st.metadata_mut().insert("x-fail-detail", "why-not".parse().unwrap());
        Err(st)
    }
}

fn routes() -> tonic::service::Routes {
    tonic::service::Routes::new(EchoServer::new(EchoImpl))
}

/// Streams with a pause between messages, so a break can land mid-stream
/// rather than after the server has already flushed everything.
struct SlowEcho;

#[tonic::async_trait]
impl Echo for SlowEcho {
    async fn unary(&self, req: Request<Msg>) -> Result<Response<Msg>, Status> {
        Ok(Response::new(req.into_inner()))
    }

    type ServerStreamStream = MsgStream;
    async fn server_stream(&self, req: Request<Count>) -> Result<Response<MsgStream>, Status> {
        let n = req.into_inner().n;
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            for i in 0..n {
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
                if tx.send(Ok(Msg { payload: Bytes::from(vec![i as u8]) })).await.is_err() {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn client_stream(&self, _: Request<Streaming<Msg>>) -> Result<Response<Msg>, Status> {
        Err(Status::unimplemented("not under test"))
    }
    type BidiStream = MsgStream;
    async fn bidi(&self, _: Request<Streaming<Msg>>) -> Result<Response<MsgStream>, Status> {
        Err(Status::unimplemented("not under test"))
    }
    async fn fail(&self, _: Request<Msg>) -> Result<Response<Msg>, Status> {
        Err(Status::unimplemented("not under test"))
    }
}

fn slow_routes() -> tonic::service::Routes {
    tonic::service::Routes::new(EchoServer::new(SlowEcho))
}

fn msg(payload: &'static [u8]) -> Request<Msg> {
    Request::new(Msg { payload: Bytes::from_static(payload) })
}

// ---------------------------------------------------------------------
// Level 1: direct frame pipe
// ---------------------------------------------------------------------

#[tokio::test]
async fn unary_with_metadata_over_frame_pipe() {
    let mut client = EchoClient::new(slozhn::testing::channel(routes()));

    let mut req = msg(b"hello");
    req.metadata_mut().insert("x-echo", "ping".parse().unwrap());
    let resp = client.unary(req).await.unwrap();

    assert_eq!(resp.metadata().get("x-echo-back").unwrap(), "ping");
    assert_eq!(resp.into_inner().payload.as_ref(), b"hello");
}

#[tokio::test]
async fn all_four_streaming_kinds_over_frame_pipe() {
    let mut client = EchoClient::new(slozhn::testing::channel(routes()));

    // server streaming
    let mut s = client.server_stream(Request::new(Count { n: 5 })).await.unwrap().into_inner();
    let mut got = 0;
    while let Some(m) = s.next().await {
        m.unwrap();
        got += 1;
    }
    assert_eq!(got, 5);

    // client streaming
    let outbound = tokio_stream::iter((1..=3u8).map(|i| Msg { payload: Bytes::from(vec![i]) }));
    let resp = client.client_stream(Request::new(outbound)).await.unwrap();
    let sum = u64::from_le_bytes(resp.into_inner().payload.as_ref().try_into().unwrap());
    assert_eq!(sum, 6);

    // bidi
    let outbound = tokio_stream::iter(
        (1..=3u64).map(|i| Msg { payload: Bytes::copy_from_slice(&i.to_le_bytes()) }),
    );
    let mut s = client.bidi(Request::new(outbound)).await.unwrap().into_inner();
    let mut doubled = Vec::new();
    while let Some(m) = s.next().await {
        let m = m.unwrap();
        doubled.push(u64::from_le_bytes(m.payload.as_ref().try_into().unwrap()));
    }
    assert_eq!(doubled, vec![2, 4, 6]);
}

#[tokio::test]
async fn error_status_and_trailer_metadata_survive() {
    let mut client = EchoClient::new(slozhn::testing::channel(routes()));

    let err = client.fail(msg(b"x")).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(err.message(), "deliberate failure");
    assert_eq!(err.metadata().get("x-fail-detail").unwrap(), "why-not");
}

// ---------------------------------------------------------------------
// Level 2: real byte codec (the literal bufconn equivalent)
// ---------------------------------------------------------------------

#[tokio::test]
async fn unary_over_byte_pipe_through_the_codec() {
    let mut client = EchoClient::new(slozhn::testing::channel_over_bytes(routes()));
    let resp = client.unary(msg(b"through-the-codec")).await.unwrap();
    assert_eq!(resp.into_inner().payload.as_ref(), b"through-the-codec");
}

#[tokio::test]
async fn large_payload_round_trips_through_the_codec() {
    let mut client = EchoClient::new(slozhn::testing::channel_over_bytes(routes()));
    // spans many frames: exercises encode/decode + flow control for real
    let big = vec![7u8; 512 * 1024];
    let resp = client
        .unary(Request::new(Msg { payload: Bytes::from(big.clone()) }))
        .await
        .unwrap();
    assert_eq!(resp.into_inner().payload.len(), big.len());
}

// ---------------------------------------------------------------------
// Level 3: session layer + simulated network break
// ---------------------------------------------------------------------

#[tokio::test]
async fn session_channel_works_before_any_break() {
    let (channel, _breaker) = slozhn::testing::session_channel(routes()).await;
    let mut client = EchoClient::new(channel);
    let resp = client.unary(msg(b"session")).await.unwrap();
    assert_eq!(resp.into_inner().payload.as_ref(), b"session");
}

#[tokio::test]
async fn rpc_after_a_break_succeeds_via_resume() {
    let (channel, breaker) = slozhn::testing::session_channel(routes()).await;
    let mut client = EchoClient::new(channel);

    client.unary(msg(b"before")).await.unwrap();
    assert_eq!(breaker.connect_count(), 1, "one physical connection so far");

    breaker.kill(); // the physical transport dies, in-memory

    // the session layer reconnects through the factory and resumes the
    // logical connection — a new RPC goes through with no retry from us
    let resp = client.unary(msg(b"after")).await.unwrap();
    assert_eq!(resp.into_inner().payload.as_ref(), b"after");
    assert_eq!(breaker.connect_count(), 2, "the break forced a real reconnect");
}

#[tokio::test]
async fn server_stream_survives_a_break_mid_stream() {
    // The whole point of the session layer: an in-flight stream is NOT torn
    // down by a physical disconnect. Without resume this stream would end
    // with UNAVAILABLE after the kill.
    let (channel, breaker) = slozhn::testing::session_channel(slow_routes()).await;
    let mut client = EchoClient::new(channel);

    let mut stream = client
        .server_stream(Request::new(Count { n: 5 }))
        .await
        .unwrap()
        .into_inner();

    let first = stream.next().await.expect("first message").expect("ok");
    assert_eq!(first.payload.as_ref(), &[0u8]);

    breaker.kill(); // network dies with the stream still open

    // the remaining messages arrive across the resume — replayed if the
    // server had already sent them, delivered normally otherwise
    let mut seen = 1;
    while let Some(m) = stream.next().await {
        m.expect("stream survives the break");
        seen += 1;
    }
    assert_eq!(seen, 5, "all five messages arrived despite the break");
    assert_eq!(
        breaker.connect_count(),
        2,
        "the stream really did cross a physical reconnect"
    );
}
