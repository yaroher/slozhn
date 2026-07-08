use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use futures::Stream;
use slozhn_frame::connection::{bind, Config};
use slozhn_frame::ids::Side;
use slozhn_frame::loopback;
use slozhn_proto::testing::v1::echo_client::EchoClient;
use slozhn_proto::testing::v1::echo_server::{Echo, EchoServer};
use slozhn_proto::testing::v1::{Count, Msg};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

struct EchoImpl;

type MsgStream = Pin<Box<dyn Stream<Item = Result<Msg, Status>> + Send>>;

#[tonic::async_trait]
impl Echo for EchoImpl {
    async fn unary(&self, req: Request<Msg>) -> Result<Response<Msg>, Status> {
        let echo_meta = req.metadata().get("x-echo").cloned();
        let msg = req.into_inner();
        let mut resp = Response::new(msg);
        if let Some(v) = echo_meta {
            resp.metadata_mut().insert("x-echo-back", v);
        }
        Ok(resp)
    }

    type ServerStreamStream = MsgStream;
    async fn server_stream(&self, req: Request<Count>) -> Result<Response<MsgStream>, Status> {
        let n = req.into_inner().n;
        let s =
            tokio_stream::iter((0..n).map(|i| Ok(Msg { payload: Bytes::from(vec![i as u8]) })));
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
            while let Some(m) = inbound.next().await {
                let Ok(m) = m else { break };
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

fn setup() -> EchoClient<slozhn_client::Channel> {
    let (a, b) = loopback::pair();
    let (cconn, cd) = bind(Side::Client, Config::default(), a);
    let (sconn, sd) = bind(Side::Server, Config::default(), b);
    tokio::spawn(async move {
        if let Err(e) = cd.run().await {
            eprintln!("client driver died: {e}");
        }
    });
    tokio::spawn(async move {
        if let Err(e) = sd.run().await {
            eprintln!("server driver died: {e}");
        }
    });

    let routes = tonic::service::Routes::new(EchoServer::new(EchoImpl));
    tokio::spawn(slozhn_server::serve(sconn, routes));

    let spawner: slozhn_client::Spawner = Arc::new(|f| {
        tokio::spawn(f);
    });
    EchoClient::new(slozhn_client::Channel::new(cconn, spawner))
}

#[tokio::test]
async fn unary_with_metadata() {
    let mut client = setup();
    let mut req = Request::new(Msg { payload: Bytes::from_static(b"hi") });
    req.metadata_mut().insert("x-echo", "ping".parse().unwrap());
    let resp = client.unary(req).await.unwrap();
    assert_eq!(resp.metadata().get("x-echo-back").unwrap(), "ping");
    assert_eq!(resp.into_inner().payload.as_ref(), b"hi");
}

#[tokio::test]
async fn server_streaming_five() {
    let mut client = setup();
    let mut s = client
        .server_stream(Request::new(Count { n: 5 }))
        .await
        .unwrap()
        .into_inner();
    let mut got = Vec::new();
    while let Some(m) = s.next().await {
        got.push(m.unwrap().payload[0]);
    }
    assert_eq!(got, vec![0, 1, 2, 3, 4]);
}

#[tokio::test]
async fn client_streaming_sum() {
    let mut client = setup();
    let outbound = tokio_stream::iter((1..=10u8).map(|i| Msg { payload: Bytes::from(vec![i]) }));
    let resp = client.client_stream(Request::new(outbound)).await.unwrap();
    assert_eq!(
        u64::from_le_bytes(resp.into_inner().payload.as_ref().try_into().unwrap()),
        55
    );
}

#[tokio::test]
async fn bidi_doubles() {
    let mut client = setup();
    let (tx, rx) = tokio::sync::mpsc::channel::<Msg>(1);
    let mut inbound = client
        .bidi(Request::new(ReceiverStream::new(rx)))
        .await
        .unwrap()
        .into_inner();
    for i in 1u64..=20 {
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
async fn error_status_with_metadata_propagates() {
    let mut client = setup();
    let err = client
        .fail(Request::new(Msg { payload: Bytes::new() }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(err.message(), "deliberate failure");
    assert_eq!(err.metadata().get("x-fail-detail").unwrap(), "why-not");
}

#[tokio::test]
async fn big_payload_crosses_flow_control() {
    // 1 MiB >> 64 KiB windows: exercises the WindowUpdate machinery under real tonic
    let mut client = setup();
    let payload = Bytes::from(vec![0xAB; 1024 * 1024]);
    let resp = client
        .unary(Request::new(Msg { payload: payload.clone() }))
        .await
        .unwrap();
    assert_eq!(resp.into_inner().payload, payload);
}
