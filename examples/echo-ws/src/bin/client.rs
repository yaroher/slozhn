use bytes::Bytes;
use slozhn_proto::testing::v1::echo_client::EchoClient;
use slozhn_proto::testing::v1::{Count, Msg};
use tokio_stream::StreamExt;
use tonic::Request;

#[tokio::main]
async fn main() {
    let url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "ws://127.0.0.1:50052/rpc".into());
    let mut client = EchoClient::new(echo_ws::auto_channel(url));

    // unary
    let resp = client
        .unary(Request::new(Msg { payload: Bytes::from_static(b"hello over ws") }))
        .await
        .expect("unary");
    println!("unary: {:?}", String::from_utf8_lossy(&resp.into_inner().payload));

    // server-stream
    let mut s = client
        .server_stream(Request::new(Count { n: 5 }))
        .await
        .expect("server_stream")
        .into_inner();
    print!("server_stream:");
    while let Some(m) = s.next().await {
        print!(" {}", m.expect("msg").payload[0]);
    }
    println!();

    // bidi: 3 rounds
    let (tx, rx) = tokio::sync::mpsc::channel::<Msg>(1);
    let mut inbound = client
        .bidi(Request::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
        .await
        .expect("bidi")
        .into_inner();
    for i in 1u64..=3 {
        tx.send(Msg { payload: Bytes::copy_from_slice(&i.to_le_bytes()) })
            .await
            .unwrap();
        let m = inbound.next().await.unwrap().unwrap();
        println!(
            "bidi: {i} -> {}",
            u64::from_le_bytes(m.payload.as_ref().try_into().unwrap())
        );
    }
    drop(tx);

    println!("done");
}
