#![cfg(target_arch = "wasm32")]

use bytes::Bytes;
use futures::StreamExt;
use slozhn_proto::testing::v1::echo_client::EchoClient;
use slozhn_proto::testing::v1::{Count, Msg};
use tonic::Request;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

const SERVER: &str = "ws://127.0.0.1:50123/rpc";

fn client() -> EchoClient<slozhn::client::Channel> {
    EchoClient::new(echo_wasm::auto_channel(SERVER.to_owned()))
}

#[wasm_bindgen_test]
async fn unary_from_browser() {
    let mut client = client();
    let mut req = Request::new(Msg { payload: Bytes::from_static(b"from-browser") });
    req.metadata_mut().insert("x-echo", "wasm".parse().unwrap());
    let resp = client.unary(req).await.expect("unary");
    assert_eq!(resp.metadata().get("x-echo-back").unwrap(), "wasm");
    assert_eq!(resp.into_inner().payload.as_ref(), b"from-browser");
}

#[wasm_bindgen_test]
async fn server_streaming_from_browser() {
    let mut client = client();
    let mut s = client
        .server_stream(Request::new(Count { n: 5 }))
        .await
        .expect("server_stream")
        .into_inner();
    let mut got = vec![];
    while let Some(m) = s.next().await {
        got.push(m.expect("msg").payload[0]);
    }
    assert_eq!(got, vec![0, 1, 2, 3, 4]);
}

#[wasm_bindgen_test]
async fn bidi_from_browser() {
    use futures::SinkExt;

    let mut client = client();
    let (mut tx, rx) = futures::channel::mpsc::channel::<Msg>(1);
    let mut inbound = client.bidi(Request::new(rx)).await.expect("bidi").into_inner();
    for i in 1u64..=5 {
        tx.send(Msg { payload: Bytes::copy_from_slice(&i.to_le_bytes()) })
            .await
            .unwrap();
        let m = inbound.next().await.unwrap().unwrap();
        assert_eq!(u64::from_le_bytes(m.payload.as_ref().try_into().unwrap()), i * 2);
    }
    drop(tx);
    assert!(inbound.next().await.is_none());
}

#[wasm_bindgen_test]
async fn error_status_from_browser() {
    let mut client = client();
    let err = client
        .fail(Request::new(Msg { payload: Bytes::new() }))
        .await
        .expect_err("must fail");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(err.metadata().get("x-fail-detail").unwrap(), "why-not");
}

#[wasm_bindgen_test]
async fn big_payload_from_browser() {
    let mut client = client();
    let payload = Bytes::from(vec![0xCD; 256 * 1024]);
    let resp = client
        .unary(Request::new(Msg { payload: payload.clone() }))
        .await
        .expect("unary big");
    assert_eq!(resp.into_inner().payload, payload);
}
