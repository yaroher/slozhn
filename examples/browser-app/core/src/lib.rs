//! Browser app core: a gRPC client via slozhn, exposing a thin wasm-bindgen
//! API to the TS UI. (A typed proto boundary is a separate deferred task;
//! manual bindgen here.)

#![cfg(target_arch = "wasm32")]

use bytes::Bytes;
use futures::StreamExt;
use slozhn_proto::testing::v1::echo_client::EchoClient;
use slozhn_proto::testing::v1::{Count, Msg};
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Core {
    client: EchoClient<slozhn::client::Channel>,
}

fn status_err(s: tonic::Status) -> JsValue {
    JsValue::from_str(&format!("{:?}: {}", s.code(), s.message()))
}

#[wasm_bindgen]
impl Core {
    /// Channel with resume: streams survive network breaks.
    #[wasm_bindgen(constructor)]
    pub fn new(url: String) -> Core {
        let channel = slozhn::client::builder(url).resume().build();
        Core { client: EchoClient::new(channel) }
    }

    /// Unary echo.
    pub async fn unary(&mut self, text: String) -> Result<String, JsValue> {
        let resp = self
            .client
            .unary(tonic::Request::new(Msg { payload: Bytes::from(text.into_bytes()) }))
            .await
            .map_err(status_err)?;
        Ok(String::from_utf8_lossy(&resp.into_inner().payload).into_owned())
    }

    /// Server-stream: n messages, each delivered to a JS callback.
    pub async fn stream(&mut self, n: u32, on_item: js_sys::Function) -> Result<(), JsValue> {
        let mut s = self
            .client
            .server_stream(tonic::Request::new(Count { n }))
            .await
            .map_err(status_err)?
            .into_inner();
        while let Some(item) = s.next().await {
            let m = item.map_err(status_err)?;
            let _ = on_item.call1(&JsValue::NULL, &JsValue::from_f64(f64::from(m.payload[0])));
        }
        Ok(())
    }
}
