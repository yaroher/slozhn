//! Browser app core: a gRPC client via slozhn, exposing a thin wasm-bindgen
//! API to the TS UI. (A typed proto boundary is a separate deferred task;
//! manual bindgen here.)

#![cfg(target_arch = "wasm32")]

use bytes::Bytes;
use futures::StreamExt;
use slozhn_proto::testing::v1::echo_client::EchoClient;
use slozhn_proto::testing::v1::{Count, Msg};
use wasm_bindgen::prelude::*;

type Stack = slozhn::middleware::TraceService<slozhn::client::Channel>;

#[wasm_bindgen]
pub struct Core {
    client: EchoClient<Stack>,
    channel: slozhn::client::Channel,
}

/// Route `tracing` output (incl. TraceLayer RPC logs) to the browser console.
/// Call once, before creating cores.
#[wasm_bindgen]
pub fn init_logging() {
    console_error_panic_hook::set_once();
    tracing_wasm::set_as_global_default();
}

fn status_err(s: tonic::Status) -> JsValue {
    JsValue::from_str(&format!("{:?}: {}", s.code(), s.message()))
}

#[wasm_bindgen]
impl Core {
    /// Channel with resume: streams survive network breaks. RPCs are wrapped
    /// in TraceLayer — with `init_logging()` every call lands in the console.
    #[wasm_bindgen(constructor)]
    pub fn new(url: String) -> Core {
        use tower::Layer as _;
        let channel = slozhn::client::builder(url).resume().build();
        let stack = slozhn::middleware::TraceLayer::client().layer(channel.clone());
        Core { client: EchoClient::new(stack), channel }
    }

    /// Subscribe to connection state changes; the callback receives a
    /// human-readable string ("connected", "backoff 3.2s (attempt 2)", ...).
    pub fn on_state(&self, cb: js_sys::Function) {
        use slozhn::frame::transport::ConnState;

        let mut rx = self.channel.state();
        wasm_bindgen_futures::spawn_local(async move {
            loop {
                let text = match &*rx.borrow_and_update() {
                    ConnState::Idle => "idle".to_string(),
                    ConnState::Connecting => "connecting".to_string(),
                    ConnState::Connected => "connected".to_string(),
                    ConnState::Disconnected => "disconnected".to_string(),
                    ConnState::Backoff { delay, attempt } => {
                        format!("backoff {:.1}s (attempt {attempt})", delay.as_secs_f32())
                    }
                };
                let _ = cb.call1(&JsValue::NULL, &JsValue::from_str(&text));
                if rx.changed().await.is_err() {
                    break;
                }
            }
        });
    }

    /// Punch through a backoff wait and try to reconnect immediately.
    pub fn reconnect_now(&self) {
        self.channel.reconnect_now();
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
