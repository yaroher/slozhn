//! Browser side of the phase 4 e2e: the same tonic client as on native, on
//! top of the slozhn-ws wasm backend. Server: examples/echo-ws (native).
//! Run: ./examples/echo-wasm/run-browser-tests.sh

#![cfg(target_arch = "wasm32")]

pub fn auto_channel(url: String) -> slozhn::client::Channel {
    slozhn::client::builder(url).resume().build()
}
