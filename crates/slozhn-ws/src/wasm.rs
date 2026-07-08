//! wasm32 backend: web_sys::WebSocket lives inside the spawn_local glue,
//! only channels (Send) stick out. onmessage → Bytes → mpsc immediately
//! (spec §6); outgoing — bounded channel → send_with_u8_array.

use std::cell::RefCell;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::channel::{mpsc, oneshot};
use futures::{Sink, Stream, StreamExt};
use slozhn_frame::TransportClosed;
use wasm_bindgen::closure::Closure;
use wasm_bindgen::JsCast;
use web_sys::{BinaryType, MessageEvent, WebSocket};

use super::{WsConfig, WsError};

pub async fn connect(url: &str, config: WsConfig) -> Result<WsStream, WsError> {
    if !config.headers.is_empty() {
        return Err(WsError::Unsupported(
            "browser WebSocket cannot set headers; use query params or cookies",
        ));
    }
    let (in_tx, in_rx) = mpsc::unbounded::<Bytes>();
    let (out_tx, out_rx) = mpsc::channel::<Bytes>(32);
    let (ready_tx, ready_rx) = oneshot::channel::<Result<(), String>>();

    let url = url.to_owned();
    wasm_bindgen_futures::spawn_local(glue(url, in_tx, out_rx, ready_tx));

    match ready_rx.await {
        Ok(Ok(())) => Ok(WsStream { rx: in_rx, tx: out_tx }),
        Ok(Err(e)) => Err(WsError::Connect(e)),
        Err(_) => Err(WsError::Connect("glue task died".into())),
    }
}

/// The entire JS part: socket + closures + outgoing pump. Lives in spawn_local.
async fn glue(
    url: String,
    in_tx: mpsc::UnboundedSender<Bytes>,
    mut out_rx: mpsc::Receiver<Bytes>,
    ready: oneshot::Sender<Result<(), String>>,
) {
    let ws = match WebSocket::new(&url) {
        Ok(ws) => ws,
        Err(e) => {
            let _ = ready.send(Err(format!("{e:?}")));
            return;
        }
    };
    ws.set_binary_type(BinaryType::Arraybuffer);

    let (open_tx, open_rx) = oneshot::channel::<Result<(), String>>();
    let open_tx = Rc::new(RefCell::new(Some(open_tx)));

    let on_open = {
        let open_tx = open_tx.clone();
        Closure::<dyn FnMut()>::new(move || {
            if let Some(tx) = open_tx.borrow_mut().take() {
                let _ = tx.send(Ok(()));
            }
        })
    };
    let on_message = {
        let in_tx = in_tx.clone();
        Closure::<dyn FnMut(MessageEvent)>::new(move |ev: MessageEvent| {
            if let Ok(buf) = ev.data().dyn_into::<js_sys::ArrayBuffer>() {
                let bytes = Bytes::from(js_sys::Uint8Array::new(&buf).to_vec());
                let _ = in_tx.unbounded_send(bytes);
            }
        })
    };
    let on_error = {
        let open_tx = open_tx.clone();
        let in_tx = in_tx.clone();
        Closure::<dyn FnMut()>::new(move || {
            if let Some(tx) = open_tx.borrow_mut().take() {
                let _ = tx.send(Err("websocket error".into()));
            }
            in_tx.close_channel();
        })
    };
    let on_close = {
        let in_tx = in_tx.clone();
        Closure::<dyn FnMut()>::new(move || {
            in_tx.close_channel();
        })
    };
    ws.set_onopen(Some(on_open.as_ref().unchecked_ref()));
    ws.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
    ws.set_onerror(Some(on_error.as_ref().unchecked_ref()));
    ws.set_onclose(Some(on_close.as_ref().unchecked_ref()));

    // wait for open/error, report back to connect()
    let opened = matches!(open_rx.await, Ok(Ok(())));
    let _ = ready.send(if opened {
        Ok(())
    } else {
        Err("connect failed".into())
    });
    if !opened {
        return; // closures are dropped, the socket dies
    }

    // outgoing pump; channel end (drop WsStream) → close the socket
    while let Some(bytes) = out_rx.next().await {
        if ws.send_with_u8_array(&bytes).is_err() {
            break;
        }
    }
    let _ = ws.close();
    // closures live until this point — handlers stay valid for the socket's lifetime
    drop((on_open, on_message, on_error, on_close));
}

/// The outward half: channels only — `Send`, fit for `bind()`.
pub struct WsStream {
    rx: mpsc::UnboundedReceiver<Bytes>,
    tx: mpsc::Sender<Bytes>,
}

impl Stream for WsStream {
    type Item = Bytes;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Bytes>> {
        Pin::new(&mut self.rx).poll_next(cx)
    }
}

impl Sink<Bytes> for WsStream {
    type Error = TransportClosed;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.tx).poll_ready(cx).map_err(|_| TransportClosed)
    }
    fn start_send(mut self: Pin<&mut Self>, item: Bytes) -> Result<(), Self::Error> {
        Pin::new(&mut self.tx).start_send(item).map_err(|_| TransportClosed)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.tx).poll_flush(cx).map_err(|_| TransportClosed)
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.tx).poll_close(cx).map_err(|_| TransportClosed)
    }
}
