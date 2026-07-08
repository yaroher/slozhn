use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;

use bytes::Bytes;
use futures::{Sink, SinkExt, Stream, StreamExt};
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::error::{ConnError, GoAwayCode, OpenError, ProtocolError, StreamError, TransportClosed};
use crate::flow::Window;
use crate::ids::{Side, StreamIdAllocator};
use crate::proto::v1::{
    frame, Frame, GoAway, HalfClose, Headers, Hello, Message, Metadata, Open, Ping, Pong,
    Status, WindowUpdate,
};
use crate::stream::{StreamEvent, StreamState};
use crate::{DEFAULT_WINDOW, PROTOCOL_VERSION};

/// How many recently closed stream ids we remember to tell a legal race
/// (frames in flight after our terminal) apart from a protocol error.
const RESET_IDS_CAP: usize = 1024;

#[derive(Debug, Clone)]
pub struct Config {
    /// Our per-stream recv window (announced in Hello).
    pub initial_stream_window: u32,
    /// Our connection recv window (announced in Hello).
    pub initial_connection_window: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            initial_stream_window: DEFAULT_WINDOW,
            initial_connection_window: DEFAULT_WINDOW,
        }
    }
}

pub(crate) enum Command {
    Ping {
        done: oneshot::Sender<()>,
    },
    Open {
        method: String,
        metadata: Metadata,
        reply: oneshot::Sender<Result<(SendHalf, RecvHalf), OpenError>>,
    },
    Send {
        stream_id: u64,
        payload: Bytes,
        done: oneshot::Sender<Result<(), StreamError>>,
    },
    SendHeaders {
        stream_id: u64,
        metadata: Metadata,
    },
    HalfClose {
        stream_id: u64,
        done: oneshot::Sender<Result<(), StreamError>>,
    },
    Finish {
        stream_id: u64,
        status: Status,
        done: oneshot::Sender<Result<(), StreamError>>,
    },
    Cancel {
        stream_id: u64,
    },
    Credit {
        stream_id: u64,
        bytes: u32,
    },
    GoAway {
        code: GoAwayCode,
    },
}

/// Sending half of a stream.
pub struct SendHalf {
    stream_id: u64,
    is_opener: bool,
    cmd_tx: mpsc::UnboundedSender<Command>,
}

/// Receiving half of a stream: ordered events.
///
/// On the opener side, dropping without having seen a terminal event cancels
/// the RPC (sends Cancel). On the accepting side, drop is a no-op: its
/// terminal is `finish`.
pub struct RecvHalf {
    stream_id: u64,
    events: mpsc::UnboundedReceiver<StreamEvent>,
    cmd_tx: mpsc::UnboundedSender<Command>,
    saw_terminal: bool,
    cancel_on_drop: bool,
}

/// Incoming stream on the accepting side.
pub struct Incoming {
    pub method: String,
    pub metadata: Metadata,
    pub send: SendHalf,
    pub recv: RecvHalf,
}

impl SendHalf {
    async fn rt(
        &self,
        make: impl FnOnce(oneshot::Sender<Result<(), StreamError>>) -> Command,
    ) -> Result<(), StreamError> {
        let (done, wait) = oneshot::channel();
        self.cmd_tx
            .send(make(done))
            .map_err(|_| StreamError::Connection("closed".into()))?;
        wait.await
            .map_err(|_| StreamError::Connection("closed".into()))?
    }

    /// Send a message. Completes once the frame has gone to the transport
    /// (after waiting for the flow-control window).
    pub async fn send(&self, payload: Bytes) -> Result<(), StreamError> {
        let stream_id = self.stream_id;
        self.rt(move |done| Command::Send { stream_id, payload, done }).await
    }

    /// Initial response metadata. Accepting side only.
    pub fn send_headers(&self, metadata: Metadata) -> Result<(), StreamError> {
        if self.is_opener {
            return Err(StreamError::Connection("opener cannot send HEADERS".into()));
        }
        self.cmd_tx
            .send(Command::SendHeaders { stream_id: self.stream_id, metadata })
            .map_err(|_| StreamError::Connection("closed".into()))
    }

    /// "I'm done sending" (opener side).
    pub async fn half_close(self) -> Result<(), StreamError> {
        let stream_id = self.stream_id;
        self.rt(move |done| Command::HalfClose { stream_id, done }).await
    }

    /// Terminal Status (accepting side).
    pub async fn finish(self, status: Status) -> Result<(), StreamError> {
        let stream_id = self.stream_id;
        self.rt(move |done| Command::Finish { stream_id, status, done }).await
    }

    pub fn cancel(self) {
        let _ = self.cmd_tx.send(Command::Cancel { stream_id: self.stream_id });
    }
}

impl RecvHalf {
    /// Poll variant of [`Self::next_event`] — for `http_body::Body` impls.
    pub fn poll_next_event(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<StreamEvent>> {
        match self.events.poll_recv(cx) {
            std::task::Poll::Ready(Some(ev)) => {
                match &ev {
                    StreamEvent::Message(payload) => {
                        // the application consumed the message → credit the window (spec §5)
                        let _ = self.cmd_tx.send(Command::Credit {
                            stream_id: self.stream_id,
                            bytes: payload.len() as u32,
                        });
                    }
                    StreamEvent::Terminated(_) | StreamEvent::Cancelled => {
                        self.saw_terminal = true;
                    }
                    _ => {}
                }
                std::task::Poll::Ready(Some(ev))
            }
            std::task::Poll::Ready(None) => {
                self.saw_terminal = true; // channel drained — terminal already seen
                std::task::Poll::Ready(None)
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }

    /// Next stream event; `None` — stream closed and drained.
    pub async fn next_event(&mut self) -> Option<StreamEvent> {
        std::future::poll_fn(|cx| self.poll_next_event(cx)).await
    }
}

impl Drop for RecvHalf {
    fn drop(&mut self) {
        if self.cancel_on_drop && !self.saw_terminal {
            // stream dropped without a terminal — cancel; the actor silently
            // ignores already-closed streams (reset_ids)
            let _ = self.cmd_tx.send(Command::Cancel { stream_id: self.stream_id });
        }
    }
}

#[derive(Clone)]
pub struct Connection {
    cmd_tx: mpsc::UnboundedSender<Command>,
    accept_rx: Arc<Mutex<mpsc::UnboundedReceiver<Incoming>>>,
}

impl Connection {
    pub async fn ping(&self) -> Result<(), String> {
        let (done, wait) = oneshot::channel();
        self.cmd_tx
            .send(Command::Ping { done })
            .map_err(|_| "connection closed".to_owned())?;
        wait.await.map_err(|_| "connection closed".to_owned())
    }

    /// Open an outgoing stream.
    pub async fn open(
        &self,
        method: String,
        metadata: Metadata,
    ) -> Result<(SendHalf, RecvHalf), OpenError> {
        let (reply, wait) = oneshot::channel();
        self.cmd_tx
            .send(Command::Open { method, metadata, reply })
            .map_err(|_| OpenError::Connection("closed".into()))?;
        wait.await.map_err(|_| OpenError::Connection("closed".into()))?
    }

    /// Accept an incoming stream; `None` — connection closed.
    pub async fn accept(&self) -> Option<Incoming> {
        self.accept_rx.lock().await.recv().await
    }

    /// The driver is dead (connection closed) — commands are no longer accepted.
    pub fn is_closed(&self) -> bool {
        self.cmd_tx.is_closed()
    }

    /// Graceful shutdown: no new streams open, active ones run to completion.
    pub fn go_away(&self, code: GoAwayCode) {
        let _ = self.cmd_tx.send(Command::GoAway { code });
    }
}

pub struct ConnectionDriver<T> {
    side: Side,
    config: Config,
    /// Some = handshake already done externally (session layer), don't send Hello.
    pre_negotiated: Option<Hello>,
    transport: T,
    /// Weak: live strong senders remain only in user handles
    /// (Connection/SendHalf/RecvHalf) — dropping them closes cmd_rx
    /// and finishes the driver.
    cmd_tx: mpsc::WeakUnboundedSender<Command>,
    cmd_rx: mpsc::UnboundedReceiver<Command>,
    accept_tx: mpsc::UnboundedSender<Incoming>,
}

pub fn bind<T>(side: Side, config: Config, transport: T) -> (Connection, ConnectionDriver<T>)
where
    T: Stream<Item = Frame> + Sink<Frame, Error = TransportClosed> + Unpin + Send,
{
    bind_inner(side, config, None, transport)
}

/// Like [`bind`], but the handshake was already done externally (session
/// layer, spec §8): the driver neither sends nor waits for Hello and takes
/// the windows from `peer_hello`.
pub fn bind_pre_negotiated<T>(
    side: Side,
    config: Config,
    peer_hello: Hello,
    transport: T,
) -> (Connection, ConnectionDriver<T>)
where
    T: Stream<Item = Frame> + Sink<Frame, Error = TransportClosed> + Unpin + Send,
{
    bind_inner(side, config, Some(peer_hello), transport)
}

fn bind_inner<T>(
    side: Side,
    config: Config,
    pre_negotiated: Option<Hello>,
    transport: T,
) -> (Connection, ConnectionDriver<T>)
where
    T: Stream<Item = Frame> + Sink<Frame, Error = TransportClosed> + Unpin + Send,
{
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (accept_tx, accept_rx) = mpsc::unbounded_channel();
    let weak = cmd_tx.downgrade();
    (
        Connection {
            cmd_tx,
            accept_rx: Arc::new(Mutex::new(accept_rx)),
        },
        ConnectionDriver {
            side,
            config,
            pre_negotiated,
            transport,
            cmd_tx: weak,
            cmd_rx,
            accept_tx,
        },
    )
}

impl<T> ConnectionDriver<T>
where
    T: Stream<Item = Frame> + Sink<Frame, Error = TransportClosed> + Unpin + Send,
{
    pub async fn run(mut self) -> Result<(), ConnError> {
        // 1. Hello both ways (unless the session layer did the handshake)
        let peer_hello = match self.pre_negotiated.take() {
            Some(h) => h,
            None => {
                self.transport
                    .send(conn_frame(frame::Kind::Hello(Hello {
                        version: PROTOCOL_VERSION,
                        initial_stream_window: self.config.initial_stream_window,
                        initial_connection_window: self.config.initial_connection_window,
                        ..Default::default()
                    })))
                    .await?;

                let first = self.transport.next().await.ok_or(TransportClosed)?;
                let h = match first.kind {
                    Some(frame::Kind::Hello(h)) => h,
                    _ => return Err(ProtocolError::ExpectedHello.into()),
                };
                if h.version != PROTOCOL_VERSION {
                    return Err(ProtocolError::VersionMismatch(h.version).into());
                }
                h
            }
        };
        let mut actor =
            Actor::new(self.side, &self.config, peer_hello, self.cmd_tx, self.accept_tx);

        // 2. main loop
        loop {
            tokio::select! {
                cmd = self.cmd_rx.recv() => match cmd {
                    None => return Ok(()), // all handles dropped
                    Some(cmd) => {
                        for out in actor.on_command(cmd) {
                            self.transport.send(out).await?;
                        }
                    }
                },
                frame = self.transport.next() => match frame {
                    None => return Err(TransportClosed.into()),
                    Some(frame) => {
                        match actor.on_frame(frame) {
                            Ok(out) => {
                                for f in out {
                                    self.transport.send(f).await?;
                                }
                            }
                            Err(e) => {
                                // protocol violation: best-effort GoAway, then bail
                                let _ = self
                                    .transport
                                    .send(conn_frame(frame::Kind::GoAway(GoAway {
                                        last_stream_id: actor.highest_remote_open,
                                        code: GoAwayCode::ProtocolError as u32,
                                        message: e.to_string(),
                                    })))
                                    .await;
                                return Err(e.into());
                            }
                        }
                    }
                },
            }
        }
    }
}

fn conn_frame(kind: frame::Kind) -> Frame {
    Frame { stream_id: 0, seq: 0, kind: Some(kind) }
}

fn stream_frame(stream_id: u64, kind: frame::Kind) -> Frame {
    Frame { stream_id, seq: 0, kind: Some(kind) }
}

struct StreamSlot {
    state: StreamState,
    events: mpsc::UnboundedSender<StreamEvent>,
    /// send commands waiting for window: (payload, done)
    blocked: VecDeque<(Bytes, oneshot::Sender<Result<(), StreamError>>)>,
}

/// Pure connection logic: commands/frames in, frames out.
/// An error = protocol violation → the driver bails.
struct Actor {
    side: Side,
    ids: StreamIdAllocator,
    cmd_tx: mpsc::WeakUnboundedSender<Command>,
    /// None after drop (peer GoAway + all streams closed) — accept() → None.
    accept_tx: Option<mpsc::UnboundedSender<Incoming>>,
    going_away_local: bool,
    going_away_remote: bool,
    /// Highest id of a stream opened by the peer (for GoAway.last_stream_id).
    highest_remote_open: u64,
    /// BTreeMap — deterministic iteration order when distributing window.
    streams: BTreeMap<u64, StreamSlot>,
    /// Streams recently closed by us: frames in flight are a legal race.
    reset_ids: HashSet<u64>,
    reset_order: VecDeque<u64>,
    /// Our connection send window (from the peer's Hello).
    conn_send_window: Window,
    /// Accumulated connection recv-window credit pending send.
    conn_recv_credit: u32,
    /// Our announced recv windows.
    local_stream_window: u32,
    local_conn_window: u32,
    /// Send window for new streams (peer's recv window from its Hello).
    peer_stream_window: u32,
    next_ping: u64,
    pending_pings: HashMap<u64, oneshot::Sender<()>>,
}

impl Actor {
    fn new(
        side: Side,
        config: &Config,
        peer_hello: Hello,
        cmd_tx: mpsc::WeakUnboundedSender<Command>,
        accept_tx: mpsc::UnboundedSender<Incoming>,
    ) -> Self {
        Self {
            side,
            ids: StreamIdAllocator::new(side),
            cmd_tx,
            accept_tx: Some(accept_tx),
            going_away_local: false,
            going_away_remote: false,
            highest_remote_open: 0,
            streams: BTreeMap::new(),
            reset_ids: HashSet::new(),
            reset_order: VecDeque::new(),
            conn_send_window: Window::new(peer_hello.initial_connection_window),
            conn_recv_credit: 0,
            local_stream_window: config.initial_stream_window,
            local_conn_window: config.initial_connection_window,
            peer_stream_window: peer_hello.initial_stream_window,
            next_ping: 0,
            pending_pings: HashMap::new(),
        }
    }

    fn remove_slot(&mut self, stream_id: u64) {
        self.streams.remove(&stream_id);
        if self.reset_ids.insert(stream_id) {
            self.reset_order.push_back(stream_id);
            if self.reset_order.len() > RESET_IDS_CAP
                && let Some(old) = self.reset_order.pop_front()
            {
                self.reset_ids.remove(&old);
            }
        }
    }

    fn on_command(&mut self, cmd: Command) -> Vec<Frame> {
        match cmd {
            Command::Ping { done } => {
                let opaque = self.next_ping;
                self.next_ping += 1;
                self.pending_pings.insert(opaque, done);
                vec![conn_frame(frame::Kind::Ping(Ping { opaque }))]
            }
            Command::Open { method, metadata, reply } => {
                if self.going_away_local || self.going_away_remote {
                    let _ = reply.send(Err(OpenError::GoingAway));
                    return vec![];
                }
                let Some(cmd_tx) = self.cmd_tx.upgrade() else {
                    let _ = reply.send(Err(OpenError::Connection("closed".into())));
                    return vec![];
                };
                let stream_id = self.ids.next_id();
                let (events_tx, events_rx) = mpsc::unbounded_channel();
                self.streams.insert(
                    stream_id,
                    StreamSlot {
                        state: StreamState::new(
                            true,
                            self.peer_stream_window,
                            self.local_stream_window,
                        ),
                        events: events_tx,
                        blocked: VecDeque::new(),
                    },
                );
                let _ = reply.send(Ok((
                    SendHalf { stream_id, is_opener: true, cmd_tx: cmd_tx.clone() },
                    RecvHalf { stream_id, events: events_rx, cmd_tx, saw_terminal: false, cancel_on_drop: true },
                )));
                vec![stream_frame(
                    stream_id,
                    frame::Kind::Open(Open { method, metadata: Some(metadata) }),
                )]
            }
            Command::Send { stream_id, payload, done } => {
                let Some(slot) = self.streams.get_mut(&stream_id) else {
                    let _ = done.send(Err(StreamError::Cancelled));
                    return vec![];
                };
                if slot.state.is_terminated() {
                    let _ = done.send(Err(StreamError::Cancelled));
                    return vec![];
                }
                if !slot.state.can_send() && slot.state.send_window.can_send() {
                    // locally half-closed (window available, sending forbidden) — usage error
                    let _ = done.send(Err(StreamError::Connection("send after half-close".into())));
                    return vec![];
                }
                if slot.blocked.is_empty()
                    && slot.state.can_send()
                    && self.conn_send_window.can_send()
                {
                    slot.state.consume_send(payload.len());
                    self.conn_send_window.consume(payload.len());
                    let _ = done.send(Ok(()));
                    vec![stream_frame(
                        stream_id,
                        frame::Kind::Message(Message { payload, compressed: false }),
                    )]
                } else {
                    slot.blocked.push_back((payload, done));
                    vec![]
                }
            }
            Command::SendHeaders { stream_id, metadata } => {
                if self
                    .streams
                    .get(&stream_id)
                    .is_some_and(|s| !s.state.is_terminated())
                {
                    vec![stream_frame(
                        stream_id,
                        frame::Kind::Headers(Headers { metadata: Some(metadata) }),
                    )]
                } else {
                    vec![]
                }
            }
            Command::HalfClose { stream_id, done } => {
                let Some(slot) = self.streams.get_mut(&stream_id) else {
                    // stream already ended with a terminal (Status/Cancel) — no-op
                    let _ = done.send(if self.reset_ids.contains(&stream_id) {
                        Ok(())
                    } else {
                        Err(StreamError::Cancelled)
                    });
                    return vec![];
                };
                slot.state.local_half_close();
                let closed = slot.state.is_closed();
                let _ = done.send(Ok(()));
                if closed {
                    self.remove_slot(stream_id);
                }
                vec![stream_frame(stream_id, frame::Kind::HalfClose(HalfClose {}))]
            }
            Command::Finish { stream_id, status, done } => {
                let Some(slot) = self.streams.get_mut(&stream_id) else {
                    let _ = done.send(Err(StreamError::Cancelled));
                    return vec![];
                };
                slot.state.local_terminate();
                let _ = done.send(Ok(()));
                self.remove_slot(stream_id);
                vec![stream_frame(stream_id, frame::Kind::Status(status))]
            }
            Command::Cancel { stream_id } => {
                let Some(slot) = self.streams.get_mut(&stream_id) else {
                    return vec![];
                };
                if slot.state.is_terminated() {
                    return vec![];
                }
                slot.state.local_terminate();
                let _ = slot.events.send(StreamEvent::Cancelled);
                self.remove_slot(stream_id);
                vec![stream_frame(stream_id, frame::Kind::Cancel(crate::proto::v1::Cancel {}))]
            }
            Command::Credit { stream_id, bytes } => {
                let mut out = Vec::new();
                if let Some(slot) = self.streams.get_mut(&stream_id)
                    && slot.state.credit_recv(bytes).is_ok()
                {
                    out.push(stream_frame(
                        stream_id,
                        frame::Kind::WindowUpdate(WindowUpdate { increment: bytes }),
                    ));
                }
                self.conn_recv_credit = self.conn_recv_credit.saturating_add(bytes);
                if self.conn_recv_credit >= self.local_conn_window / 2 {
                    out.push(conn_frame(frame::Kind::WindowUpdate(WindowUpdate {
                        increment: self.conn_recv_credit,
                    })));
                    self.conn_recv_credit = 0;
                }
                out
            }
            Command::GoAway { code } => {
                self.going_away_local = true;
                vec![conn_frame(frame::Kind::GoAway(GoAway {
                    last_stream_id: self.highest_remote_open,
                    code: code as u32,
                    message: String::new(),
                }))]
            }
        }
    }

    fn on_frame(&mut self, f: Frame) -> Result<Vec<Frame>, ProtocolError> {
        let stream_id = f.stream_id;
        let kind = f.kind.ok_or(ProtocolError::EmptyFrame)?;
        if stream_id == 0 {
            return self.on_conn_frame(kind);
        }
        match kind {
            frame::Kind::Open(open) => self.on_open(stream_id, open),
            frame::Kind::Headers(h) => {
                self.stream_event(stream_id, |slot| {
                    slot.state.on_headers(h.metadata.unwrap_or_default())
                })?;
                Ok(vec![])
            }
            frame::Kind::Message(msg) => {
                let len = msg.payload.len() as u32;
                let delivered = self.stream_event(stream_id, |slot| {
                    slot.state.on_message(stream_id, msg)
                })?;
                if !delivered {
                    // message discarded (terminal/race) — refund the connection credit
                    return Ok(self.refund_conn_credit(len));
                }
                Ok(vec![])
            }
            frame::Kind::HalfClose(_) => {
                let out = self.stream_event(stream_id, |slot| slot.state.on_half_close())?;
                let _ = out;
                if self.streams.get(&stream_id).is_some_and(|s| s.state.is_closed()) {
                    self.remove_slot(stream_id);
                }
                Ok(vec![])
            }
            frame::Kind::Status(status) => {
                if let Some(slot) = self.streams.get_mut(&stream_id) {
                    let ev = slot.state.on_status(status);
                    let _ = slot.events.send(ev);
                    Self::fail_blocked(slot);
                    self.remove_slot(stream_id);
                    Ok(vec![])
                } else {
                    self.unknown(stream_id)
                }
            }
            frame::Kind::Cancel(_) => {
                if let Some(slot) = self.streams.get_mut(&stream_id) {
                    let ev = slot.state.on_cancel();
                    let _ = slot.events.send(ev);
                    Self::fail_blocked(slot);
                    self.remove_slot(stream_id);
                    Ok(vec![])
                } else {
                    self.unknown(stream_id)
                }
            }
            frame::Kind::WindowUpdate(wu) => {
                if let Some(slot) = self.streams.get_mut(&stream_id) {
                    slot.state.on_window_update(wu.increment)?;
                    Ok(self.drain_blocked(Some(stream_id)))
                } else if self.reset_ids.contains(&stream_id) {
                    Ok(vec![])
                } else {
                    self.unknown(stream_id)
                }
            }
            frame::Kind::Ping(_)
            | frame::Kind::Pong(_)
            | frame::Kind::Hello(_)
            | frame::Kind::GoAway(_)
            | frame::Kind::Ack(_) => Err(ProtocolError::ConnectionFrameOnStream(stream_id)),
        }
    }

    fn on_conn_frame(&mut self, kind: frame::Kind) -> Result<Vec<Frame>, ProtocolError> {
        match kind {
            frame::Kind::Ping(p) => {
                Ok(vec![conn_frame(frame::Kind::Pong(Pong { opaque: p.opaque }))])
            }
            frame::Kind::Pong(p) => {
                if let Some(done) = self.pending_pings.remove(&p.opaque) {
                    let _ = done.send(());
                }
                Ok(vec![])
            }
            frame::Kind::WindowUpdate(wu) => {
                self.conn_send_window.credit(wu.increment)?;
                Ok(self.drain_blocked(None))
            }
            frame::Kind::Hello(_) => Err(ProtocolError::ExpectedHello), // a second Hello is forbidden
            frame::Kind::GoAway(ga) => {
                self.going_away_remote = true;
                // streams the peer will no longer accept — fail their pending sends
                let last = ga.last_stream_id;
                for (&id, slot) in self.streams.iter_mut() {
                    if self.side.opens(id) && id > last {
                        Self::fail_blocked(slot);
                    }
                }
                if self.streams.is_empty() {
                    self.accept_tx = None;
                }
                Ok(vec![])
            }
            frame::Kind::Ack(_) => Ok(vec![]), // session layer; phase 1 ignores it
            _ => Err(ProtocolError::StreamFrameOnConnection),
        }
    }

    fn on_open(&mut self, stream_id: u64, open: Open) -> Result<Vec<Frame>, ProtocolError> {
        if !self.side.peer().opens(stream_id) {
            return Err(ProtocolError::InvalidParity(stream_id));
        }
        if self.streams.contains_key(&stream_id) || self.reset_ids.contains(&stream_id) {
            return Err(ProtocolError::DuplicateOpen(stream_id));
        }
        if self.going_away_local {
            // we're going away — reject the peer's new streams (it hasn't seen GoAway yet)
            return Ok(vec![stream_frame(stream_id, frame::Kind::Cancel(crate::proto::v1::Cancel {}))]);
        }
        self.highest_remote_open = self.highest_remote_open.max(stream_id);
        let Some(cmd_tx) = self.cmd_tx.upgrade() else {
            return Ok(vec![]); // all handles dropped — connection is dying
        };
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        self.streams.insert(
            stream_id,
            StreamSlot {
                state: StreamState::new(false, self.peer_stream_window, self.local_stream_window),
                events: events_tx,
                blocked: VecDeque::new(),
            },
        );
        if let Some(accept_tx) = &self.accept_tx {
            let _ = accept_tx.send(Incoming {
                method: open.method,
                metadata: open.metadata.unwrap_or_default(),
                send: SendHalf { stream_id, is_opener: false, cmd_tx: cmd_tx.clone() },
                recv: RecvHalf { stream_id, events: events_rx, cmd_tx, saw_terminal: false, cancel_on_drop: false },
            });
        }
        Ok(vec![])
    }

    /// Apply an event to the slot; `Ok(true)` = delivered to the application.
    fn stream_event(
        &mut self,
        stream_id: u64,
        apply: impl FnOnce(&mut StreamSlot) -> Result<StreamEvent, ProtocolError>,
    ) -> Result<bool, ProtocolError> {
        let Some(slot) = self.streams.get_mut(&stream_id) else {
            return if self.reset_ids.contains(&stream_id) {
                Ok(false) // legal race: frame in flight after our terminal
            } else {
                Err(ProtocolError::UnknownStream(stream_id))
            };
        };
        if slot.state.is_terminated() {
            return Ok(false);
        }
        let ev = apply(slot).map_err(|e| match e {
            // the sans-io layer doesn't know the id — fill it in
            ProtocolError::UnexpectedHeaders(_) => ProtocolError::UnexpectedHeaders(stream_id),
            other => other,
        })?;
        let _ = slot.events.send(ev);
        Ok(true)
    }

    fn refund_conn_credit(&mut self, bytes: u32) -> Vec<Frame> {
        self.conn_recv_credit = self.conn_recv_credit.saturating_add(bytes);
        if self.conn_recv_credit >= self.local_conn_window / 2 {
            let inc = self.conn_recv_credit;
            self.conn_recv_credit = 0;
            vec![conn_frame(frame::Kind::WindowUpdate(WindowUpdate { increment: inc }))]
        } else {
            vec![]
        }
    }

    fn fail_blocked(slot: &mut StreamSlot) {
        for (_, done) in slot.blocked.drain(..) {
            let _ = done.send(Err(StreamError::Cancelled));
        }
    }

    /// Hand out window to blocked sends. `only` — a specific stream
    /// (its window was replenished) or all (the connection window was).
    fn drain_blocked(&mut self, only: Option<u64>) -> Vec<Frame> {
        let mut out = Vec::new();
        let ids: Vec<u64> = match only {
            Some(id) => vec![id],
            None => self.streams.keys().copied().collect(),
        };
        for id in ids {
            let Some(slot) = self.streams.get_mut(&id) else { continue };
            while let Some((payload, _)) = slot.blocked.front() {
                if slot.state.can_send() && self.conn_send_window.can_send() {
                    let len = payload.len();
                    let (payload, done) = slot.blocked.pop_front().unwrap();
                    slot.state.consume_send(len);
                    self.conn_send_window.consume(len);
                    let _ = done.send(Ok(()));
                    out.push(stream_frame(
                        id,
                        frame::Kind::Message(Message { payload, compressed: false }),
                    ));
                } else {
                    break;
                }
            }
        }
        out
    }

    fn unknown(&self, stream_id: u64) -> Result<Vec<Frame>, ProtocolError> {
        if self.reset_ids.contains(&stream_id) {
            Ok(vec![])
        } else {
            Err(ProtocolError::UnknownStream(stream_id))
        }
    }
}
