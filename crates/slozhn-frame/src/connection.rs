use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::{Sink, SinkExt, Stream, StreamExt};
use tokio::sync::{Mutex, mpsc, oneshot};

use crate::error::{ConnError, GoAwayCode, OpenError, ProtocolError, StreamError, TransportClosed};
use crate::flow::Window;
use crate::ids::{Side, StreamIdAllocator};
use crate::proto::v1::{
    Frame, GoAway, HalfClose, Headers, Hello, Message, Metadata, Open, Ping, Pong, Status,
    WindowUpdate, frame, metadata_entry,
};
use crate::stream::{StreamEvent, StreamState};
use crate::{DEFAULT_WINDOW, MAX_MESSAGE_SIZE, PROTOCOL_VERSION};

/// How many recently closed stream ids we remember to tell a legal race
/// (frames in flight after our terminal) apart from a protocol error.
/// Beyond this many, a still-racing id may be evicted — the `stream_id
/// <= highest_remote_open` fallback in `stream_event` catches that case
/// (a frame on an already-seen id is a race, not a fatal UnknownStream).
const RESET_IDS_CAP: usize = 8192;
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
pub struct Config {
    /// Our per-stream recv window (announced in Hello).
    pub initial_stream_window: u32,
    /// Our connection recv window (announced in Hello).
    pub initial_connection_window: u32,
    /// Maximum number of concurrently open streams per connection, counting
    /// both directions (locally opened + peer-opened). Protects against a
    /// peer (or a runaway local caller) exhausting memory by opening an
    /// unbounded number of streams. Inbound `Open` frames beyond the limit
    /// are rejected with a stream-level `Status` (code 8,
    /// RESOURCE_EXHAUSTED); local `Command::Open` beyond the limit fails
    /// with `OpenError::LimitExceeded`.
    pub max_streams: usize,
    /// Deadline for receiving the peer's HELLO after we've sent ours (only
    /// applies to `bind`, not `bind_pre_negotiated` — the handshake is
    /// external there). Guards against a peer that upgrades the transport
    /// and then goes silent (slowloris), which would otherwise pin the
    /// driver task in `transport.next().await` forever.
    pub handshake_timeout: Duration,
    /// Maximum total metadata bytes (sum of key+value lengths across all
    /// entries) accepted on a single `Open` or `Headers` frame. Guards
    /// against a peer opening up to `max_streams` streams each carrying
    /// huge metadata. Exceeding it is a stream-level rejection (Status code
    /// 8, RESOURCE_EXHAUSTED), not connection-fatal — mirrors real gRPC's
    /// ~8-16 KiB header-list default.
    pub max_metadata_bytes: usize,
    /// Maximum number of metadata entries accepted on a single `Open` or
    /// `Headers` frame. Same rejection semantics as `max_metadata_bytes`.
    pub max_metadata_entries: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            initial_stream_window: DEFAULT_WINDOW,
            initial_connection_window: DEFAULT_WINDOW,
            max_streams: 1024,
            handshake_timeout: Duration::from_secs(10),
            max_metadata_bytes: 16 * 1024,
            max_metadata_entries: 128,
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
    Close,
}

/// Sending half of a stream.
///
/// On drop without having reached a local terminal (`finish`/`cancel`, or
/// `half_close` on the opener side), the slot would otherwise leak forever
/// in `Actor::streams` (consuming a `max_streams` slot) — see `Drop` impl
/// below, mirroring `RecvHalf::cancel_on_drop`.
pub struct SendHalf {
    stream_id: u64,
    is_opener: bool,
    cmd_tx: mpsc::UnboundedSender<Command>,
    /// Set once a terminal command (`Finish`/`Cancel`) or, for the opener,
    /// `HalfClose` has already been sent — makes `Drop` a no-op so we never
    /// double-send a terminal frame.
    terminal_sent: bool,
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
        self.rt(move |done| Command::Send {
            stream_id,
            payload,
            done,
        })
        .await
    }

    /// Initial response metadata. Accepting side only.
    pub fn send_headers(&self, metadata: Metadata) -> Result<(), StreamError> {
        if self.is_opener {
            return Err(StreamError::Connection("opener cannot send HEADERS".into()));
        }
        self.cmd_tx
            .send(Command::SendHeaders {
                stream_id: self.stream_id,
                metadata,
            })
            .map_err(|_| StreamError::Connection("closed".into()))
    }

    /// "I'm done sending" (opener side).
    pub async fn half_close(mut self) -> Result<(), StreamError> {
        self.terminal_sent = true;
        let stream_id = self.stream_id;
        self.rt(move |done| Command::HalfClose { stream_id, done })
            .await
    }

    /// Terminal Status (accepting side).
    pub async fn finish(mut self, status: Status) -> Result<(), StreamError> {
        self.terminal_sent = true;
        let stream_id = self.stream_id;
        self.rt(move |done| Command::Finish {
            stream_id,
            status,
            done,
        })
        .await
    }

    pub fn cancel(mut self) {
        self.terminal_sent = true;
        let _ = self.cmd_tx.send(Command::Cancel {
            stream_id: self.stream_id,
        });
    }
}

impl Drop for SendHalf {
    fn drop(&mut self) {
        if self.terminal_sent {
            return;
        }
        if self.is_opener {
            // opener dropped without half_close/cancel — abandon the RPC so
            // the peer learns and the actor reclaims the slot.
            let _ = self.cmd_tx.send(Command::Cancel {
                stream_id: self.stream_id,
            });
        } else {
            // acceptor dropped without finish/cancel — synthesize a
            // terminal Status so the peer sees a definite end and our own
            // slot is reclaimed instead of leaking. Nothing waits on the
            // reply oneshot, so a fresh one is created and dropped.
            let (done, _wait) = oneshot::channel();
            let _ = self.cmd_tx.send(Command::Finish {
                stream_id: self.stream_id,
                status: Status {
                    code: 2, // UNKNOWN (gRPC canonical code) — sender dropped without a status
                    message: "SendHalf dropped without finish/cancel".into(),
                    trailers: Some(Metadata { entries: Vec::new() }),
                },
                done,
            });
        }
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
            let _ = self.cmd_tx.send(Command::Cancel {
                stream_id: self.stream_id,
            });
        }
    }
}

#[derive(Clone)]
pub struct Connection {
    id: u64,
    cmd_tx: mpsc::UnboundedSender<Command>,
    accept_rx: Arc<Mutex<mpsc::UnboundedReceiver<Incoming>>>,
}

#[derive(Clone)]
pub struct WeakConnection {
    id: u64,
    cmd_tx: mpsc::WeakUnboundedSender<Command>,
}

impl Connection {
    pub fn id(&self) -> u64 {
        self.id
    }

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
            .send(Command::Open {
                method,
                metadata,
                reply,
            })
            .map_err(|_| OpenError::Connection("closed".into()))?;
        wait.await
            .map_err(|_| OpenError::Connection("closed".into()))?
    }

    /// Accept an incoming stream; `None` — connection closed.
    pub async fn accept(&self) -> Option<Incoming> {
        self.accept_rx.lock().await.recv().await
    }

    /// The driver is dead (connection closed) — commands are no longer accepted.
    pub fn is_closed(&self) -> bool {
        self.cmd_tx.is_closed()
    }

    /// Weak handle that does not keep the connection driver alive.
    pub fn downgrade(&self) -> WeakConnection {
        WeakConnection {
            id: self.id,
            cmd_tx: self.cmd_tx.downgrade(),
        }
    }

    /// Graceful shutdown: no new streams open, active ones run to completion.
    pub fn go_away(&self, code: GoAwayCode) {
        let _ = self.cmd_tx.send(Command::GoAway { code });
    }

    /// Close the connection immediately. Active streams are cancelled by
    /// transport teardown; use [`Self::go_away`] for graceful drain.
    pub fn close(&self) {
        let _ = self.cmd_tx.send(Command::Close);
    }
}

impl WeakConnection {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub async fn ping(&self) -> Result<(), String> {
        let Some(cmd_tx) = self.cmd_tx.upgrade() else {
            return Err("connection closed".to_owned());
        };
        let (done, wait) = oneshot::channel();
        cmd_tx
            .send(Command::Ping { done })
            .map_err(|_| "connection closed".to_owned())?;
        wait.await.map_err(|_| "connection closed".to_owned())
    }

    /// The driver is dead or no strong connection handles remain.
    pub fn is_closed(&self) -> bool {
        self.cmd_tx.upgrade().is_none()
    }

    pub fn close(&self) {
        if let Some(cmd_tx) = self.cmd_tx.upgrade() {
            let _ = cmd_tx.send(Command::Close);
        }
    }
}

pub struct ConnectionDriver<T> {
    connection_id: u64,
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
    let connection_id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    // Unbounded, but naturally capped: on_open only ever inserts an Incoming
    // once per accepted stream, and the number of concurrently open streams
    // is already bounded by Config::max_streams — so an app that stalls
    // Connection::accept() can accumulate at most max_streams queued
    // Incoming values before new peer Opens start getting rejected.
    let (accept_tx, accept_rx) = mpsc::unbounded_channel();
    let weak = cmd_tx.downgrade();
    (
        Connection {
            id: connection_id,
            cmd_tx,
            accept_rx: Arc::new(Mutex::new(accept_rx)),
        },
        ConnectionDriver {
            connection_id,
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
        tracing::debug!(
            connection_id = self.connection_id,
            side = ?self.side,
            "slozhn connection driver starting"
        );
        // 1. Hello both ways (unless the session layer did the handshake)
        let peer_hello = match self.pre_negotiated.take() {
            Some(h) => h,
            None => {
                tracing::trace!(
                    connection_id = self.connection_id,
                    side = ?self.side,
                    "sending connection hello"
                );
                self.transport
                    .send(conn_frame(frame::Kind::Hello(Hello {
                        version: PROTOCOL_VERSION,
                        initial_stream_window: self.config.initial_stream_window,
                        initial_connection_window: self.config.initial_connection_window,
                        ..Default::default()
                    })))
                    .await?;

                // wasm-safe deadline on the pre-Hello read: a peer that
                // upgrades the transport and then goes silent must not pin
                // this task forever (slowloris). futures_timer::Delay is
                // Send and works on both native and wasm32, unlike
                // tokio::time.
                let first = match futures::future::select(
                    self.transport.next(),
                    futures_timer::Delay::new(self.config.handshake_timeout),
                )
                .await
                {
                    futures::future::Either::Left((frame, _)) => frame.ok_or(TransportClosed)?,
                    futures::future::Either::Right((_, _)) => {
                        tracing::warn!(
                            connection_id = self.connection_id,
                            side = ?self.side,
                            timeout = ?self.config.handshake_timeout,
                            "handshake timed out waiting for peer HELLO",
                        );
                        return Err(ConnError::HandshakeTimeout);
                    }
                };
                let h = match first.kind {
                    Some(frame::Kind::Hello(h)) => h,
                    _ => return Err(ProtocolError::ExpectedHello.into()),
                };
                tracing::debug!(
                    connection_id = self.connection_id,
                    side = ?self.side,
                    version = h.version,
                    initial_stream_window = h.initial_stream_window,
                    initial_connection_window = h.initial_connection_window,
                    "received connection hello",
                );
                if h.version != PROTOCOL_VERSION {
                    tracing::warn!(
                        connection_id = self.connection_id,
                        side = ?self.side,
                        peer_version = h.version,
                        expected_version = PROTOCOL_VERSION,
                        "connection protocol version mismatch",
                    );
                    return Err(ProtocolError::VersionMismatch(h.version).into());
                }
                h
            }
        };
        let mut actor = Actor::new(
            self.side,
            self.connection_id,
            &self.config,
            peer_hello,
            self.cmd_tx,
            self.accept_tx,
        );

        // 2. main loop
        loop {
            tokio::select! {
                cmd = self.cmd_rx.recv() => match cmd {
                    None => {
                        tracing::debug!(
                            connection_id = actor.connection_id,
                            side = ?actor.side,
                            "connection driver stopping: all handles dropped"
                        );
                        return Ok(());
                    }
                    Some(Command::Close) => {
                        tracing::warn!(
                            connection_id = actor.connection_id,
                            side = ?actor.side,
                            "connection driver closing by command"
                        );
                        return Ok(());
                    }
                    Some(cmd) => {
                        // feed all frames, then one flush — a batch of output
                        // (e.g. WindowUpdate + data) costs a single flush
                        // rather than N, shrinking the window during which the
                        // driver is blocked writing and not reading inbound
                        // Ack/WindowUpdate/Pong.
                        let out = actor.on_command(cmd);
                        for f in out {
                            self.transport.feed(f).await?;
                        }
                        self.transport.flush().await?;
                    }
                },
                frame = self.transport.next() => match frame {
                    None => {
                        tracing::debug!(
                            connection_id = actor.connection_id,
                            side = ?actor.side,
                            "connection transport closed"
                        );
                        return Err(TransportClosed.into());
                    }
                    Some(frame) => {
                        match actor.on_frame(frame) {
                            Ok(out) => {
                                for f in out {
                                    self.transport.feed(f).await?;
                                }
                                self.transport.flush().await?;
                            }
                            Err(e) => {
                                // protocol violation: best-effort GoAway, then bail
                                tracing::warn!(
                                    connection_id = actor.connection_id,
                                    side = ?actor.side,
                                    error = %e,
                                    highest_remote_open = actor.highest_remote_open,
                                    "connection protocol violation",
                                );
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
    Frame {
        stream_id: 0,
        seq: 0,
        kind: Some(kind),
    }
}

fn stream_frame(stream_id: u64, kind: frame::Kind) -> Frame {
    Frame {
        stream_id,
        seq: 0,
        kind: Some(kind),
    }
}

struct StreamSlot {
    state: StreamState,
    events: mpsc::UnboundedSender<StreamEvent>,
    /// send commands waiting for window: (payload, done)
    blocked: VecDeque<(Bytes, oneshot::Sender<Result<(), StreamError>>)>,
    /// Bytes consumed from this stream's recv window (on `on_message`) that
    /// have not yet been credited back to `conn_recv_credit` (via
    /// `Command::Credit`, fired only when the app actually reads the
    /// `StreamEvent::Message`). If the stream is torn down while this is
    /// still nonzero — the app cancelled/dropped early, or the peer
    /// terminated the stream with messages still queued unread in
    /// `events` — that remainder must be refunded to `conn_recv_credit` on
    /// teardown (see `remove_slot`), or the peer's connection send window
    /// permanently shrinks by the unread amount.
    pending_credit: u32,
}

/// Pure connection logic: commands/frames in, frames out.
/// An error = protocol violation → the driver bails.
struct Actor {
    side: Side,
    connection_id: u64,
    ids: StreamIdAllocator,
    cmd_tx: mpsc::WeakUnboundedSender<Command>,
    /// None after drop (peer GoAway + all streams closed) — accept() → None.
    accept_tx: Option<mpsc::UnboundedSender<Incoming>>,
    going_away_local: bool,
    going_away_remote: bool,
    /// Highest id of a stream opened by the peer (for GoAway.last_stream_id).
    highest_remote_open: u64,
    /// Highest id of a stream WE opened. Together with `highest_remote_open`
    /// this bounds "an id that was legitimately active at some point" — a
    /// late frame on such an id (evicted from the bounded reset set) is a
    /// race to drop, not a fatal UnknownStream.
    highest_local_open: u64,
    /// BTreeMap — deterministic iteration order when distributing window.
    streams: BTreeMap<u64, StreamSlot>,
    /// Maximum concurrently open streams (both directions) — DoS guard.
    max_streams: usize,
    /// Metadata caps (Open/Headers) — DoS guard, see `Config`.
    max_metadata_bytes: usize,
    max_metadata_entries: usize,
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
        connection_id: u64,
        config: &Config,
        peer_hello: Hello,
        cmd_tx: mpsc::WeakUnboundedSender<Command>,
        accept_tx: mpsc::UnboundedSender<Incoming>,
    ) -> Self {
        Self {
            side,
            connection_id,
            ids: StreamIdAllocator::new(side),
            cmd_tx,
            accept_tx: Some(accept_tx),
            going_away_local: false,
            going_away_remote: false,
            highest_remote_open: 0,
            highest_local_open: 0,
            streams: BTreeMap::new(),
            max_streams: config.max_streams,
            max_metadata_bytes: config.max_metadata_bytes,
            max_metadata_entries: config.max_metadata_entries,
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

    /// Remember `stream_id` as recently closed so in-flight follow-up frames
    /// (a legal race) are dropped silently instead of causing a protocol
    /// error.
    fn mark_reset(&mut self, stream_id: u64) {
        if self.reset_ids.insert(stream_id) {
            self.reset_order.push_back(stream_id);
            if self.reset_order.len() > RESET_IDS_CAP
                && let Some(old) = self.reset_order.pop_front()
            {
                self.reset_ids.remove(&old);
            }
        }
    }

    /// Tear down a stream slot. Any bytes consumed from its recv window but
    /// never credited back to `conn_recv_credit` (see `StreamSlot::pending_credit`
    /// — messages delivered into `events` but never read by the app before
    /// teardown) are refunded here, same as `refund_conn_credit` does for a
    /// message discarded outright. Without this, an early-cancelled or
    /// remotely-terminated stream with unread buffered messages would leak
    /// that credit forever and the peer's connection send window would
    /// permanently shrink by the unread amount.
    fn remove_slot(&mut self, stream_id: u64) -> Vec<Frame> {
        let pending = self
            .streams
            .remove(&stream_id)
            .map(|slot| slot.pending_credit)
            .unwrap_or(0);
        self.mark_reset(stream_id);
        self.close_accept_if_drained();
        if pending > 0 {
            self.refund_conn_credit(pending)
        } else {
            Vec::new()
        }
    }

    fn close_accept_if_drained(&mut self) {
        if (self.going_away_local || self.going_away_remote) && self.streams.is_empty() {
            self.accept_tx = None;
        }
    }

    fn on_command(&mut self, cmd: Command) -> Vec<Frame> {
        match cmd {
            Command::Ping { done } => {
                let opaque = self.next_ping;
                self.next_ping += 1;
                self.pending_pings.insert(opaque, done);
                tracing::trace!(
                    connection_id = self.connection_id,
                    side = ?self.side,
                    opaque,
                    "sending ping"
                );
                vec![conn_frame(frame::Kind::Ping(Ping { opaque }))]
            }
            Command::Open {
                method,
                metadata,
                reply,
            } => {
                if self.going_away_local || self.going_away_remote {
                    tracing::debug!(
                        connection_id = self.connection_id,
                        side = ?self.side,
                        method,
                        going_away_local = self.going_away_local,
                        going_away_remote = self.going_away_remote,
                        "rejecting open on going-away connection",
                    );
                    let _ = reply.send(Err(OpenError::GoingAway));
                    return vec![];
                }
                if self.streams.len() >= self.max_streams {
                    tracing::warn!(
                        connection_id = self.connection_id,
                        side = ?self.side,
                        method,
                        max_streams = self.max_streams,
                        "rejecting local open: stream limit exceeded",
                    );
                    let _ = reply.send(Err(OpenError::LimitExceeded));
                    return vec![];
                }
                let Some(cmd_tx) = self.cmd_tx.upgrade() else {
                    let _ = reply.send(Err(OpenError::Connection("closed".into())));
                    return vec![];
                };
                let stream_id = self.ids.next_id();
                self.highest_local_open = self.highest_local_open.max(stream_id);
                // Unbounded by design, not oversight: the per-stream flow
                // control window (stream.rs::on_message, enforced against
                // the peer as a FlowControlViolation) already caps how many
                // in-flight Message bytes can ever be queued here — a
                // stalled consumer stops the *sender* via the window, not
                // via this channel. Switching to a bounded channel would
                // need `try_send` + a drop/backpressure path inside the
                // synchronous `Actor::on_command`/`on_frame` (which return
                // `Vec<Frame>` and must not block), a bigger structural
                // change than this fix warrants. See connection.rs on_open
                // for the mirror-image acceptor-side channel.
                let (events_tx, events_rx) = mpsc::unbounded_channel();
                tracing::debug!(
                    connection_id = self.connection_id,
                    side = ?self.side,
                    stream_id,
                    method,
                    "opening stream"
                );
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
                        pending_credit: 0,
                    },
                );
                let _ = reply.send(Ok((
                    SendHalf {
                        stream_id,
                        is_opener: true,
                        cmd_tx: cmd_tx.clone(),
                        terminal_sent: false,
                    },
                    RecvHalf {
                        stream_id,
                        events: events_rx,
                        cmd_tx,
                        saw_terminal: false,
                        cancel_on_drop: true,
                    },
                )));
                vec![stream_frame(
                    stream_id,
                    frame::Kind::Open(Open {
                        method,
                        metadata: Some(metadata),
                    }),
                )]
            }
            Command::Send {
                stream_id,
                payload,
                done,
            } => {
                if payload.len() > MAX_MESSAGE_SIZE {
                    // the peer would treat an oversized frame as a
                    // connection-fatal protocol error (MessageTooLarge) —
                    // reject it locally instead of killing every stream.
                    let _ = done.send(Err(StreamError::Connection(format!(
                        "message size {} exceeds MAX_MESSAGE_SIZE {}",
                        payload.len(),
                        MAX_MESSAGE_SIZE
                    ))));
                    return vec![];
                }
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
                        frame::Kind::Message(Message {
                            payload,
                            compressed: false,
                        }),
                    )]
                } else {
                    slot.blocked.push_back((payload, done));
                    vec![]
                }
            }
            Command::SendHeaders {
                stream_id,
                metadata,
            } => {
                if self
                    .streams
                    .get(&stream_id)
                    .is_some_and(|s| !s.state.is_terminated())
                {
                    vec![stream_frame(
                        stream_id,
                        frame::Kind::Headers(Headers {
                            metadata: Some(metadata),
                        }),
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
                let mut out = vec![stream_frame(
                    stream_id,
                    frame::Kind::HalfClose(HalfClose {}),
                )];
                if closed {
                    out.extend(self.remove_slot(stream_id));
                }
                out
            }
            Command::Finish {
                stream_id,
                status,
                done,
            } => {
                let Some(slot) = self.streams.get_mut(&stream_id) else {
                    let _ = done.send(Err(StreamError::Cancelled));
                    return vec![];
                };
                slot.state.local_terminate();
                let _ = done.send(Ok(()));
                let mut out = vec![stream_frame(stream_id, frame::Kind::Status(status))];
                out.extend(self.remove_slot(stream_id));
                out
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
                let mut out = vec![stream_frame(
                    stream_id,
                    frame::Kind::Cancel(crate::proto::v1::Cancel {}),
                )];
                out.extend(self.remove_slot(stream_id));
                out
            }
            Command::Credit { stream_id, bytes } => {
                let mut out = Vec::new();
                if let Some(slot) = self.streams.get_mut(&stream_id) {
                    // the app read this many bytes' worth of Message — no
                    // longer "pending" a teardown refund.
                    slot.pending_credit = slot.pending_credit.saturating_sub(bytes);
                    if slot.state.credit_recv(bytes).is_ok() {
                        out.push(stream_frame(
                            stream_id,
                            frame::Kind::WindowUpdate(WindowUpdate { increment: bytes }),
                        ));
                    }
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
                tracing::info!(
                    connection_id = self.connection_id,
                    side = ?self.side,
                    code = ?code,
                    last_stream_id = self.highest_remote_open,
                    active_streams = self.streams.len(),
                    "sending goaway",
                );
                self.close_accept_if_drained();
                vec![conn_frame(frame::Kind::GoAway(GoAway {
                    last_stream_id: self.highest_remote_open,
                    code: code as u32,
                    message: String::new(),
                }))]
            }
            Command::Close => vec![],
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
                let metadata = h.metadata.unwrap_or_default();
                if Self::metadata_exceeds_limit(&metadata, self.max_metadata_bytes, self.max_metadata_entries) {
                    return Ok(self.reject_stream_metadata(stream_id));
                }
                self.stream_event(stream_id, |slot| slot.state.on_headers(metadata))?;
                Ok(vec![])
            }
            frame::Kind::Message(msg) => {
                let len = msg.payload.len() as u32;
                let delivered =
                    self.stream_event(stream_id, |slot| slot.state.on_message(stream_id, msg))?;
                if !delivered {
                    // message discarded (terminal/race) — refund the connection credit
                    return Ok(self.refund_conn_credit(len));
                }
                // delivered into the stream's `events` channel — counts
                // against conn_recv_credit until the app reads it
                // (Command::Credit) or the stream tears down (remove_slot).
                if let Some(slot) = self.streams.get_mut(&stream_id) {
                    slot.pending_credit = slot.pending_credit.saturating_add(len);
                }
                Ok(vec![])
            }
            frame::Kind::HalfClose(_) => {
                let out = self.stream_event(stream_id, |slot| slot.state.on_half_close())?;
                let _ = out;
                let mut frames = Vec::new();
                if self
                    .streams
                    .get(&stream_id)
                    .is_some_and(|s| s.state.is_closed())
                {
                    frames = self.remove_slot(stream_id);
                }
                Ok(frames)
            }
            frame::Kind::Status(status) => {
                if let Some(slot) = self.streams.get_mut(&stream_id) {
                    let ev = slot.state.on_status(status);
                    let _ = slot.events.send(ev);
                    Self::fail_blocked(slot);
                    Ok(self.remove_slot(stream_id))
                } else {
                    self.unknown(stream_id)
                }
            }
            frame::Kind::Cancel(_) => {
                if let Some(slot) = self.streams.get_mut(&stream_id) {
                    let ev = slot.state.on_cancel();
                    let _ = slot.events.send(ev);
                    Self::fail_blocked(slot);
                    Ok(self.remove_slot(stream_id))
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
                tracing::trace!(
                    connection_id = self.connection_id,
                    side = ?self.side,
                    opaque = p.opaque,
                    "received ping"
                );
                Ok(vec![conn_frame(frame::Kind::Pong(Pong {
                    opaque: p.opaque,
                }))])
            }
            frame::Kind::Pong(p) => {
                tracing::trace!(
                    connection_id = self.connection_id,
                    side = ?self.side,
                    opaque = p.opaque,
                    "received pong"
                );
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
                let code = GoAwayCode::from_u32(ga.code);
                tracing::info!(
                    connection_id = self.connection_id,
                    side = ?self.side,
                    code = ?code,
                    last_stream_id = ga.last_stream_id,
                    message = %ga.message,
                    active_streams = self.streams.len(),
                    "received goaway",
                );
                // streams the peer will no longer accept — fail their pending sends
                let last = ga.last_stream_id;
                for (&id, slot) in self.streams.iter_mut() {
                    if self.side.opens(id) && id > last {
                        Self::fail_blocked(slot);
                    }
                }
                self.close_accept_if_drained();
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
            tracing::debug!(
                connection_id = self.connection_id,
                side = ?self.side,
                stream_id,
                "rejecting peer open during local goaway"
            );
            return Ok(vec![stream_frame(
                stream_id,
                frame::Kind::Cancel(crate::proto::v1::Cancel {}),
            )]);
        }
        if self.streams.len() >= self.max_streams {
            tracing::warn!(
                connection_id = self.connection_id,
                side = ?self.side,
                stream_id,
                max_streams = self.max_streams,
                "rejecting peer open: stream limit exceeded",
            );
            self.mark_reset(stream_id);
            return Ok(vec![stream_frame(
                stream_id,
                frame::Kind::Status(Status {
                    code: 8, // RESOURCE_EXHAUSTED
                    message: "stream limit exceeded".into(),
                    trailers: Some(Metadata { entries: Vec::new() }),
                }),
            )]);
        }
        if open
            .metadata
            .as_ref()
            .is_some_and(|m| Self::metadata_exceeds_limit(m, self.max_metadata_bytes, self.max_metadata_entries))
        {
            tracing::warn!(
                connection_id = self.connection_id,
                side = ?self.side,
                stream_id,
                max_metadata_bytes = self.max_metadata_bytes,
                max_metadata_entries = self.max_metadata_entries,
                "rejecting peer open: metadata exceeds limit",
            );
            self.mark_reset(stream_id);
            return Ok(vec![stream_frame(
                stream_id,
                frame::Kind::Status(Status {
                    code: 8, // RESOURCE_EXHAUSTED
                    message: "metadata exceeds limit".into(),
                    trailers: Some(Metadata { entries: Vec::new() }),
                }),
            )]);
        }
        self.highest_remote_open = self.highest_remote_open.max(stream_id);
        let Some(cmd_tx) = self.cmd_tx.upgrade() else {
            return Ok(vec![]); // all handles dropped — connection is dying
        };
        // Unbounded by design here too — see the matching comment in
        // Command::Open above; flow control (stream.rs::on_message) is the
        // real bound on in-flight bytes queued through this channel.
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        tracing::debug!(
            connection_id = self.connection_id,
            side = ?self.side,
            stream_id,
            method = %open.method,
            "accepted peer stream",
        );
        self.streams.insert(
            stream_id,
            StreamSlot {
                state: StreamState::new(false, self.peer_stream_window, self.local_stream_window),
                events: events_tx,
                blocked: VecDeque::new(),
                pending_credit: 0,
            },
        );
        if let Some(accept_tx) = &self.accept_tx {
            let _ = accept_tx.send(Incoming {
                method: open.method,
                metadata: open.metadata.unwrap_or_default(),
                send: SendHalf {
                    stream_id,
                    is_opener: false,
                    cmd_tx: cmd_tx.clone(),
                    terminal_sent: false,
                },
                recv: RecvHalf {
                    stream_id,
                    events: events_rx,
                    cmd_tx,
                    saw_terminal: false,
                    cancel_on_drop: false,
                },
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
            // A frame for a stream we have no slot for is a legal race — a
            // frame in flight from the peer before it saw our terminal — when
            // the id is in our recent-reset set OR is one the peer already
            // opened at some point (`<= highest_remote_open`): the reset set is
            // bounded, so a very high stream-churn burst can evict a
            // still-racing id, but such an id is always `<= highest_remote_open`
            // and must not tear the whole connection down. A frame for an id
            // ABOVE the highest ever opened is genuinely unknown → protocol
            // error.
            return if self.reset_ids.contains(&stream_id)
                || stream_id <= self.highest_remote_open.max(self.highest_local_open)
            {
                Ok(false)
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
            vec![conn_frame(frame::Kind::WindowUpdate(WindowUpdate {
                increment: inc,
            }))]
        } else {
            vec![]
        }
    }

    fn fail_blocked(slot: &mut StreamSlot) {
        for (_, done) in slot.blocked.drain(..) {
            let _ = done.send(Err(StreamError::Cancelled));
        }
    }

    /// Total metadata size (sum of key+value bytes) or entry count exceeds
    /// the configured caps — DoS guard for `Open`/`Headers` (see `Config`).
    fn metadata_exceeds_limit(metadata: &Metadata, max_bytes: usize, max_entries: usize) -> bool {
        if metadata.entries.len() > max_entries {
            return true;
        }
        let total: usize = metadata
            .entries
            .iter()
            .map(|e| {
                let value_len = match &e.value {
                    Some(metadata_entry::Value::Ascii(s)) => s.len(),
                    Some(metadata_entry::Value::Bin(b)) => b.len(),
                    None => 0,
                };
                e.key.len() + value_len
            })
            .sum();
        total > max_bytes
    }

    /// Reject an existing stream's `Headers` for carrying oversized
    /// metadata: a stream-level Status(RESOURCE_EXHAUSTED=8), not
    /// connection-fatal, mirroring how `on_open` rejects an oversized
    /// `Open`. If the stream is unknown (already reset — a legal race),
    /// this is a silent no-op.
    fn reject_stream_metadata(&mut self, stream_id: u64) -> Vec<Frame> {
        let Some(slot) = self.streams.get_mut(&stream_id) else {
            return Vec::new();
        };
        tracing::warn!(
            connection_id = self.connection_id,
            side = ?self.side,
            stream_id,
            max_metadata_bytes = self.max_metadata_bytes,
            max_metadata_entries = self.max_metadata_entries,
            "rejecting HEADERS: metadata exceeds limit",
        );
        let status = Status {
            code: 8, // RESOURCE_EXHAUSTED
            message: "metadata exceeds limit".into(),
            trailers: Some(Metadata { entries: Vec::new() }),
        };
        let ev = slot.state.on_status(status.clone());
        let _ = slot.events.send(ev);
        Self::fail_blocked(slot);
        let mut out = self.remove_slot(stream_id);
        out.push(stream_frame(stream_id, frame::Kind::Status(status)));
        out
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
            let Some(slot) = self.streams.get_mut(&id) else {
                continue;
            };
            while let Some((payload, _)) = slot.blocked.front() {
                // defensive: Command::Send already rejects oversized
                // payloads before they ever reach `blocked`, but guard here
                // too so this invariant can never regress silently.
                if payload.len() > MAX_MESSAGE_SIZE {
                    let (payload, done) = slot.blocked.pop_front().unwrap();
                    let _ = done.send(Err(StreamError::Connection(format!(
                        "message size {} exceeds MAX_MESSAGE_SIZE {}",
                        payload.len(),
                        MAX_MESSAGE_SIZE
                    ))));
                    continue;
                }
                if slot.state.can_send() && self.conn_send_window.can_send() {
                    let len = payload.len();
                    let (payload, done) = slot.blocked.pop_front().unwrap();
                    slot.state.consume_send(len);
                    self.conn_send_window.consume(len);
                    let _ = done.send(Ok(()));
                    out.push(stream_frame(
                        id,
                        frame::Kind::Message(Message {
                            payload,
                            compressed: false,
                        }),
                    ));
                } else {
                    break;
                }
            }
        }
        out
    }

    fn unknown(&self, stream_id: u64) -> Result<Vec<Frame>, ProtocolError> {
        // Same race tolerance as `stream_event`: a recently-reset id, or any
        // id at/below the highest the peer ever opened, is a legal in-flight
        // race (bounded reset set may have evicted it) — not a fatal error.
        if self.reset_ids.contains(&stream_id)
            || stream_id <= self.highest_remote_open.max(self.highest_local_open)
        {
            Ok(vec![])
        } else {
            Err(ProtocolError::UnknownStream(stream_id))
        }
    }
}
