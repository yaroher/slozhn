//! Server side of the session layer: session manager + a transport living
//! on top of changing physical connections. Native-only (in-memory state,
//! tokio TTL). Behind a load balancer, sticky sessions are required (spec §8).

use std::collections::{HashMap, VecDeque};
use std::fmt::Write as _;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures::{Future, Sink, SinkExt, Stream, StreamExt};
use slozhn_frame::TransportClosed;
use slozhn_frame::proto::v1::{Frame, Hello, frame};
use tokio::sync::mpsc;

use crate::client::{BoxFrameTransport, FrameDuplex};
use crate::core::{Ingress, SessionCore, sessioned};
use crate::{SessionConfig, SessionError};

#[derive(Clone)]
pub struct ServerSessionConfig {
    pub session: SessionConfig,
    pub frame: slozhn_frame::connection::Config,
    /// How long to wait for the client to return after a disconnect.
    pub ttl: Duration,
}

impl Default for ServerSessionConfig {
    fn default() -> Self {
        Self {
            session: SessionConfig::default(),
            frame: slozhn_frame::connection::Config::default(),
            ttl: Duration::from_secs(60),
        }
    }
}

struct SessionEntry {
    token: Bytes,
    attach_tx: mpsc::UnboundedSender<(BoxFrameTransport, u64)>,
}

type Registry = Arc<Mutex<HashMap<Bytes, SessionEntry>>>;

fn session_label(session_id: &Bytes) -> String {
    let mut out = String::with_capacity(16);
    for b in session_id.iter().take(8) {
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}

pub struct SessionManager {
    sessions: Registry,
    config: ServerSessionConfig,
}

impl SessionManager {
    pub fn new(config: ServerSessionConfig) -> Arc<Self> {
        Arc::new(Self {
            sessions: Arc::new(Mutex::new(HashMap::new())),
            config,
        })
    }

    /// Handle a new physical connection.
    /// `Some` — new session: the caller does `bind_pre_negotiated` + serve.
    /// `None` — attach to an existing one or a rejected resume: nothing to do.
    pub async fn accept(
        &self,
        mut transport: BoxFrameTransport,
    ) -> Result<Option<(ServerSessionTransport, Hello)>, SessionError> {
        let first = transport.next().await.ok_or_else(|| {
            tracing::warn!("server session transport closed before hello");
            SessionError::Handshake("closed before hello".into())
        })?;
        let hello = match first.kind {
            Some(frame::Kind::Hello(h)) => h,
            _ => {
                tracing::warn!("server session accept received non-hello frame");
                return Err(SessionError::Handshake("expected hello".into()));
            }
        };
        if hello.version != slozhn_frame::PROTOCOL_VERSION {
            tracing::warn!(
                peer_version = hello.version,
                expected_version = slozhn_frame::PROTOCOL_VERSION,
                "server session protocol version mismatch",
            );
            return Err(SessionError::Handshake(format!(
                "unsupported version {}",
                hello.version
            )));
        }

        if hello.session_id.is_empty() {
            // new session
            let session_id = Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes());
            let session_log_id = session_label(&session_id);
            let token = Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes());
            let reply = server_hello(&self.config.frame, &session_id, &token, 0, false);
            transport.send(reply).await.map_err(|_| {
                tracing::warn!("server session transport closed during hello reply");
                SessionError::Handshake("closed during hello reply".into())
            })?;

            let (attach_tx, attach_rx) = mpsc::unbounded_channel();
            self.sessions.lock().expect("registry lock").insert(
                session_id.clone(),
                SessionEntry {
                    token: token.clone(),
                    attach_tx,
                },
            );
            tracing::info!(
                session_id = %session_log_id,
                ttl_ms = self.config.ttl.as_millis(),
                "server session created",
            );

            let st = ServerSessionTransport {
                phase: SPhase::Active(transport),
                core: SessionCore::new(
                    self.config.session.replay_buffer_bytes,
                    self.config.session.ack_every,
                ),
                pending_out: VecDeque::new(),
                ack_timer: None,
                ack_delay: self.config.session.ack_delay,
                idle_timeout: self.config.session.keepalive_interval.map(|i| {
                    // клиент пингует каждые interval; даём 2 интервала + его
                    // pong-таймаут, прежде чем счесть транспорт мёртвым
                    i * 2 + self.config.session.keepalive_timeout
                }),
                idle_timer: None,
                attach_rx,
                attach_closed: false,
                frame_config: self.config.frame.clone(),
                session_id,
                session_log_id,
                token,
                ttl: self.config.ttl,
                registry: self.sessions.clone(),
            };
            return Ok(Some((st, hello)));
        }

        // resume
        let session_id = hello.session_id.clone();
        let session_log_id = session_label(&session_id);
        let mut transport = Some(transport);
        {
            let sessions = self.sessions.lock().expect("registry lock");
            if let Some(entry) = sessions.get(&session_id)
                && entry.token.as_ref() == hello.resume_token.as_ref()
            {
                let t = transport.take().expect("present");
                match entry.attach_tx.send((t, hello.last_recv_seq)) {
                    // the ServerSessionTransport itself will send the Hello reply + replay
                    Ok(()) => {
                        tracing::info!(
                            session_id = %session_log_id,
                            client_last_recv_seq = hello.last_recv_seq,
                            "server session resume handed off",
                        );
                        return Ok(None);
                    }
                    // session died between lookup and send — take the transport back
                    Err(mpsc::error::SendError((t, _))) => {
                        tracing::warn!(
                            session_id = %session_log_id,
                            "server session resume target died during handoff",
                        );
                        transport = Some(t);
                    }
                }
            }
        }
        // no session / wrong token / session died — honest rejection
        tracing::warn!(
            session_id = %session_log_id,
            "server session resume rejected",
        );
        if let Some(mut t) = transport {
            let _ = t
                .send(server_hello(
                    &self.config.frame,
                    &Bytes::new(),
                    &Bytes::new(),
                    0,
                    true,
                ))
                .await;
        }
        Ok(None)
    }
}

/// Server's Hello reply.
fn server_hello(
    cfg: &slozhn_frame::connection::Config,
    session_id: &Bytes,
    token: &Bytes,
    last_recv_seq: u64,
    resume_rejected: bool,
) -> Frame {
    Frame {
        stream_id: 0,
        seq: 0,
        kind: Some(frame::Kind::Hello(Hello {
            version: slozhn_frame::PROTOCOL_VERSION,
            initial_stream_window: cfg.initial_stream_window,
            initial_connection_window: cfg.initial_connection_window,
            session_id: session_id.clone(),
            resume_token: token.clone(),
            last_recv_seq,
            resume_rejected,
        })),
    }
}

enum SPhase {
    Active(BoxFrameTransport),
    Detached(Pin<Box<tokio::time::Sleep>>),
    Dead,
}

pub struct ServerSessionTransport {
    phase: SPhase,
    core: SessionCore,
    pending_out: VecDeque<Frame>,
    ack_timer: Option<futures_timer::Delay>,
    ack_delay: Duration,
    /// Idle detector: no inbound frames for this long while Active → detach
    /// (the client pings every keepalive_interval, so silence means a break).
    idle_timeout: Option<Duration>,
    idle_timer: Option<futures_timer::Delay>,
    attach_rx: mpsc::UnboundedReceiver<(BoxFrameTransport, u64)>,
    attach_closed: bool,
    frame_config: slozhn_frame::connection::Config,
    session_id: Bytes,
    session_log_id: String,
    token: Bytes,
    ttl: Duration,
    registry: Registry,
}

impl ServerSessionTransport {
    fn try_flush(
        t: &mut BoxFrameTransport,
        pending: &mut VecDeque<Frame>,
        cx: &mut Context<'_>,
    ) -> bool {
        while !pending.is_empty() {
            match t.poll_ready_unpin(cx) {
                Poll::Ready(Ok(())) => {
                    let f = pending.pop_front().expect("non-empty");
                    if t.start_send_unpin(f).is_err() {
                        return false;
                    }
                }
                Poll::Ready(Err(_)) => return false,
                Poll::Pending => break,
            }
        }
        !matches!(t.poll_flush_unpin(cx), Poll::Ready(Err(_)))
    }

    fn detach(&mut self) {
        tracing::debug!(
            session_id = %self.session_log_id,
            pending_out = self.pending_out.len(),
            ttl_ms = self.ttl.as_millis(),
            "server session detached from physical transport",
        );
        self.pending_out.clear();
        self.ack_timer = None;
        self.phase = SPhase::Detached(Box::pin(tokio::time::sleep(self.ttl)));
    }

    fn die(&mut self) {
        tracing::info!(
            session_id = %self.session_log_id,
            "server session expired or closed",
        );
        self.registry
            .lock()
            .expect("registry lock")
            .remove(&self.session_id);
        self.phase = SPhase::Dead;
    }

    /// A new physical transport arrived: reply with Hello + replay.
    fn attach(&mut self, t: BoxFrameTransport, client_last_recv: u64) {
        self.pending_out.clear();
        self.pending_out.push_back(server_hello(
            &self.frame_config,
            &self.session_id,
            &self.token,
            self.core.last_recv_seq(),
            false,
        ));
        let replay = self.core.replay_after(client_last_recv);
        tracing::info!(
            session_id = %self.session_log_id,
            client_last_recv_seq = client_last_recv,
            server_last_recv_seq = self.core.last_recv_seq(),
            replay_frames = replay.len(),
            "server session attached to resumed transport",
        );
        self.pending_out.extend(replay);
        self.phase = SPhase::Active(t);
    }
}

impl Drop for ServerSessionTransport {
    fn drop(&mut self) {
        self.registry
            .lock()
            .expect("registry lock")
            .remove(&self.session_id);
    }
}

impl Stream for ServerSessionTransport {
    type Item = Frame;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Frame>> {
        let this = &mut *self;
        loop {
            // always check attach: the client may have reconnected before
            // we noticed the old socket's death
            if !this.attach_closed {
                match this.attach_rx.poll_recv(cx) {
                    Poll::Ready(Some((t, client_last))) => {
                        this.attach(t, client_last);
                    }
                    Poll::Ready(None) => this.attach_closed = true,
                    Poll::Pending => {}
                }
            }

            match &mut this.phase {
                SPhase::Dead => return Poll::Ready(None),

                SPhase::Detached(sleep) => match sleep.as_mut().poll(cx) {
                    Poll::Ready(()) => {
                        this.die();
                        return Poll::Ready(None);
                    }
                    Poll::Pending => return Poll::Pending,
                },

                SPhase::Active(t) => {
                    // idle detector: prolonged silence = dead transport
                    if let Some(timeout) = this.idle_timeout {
                        if this.idle_timer.is_none() {
                            let mut d = futures_timer::Delay::new(timeout);
                            let _ = Pin::new(&mut d).poll(cx);
                            this.idle_timer = Some(d);
                        }
                        if let Some(d) = &mut this.idle_timer
                            && Pin::new(d).poll(cx).is_ready()
                        {
                            tracing::warn!(
                                timeout_ms = timeout.as_millis(),
                                "server session transport idle; detaching",
                            );
                            this.idle_timer = None;
                            this.detach();
                            continue;
                        }
                    }
                    if let Some(d) = &mut this.ack_timer
                        && Pin::new(d).poll(cx).is_ready()
                    {
                        this.ack_timer = None;
                        if this.core.ack_pending() {
                            let a = this.core.make_ack();
                            this.pending_out.push_back(a);
                        }
                    }
                    if !Self::try_flush(t, &mut this.pending_out, cx) {
                        this.detach();
                        continue;
                    }
                    match t.poll_next_unpin(cx) {
                        Poll::Ready(Some(f)) => {
                            // any inbound frame resets the idle detector
                            if let Some(timeout) = this.idle_timeout {
                                let mut d = futures_timer::Delay::new(timeout);
                                let _ = Pin::new(&mut d).poll(cx);
                                this.idle_timer = Some(d);
                            }
                            match this.core.on_ingress(f) {
                                Ingress::Deliver { frame: f, ack_due } => {
                                    if matches!(f.kind, Some(frame::Kind::Hello(_))) {
                                        continue;
                                    }
                                    if ack_due {
                                        let a = this.core.make_ack();
                                        this.pending_out.push_back(a);
                                    } else if this.core.ack_pending() && this.ack_timer.is_none() {
                                        this.ack_timer =
                                            Some(futures_timer::Delay::new(this.ack_delay));
                                        if let Some(d) = &mut this.ack_timer {
                                            let _ = Pin::new(d).poll(cx);
                                        }
                                    }
                                    return Poll::Ready(Some(f));
                                }
                                Ingress::Consumed => continue,
                            }
                        }
                        Poll::Ready(None) => {
                            this.detach();
                            continue;
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
            }
        }
    }
}

impl Sink<Frame> for ServerSessionTransport {
    type Error = TransportClosed;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, item: Frame) -> Result<(), Self::Error> {
        let this = &mut *self;
        if matches!(this.phase, SPhase::Dead) {
            return Err(TransportClosed);
        }
        match this.core.on_egress(item) {
            Ok(stamped) => {
                if matches!(this.phase, SPhase::Active(_)) {
                    this.pending_out.push_back(stamped);
                } else if !sessioned(&stamped) {
                    // outside the session during a gap — drop
                }
                Ok(())
            }
            Err(_) => {
                tracing::warn!(
                    session_id = %this.session_log_id,
                    "server session replay buffer overflow; closing session",
                );
                this.die();
                Err(TransportClosed)
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = &mut *self;
        if let SPhase::Active(t) = &mut this.phase
            && !Self::try_flush(t, &mut this.pending_out, cx)
        {
            this.detach();
        }
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

// Stub keeping FrameDuplex used in signatures (see client.rs)
#[allow(unused)]
fn _assert_bounds(t: BoxFrameTransport) -> impl FrameDuplex {
    t
}
