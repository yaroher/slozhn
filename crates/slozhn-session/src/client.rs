//! Client-side session transport: a Frame duplex that internally handles
//! physical reconnects, the resume handshake, replay and dedup. The logical
//! connection (`bind_pre_negotiated`) on top notices nothing.
//!
//! Invariants:
//! - on disconnect `pending_out` is cleared: sessioned frames are already in
//!   the core's replay buffer and will be resent after resume in seq order;
//! - in the Resuming phase ONLY Hello goes out — data strictly after the
//!   server replies (otherwise seq order breaks and dedup loses frames);
//! - resume rejected / buffer overflow → the transport terminates honestly
//!   (`None`), the frame driver dies, RPCs get UNAVAILABLE.

use std::collections::VecDeque;
use std::fmt::Write as _;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{Future, Sink, SinkExt, Stream, StreamExt};
use slozhn_frame::TransportClosed;
use slozhn_frame::proto::v1::{Frame, Hello, frame};

use slozhn_frame::transport::{ConnState, ReconnectHooks, jittered};

use crate::core::{Ingress, SessionCore, sessioned};
use crate::{SessionConfig, SessionError};

pub use slozhn_frame::transport::{BoxFrameTransport, FrameDuplex};

pub type Factory =
    Arc<dyn Fn() -> BoxFuture<'static, Result<BoxFrameTransport, String>> + Send + Sync>;

fn session_label(session_id: &Bytes) -> String {
    let mut out = String::with_capacity(16);
    for b in session_id.iter().take(8) {
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}

/// First connect + Hello handshake of a new session. Returns the transport
/// and the server's Hello — for `bind_pre_negotiated`.
pub async fn connect_session(
    factory: Factory,
    frame_config: slozhn_frame::connection::Config,
    session_config: SessionConfig,
) -> Result<(SessionTransport, Hello), SessionError> {
    let (hooks, _rx) = ReconnectHooks::new();
    connect_session_hooked(factory, frame_config, session_config, hooks).await
}

/// Like [`connect_session`], but reporting reconnect state into an external
/// hooks pair (see `slozhn_frame::transport::ReconnectHooks`); its `kick`
/// punches through a backoff wait.
pub async fn connect_session_hooked(
    factory: Factory,
    frame_config: slozhn_frame::connection::Config,
    session_config: SessionConfig,
    hooks: ReconnectHooks,
) -> Result<(SessionTransport, Hello), SessionError> {
    hooks.set(ConnState::Connecting);
    tracing::debug!("session initial connect starting");
    let mut t = (factory)().await.map_err(|e| {
        tracing::warn!(error = %e, "session initial transport connect failed");
        SessionError::Handshake(e)
    })?;
    t.send(hello_frame(&frame_config, Bytes::new(), Bytes::new(), 0))
        .await
        .map_err(|_| {
            tracing::warn!("session transport closed during initial hello");
            SessionError::Handshake("transport closed during hello".into())
        })?;
    let first = t.next().await.ok_or_else(|| {
        tracing::warn!("session transport closed before initial hello reply");
        SessionError::Handshake("closed before hello reply".into())
    })?;
    let peer = match first.kind {
        Some(frame::Kind::Hello(h)) if !h.resume_rejected => h,
        Some(frame::Kind::Hello(_)) => {
            tracing::warn!("session initial hello was rejected");
            return Err(SessionError::ResumeRejected);
        }
        _ => {
            tracing::warn!("session initial handshake received non-hello frame");
            return Err(SessionError::Handshake("expected hello reply".into()));
        }
    };
    if peer.session_id.is_empty() {
        // server without a session layer: seq frames would be silently
        // dropped — refuse explicitly instead of a quiet incompatibility
        tracing::warn!("session initial hello had no session id");
        return Err(SessionError::Handshake(
            "server has no session support (use grpc_ws_session)".into(),
        ));
    }
    let session_log_id = session_label(&peer.session_id);
    tracing::info!(session_id = %session_log_id, last_recv_seq = peer.last_recv_seq, "session established");
    hooks.set(ConnState::Connected);
    let transport = SessionTransport {
        phase: Phase::Active(t),
        core: SessionCore::new(session_config.replay_buffer_bytes, session_config.ack_every),
        factory,
        frame_config,
        session_id: peer.session_id.clone(),
        session_log_id,
        token: peer.resume_token.clone(),
        pending_out: VecDeque::new(),
        ack_timer: None,
        ack_delay: session_config.ack_delay,
        backoff_cur: session_config.initial_backoff,
        backoff_start: session_config.initial_backoff,
        backoff_max: session_config.max_backoff,
        hooks,
        attempt: 0,
        kick_wait: None,
    };
    Ok((transport, peer))
}

fn hello_frame(
    cfg: &slozhn_frame::connection::Config,
    session_id: Bytes,
    token: Bytes,
    last_recv_seq: u64,
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
            resume_rejected: false,
        })),
    }
}

enum Phase {
    Active(BoxFrameTransport),
    Connecting(BoxFuture<'static, Result<BoxFrameTransport, String>>),
    Backoff(futures_timer::Delay),
    /// Hello sent (sitting in pending_out), waiting for the server's reply.
    Resuming(BoxFrameTransport),
    Dead,
}

pub struct SessionTransport {
    phase: Phase,
    core: SessionCore,
    factory: Factory,
    frame_config: slozhn_frame::connection::Config,
    session_id: Bytes,
    session_log_id: String,
    token: Bytes,
    pending_out: VecDeque<Frame>,
    ack_timer: Option<futures_timer::Delay>,
    ack_delay: Duration,
    backoff_cur: Duration,
    backoff_start: Duration,
    backoff_max: Duration,
    hooks: ReconnectHooks,
    attempt: u32,
    /// Armed while in Backoff: reconnect_now() future.
    kick_wait: Option<BoxFuture<'static, ()>>,
}

enum FlushOutcome {
    Ok,
    Broken,
}

impl SessionTransport {
    /// Drain pending_out into the transport. Broken = physical disconnect.
    fn try_flush(
        t: &mut BoxFrameTransport,
        pending: &mut VecDeque<Frame>,
        cx: &mut Context<'_>,
    ) -> FlushOutcome {
        while !pending.is_empty() {
            match t.poll_ready_unpin(cx) {
                Poll::Ready(Ok(())) => {
                    let f = pending.pop_front().expect("checked non-empty");
                    if t.start_send_unpin(f).is_err() {
                        return FlushOutcome::Broken;
                    }
                }
                Poll::Ready(Err(_)) => return FlushOutcome::Broken,
                Poll::Pending => break,
            }
        }
        match t.poll_flush_unpin(cx) {
            Poll::Ready(Err(_)) => FlushOutcome::Broken,
            _ => FlushOutcome::Ok,
        }
    }

    /// Disconnect: pending is cleared (sessioned frames are already in the replay buffer), reconnect.
    fn go_reconnect(&mut self) {
        tracing::debug!(
            session_id = %self.session_log_id,
            last_recv_seq = self.core.last_recv_seq(),
            pending_out = self.pending_out.len(),
            "session physical transport disconnected; reconnecting",
        );
        self.pending_out.clear();
        self.ack_timer = None;
        self.kick_wait = None;
        self.hooks.set(ConnState::Connecting);
        self.phase = Phase::Connecting((self.factory)());
    }
}

impl Stream for SessionTransport {
    type Item = Frame;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Frame>> {
        let this = &mut *self;
        loop {
            match &mut this.phase {
                Phase::Dead => return Poll::Ready(None),

                Phase::Backoff(delay) => {
                    if this.kick_wait.is_none() {
                        let kick = this.hooks.kick.clone();
                        this.kick_wait = Some(Box::pin(async move { kick.notified().await }));
                    }
                    let kicked = matches!(
                        this.kick_wait
                            .as_mut()
                            .expect("armed above")
                            .as_mut()
                            .poll(cx),
                        Poll::Ready(())
                    );
                    match Pin::new(delay).poll(cx) {
                        Poll::Ready(()) => {}
                        Poll::Pending if kicked => {} // reconnect_now() punched through
                        Poll::Pending => return Poll::Pending,
                    }
                    this.kick_wait = None;
                    tracing::debug!("session reconnect backoff elapsed");
                    this.hooks.set(ConnState::Connecting);
                    this.phase = Phase::Connecting((this.factory)());
                }

                Phase::Connecting(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(t)) => {
                        tracing::debug!(
                            session_id = %this.session_log_id,
                            last_recv_seq = this.core.last_recv_seq(),
                            "session physical reconnect established; sending resume hello",
                        );
                        this.pending_out.push_front(hello_frame(
                            &this.frame_config,
                            this.session_id.clone(),
                            this.token.clone(),
                            this.core.last_recv_seq(),
                        ));
                        this.phase = Phase::Resuming(t);
                    }
                    Poll::Ready(Err(_)) => {
                        this.attempt += 1;
                        let delay = jittered(this.backoff_cur);
                        this.backoff_cur = (this.backoff_cur * 2).min(this.backoff_max);
                        tracing::warn!(
                            session_id = %this.session_log_id,
                            attempt = this.attempt,
                            delay_ms = delay.as_millis(),
                            "session physical reconnect failed",
                        );
                        this.hooks.set(ConnState::Backoff {
                            delay,
                            attempt: this.attempt,
                        });
                        this.phase = Phase::Backoff(futures_timer::Delay::new(delay));
                    }
                    Poll::Pending => return Poll::Pending,
                },

                Phase::Resuming(t) => {
                    if matches!(
                        Self::try_flush(t, &mut this.pending_out, cx),
                        FlushOutcome::Broken
                    ) {
                        this.go_reconnect();
                        continue;
                    }
                    match t.poll_next_unpin(cx) {
                        Poll::Ready(Some(f)) => match f.kind {
                            Some(frame::Kind::Hello(h)) => {
                                if h.resume_rejected {
                                    tracing::warn!(
                                        session_id = %this.session_log_id,
                                        "session resume rejected by server"
                                    );
                                    this.hooks.set(ConnState::Disconnected);
                                    this.phase = Phase::Dead;
                                    return Poll::Ready(None);
                                }
                                this.backoff_cur = this.backoff_start;
                                this.attempt = 0;
                                this.hooks.set(ConnState::Connected);
                                let replay = this.core.replay_after(h.last_recv_seq);
                                tracing::info!(
                                    session_id = %this.session_log_id,
                                    server_last_recv_seq = h.last_recv_seq,
                                    replay_frames = replay.len(),
                                    "session resume accepted",
                                );
                                this.pending_out.extend(replay);
                                let Phase::Resuming(t) =
                                    std::mem::replace(&mut this.phase, Phase::Dead)
                                else {
                                    unreachable!()
                                };
                                this.phase = Phase::Active(t);
                            }
                            _ => {
                                // garbage before Hello — protocol filth
                                tracing::warn!(
                                    session_id = %this.session_log_id,
                                    "session resume received non-hello frame"
                                );
                                this.hooks.set(ConnState::Disconnected);
                                this.phase = Phase::Dead;
                                return Poll::Ready(None);
                            }
                        },
                        Poll::Ready(None) => this.go_reconnect(),
                        Poll::Pending => return Poll::Pending,
                    }
                }

                Phase::Active(t) => {
                    // 1. ack timer
                    if let Some(d) = &mut this.ack_timer
                        && Pin::new(d).poll(cx).is_ready()
                    {
                        this.ack_timer = None;
                        if this.core.ack_pending() {
                            let a = this.core.make_ack();
                            this.pending_out.push_back(a);
                        }
                    }
                    // 2. flush outgoing
                    if matches!(
                        Self::try_flush(t, &mut this.pending_out, cx),
                        FlushOutcome::Broken
                    ) {
                        this.go_reconnect();
                        continue;
                    }
                    // 3. receive
                    match t.poll_next_unpin(cx) {
                        Poll::Ready(Some(f)) => match this.core.on_ingress(f) {
                            Ingress::Deliver { frame: f, ack_due } => {
                                if matches!(f.kind, Some(frame::Kind::Hello(_))) {
                                    continue; // repeated Hello — not for the driver
                                }
                                if ack_due {
                                    let a = this.core.make_ack();
                                    this.pending_out.push_back(a);
                                } else if this.core.ack_pending() && this.ack_timer.is_none() {
                                    this.ack_timer =
                                        Some(futures_timer::Delay::new(this.ack_delay));
                                    // arm the timer (registers the waker)
                                    if let Some(d) = &mut this.ack_timer {
                                        let _ = Pin::new(d).poll(cx);
                                    }
                                }
                                return Poll::Ready(Some(f));
                            }
                            Ingress::Consumed => continue,
                        },
                        Poll::Ready(None) => {
                            this.go_reconnect();
                            continue;
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
            }
        }
    }
}

impl Sink<Frame> for SessionTransport {
    type Error = TransportClosed;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Simple v1: the volume limit comes from the replay buffer —
        // its overflow honestly kills the session (spec §8).
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, item: Frame) -> Result<(), Self::Error> {
        let this = &mut *self;
        if matches!(this.phase, Phase::Dead) {
            return Err(TransportClosed);
        }
        match this.core.on_egress(item) {
            Ok(stamped) => {
                if matches!(this.phase, Phase::Active(_)) {
                    this.pending_out.push_back(stamped);
                } else if !sessioned(&stamped) {
                    // ping etc. outside the session are pointless during a gap — drop
                } // sessioned frames are already in the replay buffer — will arrive after resume
                Ok(())
            }
            Err(_) => {
                tracing::warn!(
                    session_id = %this.session_log_id,
                    "session replay buffer overflow; closing logical session"
                );
                this.phase = Phase::Dead;
                Err(TransportClosed)
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = &mut *self;
        if let Phase::Active(t) = &mut this.phase
            && matches!(
                Self::try_flush(t, &mut this.pending_out, cx),
                FlushOutcome::Broken
            )
        {
            this.go_reconnect();
        }
        // in the other phases data sits in the buffer — "flush" is done
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}
