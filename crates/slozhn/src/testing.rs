//! In-process client↔server harness for tests — the slozhn analogue of
//! gRPC's `bufconn`: a real client `Channel` talking to real `tonic::Routes`
//! over an in-memory transport, in one runtime, with no sockets, no ports,
//! and no axum.
//!
//! ```ignore
//! let routes = tonic::service::Routes::new(EchoServer::new(MyEcho));
//! let mut client = EchoClient::new(slozhn::testing::channel(routes));
//! let resp = client.unary(Request::new(msg)).await?;   // ordinary tonic
//! ```
//!
//! Three fidelity levels, cheapest first — pick the lowest one that still
//! exercises what you're testing:
//!
//! - [`channel`] — the client and server frame drivers are wired directly to
//!   each other. The envelope state machine, flow control, streams, metadata,
//!   trailers and any tower middleware all run for real; only the byte codec
//!   and the WebSocket are skipped. This is what you want for service logic
//!   and middleware tests.
//! - [`channel_over_bytes`] — same, but the two sides talk through the real
//!   byte codec over an in-memory byte pipe (this is the literal `bufconn`
//!   equivalent: a `[]byte` transport). Catches encode/decode and message
//!   framing bugs. A WebSocket adds nothing beyond message boundaries, which
//!   the byte pipe already preserves.
//! - [`session_channel`] — the full session layer (seq/ack, replay, resume)
//!   with a [`Breaker`] handle that severs the physical transport on demand.
//!   The client reconnects and resumes against the same `SessionManager`, so
//!   reconnect/resume behavior is testable end to end without touching the
//!   network.
//!
//! All three spawn their drivers on the ambient tokio runtime and shut down
//! when the returned channel (and its clones) are dropped.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll};

use futures::{Sink, Stream};
use slozhn_client::{Channel, Spawner};
use slozhn_frame::connection::{Config, bind, bind_pre_negotiated};
use slozhn_frame::error::TransportClosed;
use slozhn_frame::ids::Side;
use slozhn_frame::loopback;
use slozhn_frame::proto::v1::Frame;

/// The tower service a test harness serves: anything `grpc_ws` would accept.
pub trait TestService:
    tower::Service<
        http::Request<tonic::body::Body>,
        Response = http::Response<tonic::body::Body>,
    > + Clone
    + Send
    + 'static
{
}

impl<S> TestService for S
where
    S: tower::Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
        > + Clone
        + Send
        + 'static,
{
}

fn spawner() -> Spawner {
    Arc::new(|f| {
        tokio::spawn(f);
    })
}

/// In-process channel over a direct frame pipe. Skips the byte codec and the
/// WebSocket; everything above them is real.
///
/// The service is served until the channel is dropped.
pub fn channel<S>(svc: S) -> Channel
where
    S: TestService,
    S::Future: Send,
    S::Error: std::fmt::Display + Send,
{
    let (client_side, server_side) = loopback::pair();
    spawn_pair(client_side, server_side, svc)
}

/// In-process channel over a real byte pipe through the frame codec — the
/// literal `bufconn` equivalent. Use it when the encoding itself is under
/// test (message framing, oversized payloads, decode errors).
pub fn channel_over_bytes<S>(svc: S) -> Channel
where
    S: TestService,
    S::Future: Send,
    S::Error: std::fmt::Display + Send,
{
    let (client_bytes, server_bytes) = loopback::byte_pair();
    spawn_pair(
        slozhn_frame::codec::framed(client_bytes),
        slozhn_frame::codec::framed(server_bytes),
        svc,
    )
}

fn spawn_pair<C, T, S>(client_transport: C, server_transport: T, svc: S) -> Channel
where
    C: Stream<Item = Frame> + Sink<Frame, Error = TransportClosed> + Unpin + Send + 'static,
    T: Stream<Item = Frame> + Sink<Frame, Error = TransportClosed> + Unpin + Send + 'static,
    S: TestService,
    S::Future: Send,
    S::Error: std::fmt::Display + Send,
{
    let (client_conn, client_driver) = bind(Side::Client, Config::default(), client_transport);
    let (server_conn, server_driver) = bind(Side::Server, Config::default(), server_transport);

    tokio::spawn(async move {
        let _ = client_driver.run().await;
    });
    tokio::spawn(async move {
        let _ = server_driver.run().await;
    });
    tokio::spawn(slozhn_server::serve(server_conn, svc));

    Channel::new(client_conn, spawner())
}

/// Severs the physical transport of a [`session_channel`], simulating a
/// network break. The client's session layer notices, reconnects through the
/// factory, and resumes — exactly as it would against a real server.
#[derive(Clone)]
pub struct Breaker {
    broken: Arc<AtomicBool>,
    connects: Arc<AtomicUsize>,
}

impl Breaker {
    /// Kill the current physical connection. The session layer notices, and
    /// the next reconnect is handed a fresh pipe.
    pub fn kill(&self) {
        self.broken.store(true, Ordering::SeqCst);
    }

    /// How many physical connections the factory has handed out so far — 1
    /// after the initial handshake, 2 after the first successful reconnect.
    /// Lets a test assert that a break actually happened and was recovered,
    /// rather than passing because nothing was ever severed.
    pub fn connect_count(&self) -> usize {
        self.connects.load(Ordering::SeqCst)
    }
}

/// A frame pipe that dies once its `Breaker` is tripped: reads end (`None`)
/// and writes fail, which is exactly what the frame driver sees on a real
/// disconnect.
struct Breakable<T> {
    inner: T,
    broken: Arc<AtomicBool>,
    /// Trip state captured at construction — a fresh pipe built after a
    /// `kill()` must not be born dead, so the flag is reset when the factory
    /// hands out a new transport.
    dead: bool,
}

impl<T> Breakable<T> {
    fn is_dead(&mut self) -> bool {
        if self.dead {
            return true;
        }
        if self.broken.swap(false, Ordering::SeqCst) {
            self.dead = true;
        }
        self.dead
    }
}

impl<T: Stream<Item = Frame> + Unpin> Stream for Breakable<T> {
    type Item = Frame;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Frame>> {
        let this = self.get_mut();
        if this.is_dead() {
            return Poll::Ready(None);
        }
        Pin::new(&mut this.inner).poll_next(cx)
    }
}

impl<T: Sink<Frame, Error = TransportClosed> + Unpin> Sink<Frame> for Breakable<T> {
    type Error = TransportClosed;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        if this.is_dead() {
            return Poll::Ready(Err(TransportClosed));
        }
        Pin::new(&mut this.inner).poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Frame) -> Result<(), Self::Error> {
        let this = self.get_mut();
        if this.is_dead() {
            return Err(TransportClosed);
        }
        Pin::new(&mut this.inner).start_send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        if this.is_dead() {
            return Poll::Ready(Err(TransportClosed));
        }
        Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_close(cx)
    }
}

/// In-process channel WITH the session layer: streams survive a simulated
/// break. Returns the channel and a [`Breaker`] that severs the physical
/// transport; the client then reconnects through the same in-memory factory
/// and resumes against the same `SessionManager`.
///
/// ```ignore
/// let (channel, breaker) = slozhn::testing::session_channel(routes).await;
/// let mut client = EchoClient::new(channel);
/// let mut stream = client.server_stream(Request::new(count)).await?.into_inner();
/// breaker.kill();                       // network dies mid-stream
/// let next = stream.message().await?;   // resumed transparently
/// ```
pub async fn session_channel<S>(svc: S) -> (Channel, Breaker)
where
    S: TestService + Sync,
    S::Future: Send,
    S::Error: std::fmt::Display + Send,
{
    session_channel_with(svc, Default::default(), Default::default()).await
}

/// [`session_channel`] with explicit session configs (client and server).
pub async fn session_channel_with<S>(
    svc: S,
    client_config: slozhn_session::SessionConfig,
    server_config: slozhn_session::server::ServerSessionConfig,
) -> (Channel, Breaker)
where
    S: TestService + Sync,
    S::Future: Send,
    S::Error: std::fmt::Display + Send,
{
    let manager = slozhn_session::server::SessionManager::new(server_config);
    let broken = Arc::new(AtomicBool::new(false));
    let connects = Arc::new(AtomicUsize::new(0));

    // Every reconnect calls this: a fresh in-memory pipe whose server end is
    // handed to the SessionManager (which either creates a session or resumes
    // the existing one), while the client end goes back to the session
    // transport.
    let factory: slozhn_session::client::Factory = {
        let manager = manager.clone();
        let broken = broken.clone();
        let connects = connects.clone();
        let svc = svc.clone();
        Arc::new(move || {
            let manager = manager.clone();
            let broken = broken.clone();
            let connects = connects.clone();
            let svc = svc.clone();
            Box::pin(async move {
                let (client_side, server_side) = loopback::pair();
                // reset: a pipe created after a kill() must start alive
                broken.store(false, Ordering::SeqCst);
                connects.fetch_add(1, Ordering::SeqCst);

                let server_transport: slozhn_frame::transport::BoxFrameTransport =
                    Box::pin(Breakable {
                        inner: server_side,
                        broken: broken.clone(),
                        dead: false,
                    });

                tokio::spawn(async move {
                    match manager.accept(server_transport).await {
                        Ok(Some((transport, peer_hello))) => {
                            let (conn, driver) = bind_pre_negotiated(
                                Side::Server,
                                Config::default(),
                                peer_hello,
                                transport,
                            );
                            tokio::spawn(async move {
                                let _ = driver.run().await;
                            });
                            slozhn_server::serve(conn, svc).await;
                        }
                        // resumed into an existing session, or rejected —
                        // nothing to serve on this physical connection
                        Ok(None) => {}
                        // a handshake failure here means the pipe died before
                        // Hello: the client's reconnect loop will retry
                        Err(_) => {}
                    }
                });

                let client_transport: slozhn_frame::transport::BoxFrameTransport =
                    Box::pin(Breakable {
                        inner: client_side,
                        broken,
                        dead: false,
                    });
                Ok(client_transport)
            }) as futures::future::BoxFuture<'static, Result<_, String>>
        })
    };

    let (transport, peer_hello) = slozhn_session::client::connect_session(
        factory,
        Config::default(),
        client_config,
    )
    .await
    .expect("in-memory session handshake");

    let (conn, driver) =
        bind_pre_negotiated(Side::Client, Config::default(), peer_hello, transport);
    tokio::spawn(async move {
        let _ = driver.run().await;
    });

    (Channel::new(conn, spawner()), Breaker { broken, connects })
}
