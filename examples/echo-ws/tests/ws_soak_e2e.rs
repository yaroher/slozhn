//! Soak/chaos e2e: hammer a session channel with concurrent unary calls
//! while a test-owned TCP proxy randomly kills the underlying connection,
//! then verify the stack both recovers traffic and leaks nothing (no
//! lingering sessions/connections) once every client handle is dropped.
//!
//! Manual only — `SLOZHN_SOAK_SECS=8 cargo test -p echo-ws --test
//! ws_soak_e2e -- --ignored --nocapture`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use slozhn_proto::testing::v1::Msg;
use slozhn_proto::testing::v1::echo_client::EchoClient;
use tonic::Request;

/// Tiny self-contained xorshift64 PRNG — good enough for jitter/payload
/// sizes in a test, no extra crate dependency needed.
struct Xorshift64(u64);

impl Xorshift64 {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    /// Inclusive range `[lo, hi]`.
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next_u64() % (hi - lo + 1)
    }
}

fn seed_from_time(salt: u64) -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x9E3779B97F4A7C15);
    nanos ^ salt.wrapping_mul(0x2545_F491_4F6C_DD1D)
}

/// Test-owned TCP proxy: dropping it kills every established connection
/// instantly. Mirrors the pattern in `ws_reconnect_e2e.rs`.
async fn start_proxy(
    backend: std::net::SocketAddr,
    front: Option<std::net::SocketAddr>,
) -> (std::net::SocketAddr, tokio::task::JoinSet<()>) {
    let bind_addr = front.unwrap_or_else(|| "127.0.0.1:0".parse().unwrap());
    let listener = loop {
        match tokio::net::TcpListener::bind(bind_addr).await {
            Ok(l) => break l,
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
        }
    };
    let addr = listener.local_addr().unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn(async move {
        let mut conns = tokio::task::JoinSet::new();
        loop {
            let Ok((mut front_sock, _)) = listener.accept().await else { break };
            conns.spawn(async move {
                let Ok(mut back_sock) = tokio::net::TcpStream::connect(backend).await else {
                    return;
                };
                let _ = tokio::io::copy_bidirectional(&mut front_sock, &mut back_sock).await;
            });
        }
    });
    (addr, tasks)
}

#[tokio::test]
#[ignore = "soak: run manually with --ignored"]
async fn soak_survives_repeated_breaks_without_leaking() {
    let soak_secs: u64 = std::env::var("SLOZHN_SOAK_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20);
    let total = Duration::from_secs(soak_secs);
    let clean_window = Duration::from_secs(3).min(total);

    // --- server: session layer with a SHORT ttl so idle-detach reaping is
    // observable within the test's poll window ---
    let manager = slozhn::server::SessionManager::new(slozhn::server::ServerSessionConfig {
        ttl: Duration::from_secs(2),
        ..Default::default()
    });
    let registry = slozhn::server::ConnectionRegistry::new();
    let routes = tonic::service::Routes::new(
        slozhn_proto::testing::v1::echo_server::EchoServer::new(echo_ws::EchoImpl),
    );
    let router = axum::Router::new().route(
        "/rpc",
        slozhn::server::grpc_ws_session_with_registry(routes, manager.clone(), registry.clone()),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });

    // --- test-owned proxy in front of the backend ---
    let (front, mut proxy) = start_proxy(backend, None).await;

    // --- one session channel shared by 4 concurrent client tasks ---
    let channel = slozhn::client::builder(format!("ws://{front}/rpc"))
        .resume()
        .build();

    let ok_count = Arc::new(AtomicU64::new(0));
    let failed_count = Arc::new(AtomicU64::new(0));
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);

    let mut clients = tokio::task::JoinSet::new();
    for worker_id in 0..4u64 {
        let mut client = EchoClient::new(channel.clone());
        let ok_count = ok_count.clone();
        let failed_count = failed_count.clone();
        let mut stop_rx = stop_rx.clone();
        let mut rng = Xorshift64::new(seed_from_time(worker_id + 1));
        clients.spawn(async move {
            while !*stop_rx.borrow() {
                let len = rng.range(1, 64) as usize;
                let mut payload = vec![0u8; len];
                for b in &mut payload {
                    *b = rng.next_u64() as u8;
                }
                let call = client.unary(Request::new(Msg { payload: Bytes::from(payload) }));
                match tokio::time::timeout(Duration::from_secs(2), call).await {
                    Ok(Ok(_)) => {
                        ok_count.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {
                        failed_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
                // small pacing so a single worker doesn't spin the loop
                // budget entirely on one CPU when the connection is down
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {}
                    _ = stop_rx.changed() => {}
                }
            }
        });
    }

    // --- chaos: kill + rebuild the proxy's accept loop on the same port
    // every 1-3s, for the whole soak minus the trailing clean window ---
    let mut chaos_rng = Xorshift64::new(seed_from_time(0xC1405));
    let start = Instant::now();
    let chaos_end_at = start + total.saturating_sub(clean_window);
    let mut break_count = 0u64;
    while Instant::now() < chaos_end_at {
        let wait_ms = chaos_rng.range(1_000, 3_000);
        let remaining = chaos_end_at.saturating_duration_since(Instant::now());
        tokio::time::sleep(Duration::from_millis(wait_ms).min(remaining)).await;
        if Instant::now() >= chaos_end_at {
            // don't start a break we won't have time to finish observing —
            // leave the proxy stable going into the clean window
            break;
        }
        drop(proxy);
        let (new_front, new_proxy) = start_proxy(backend, Some(front)).await;
        assert_eq!(new_front, front, "proxy must keep the same listen port");
        proxy = new_proxy;
        break_count += 1;
    }

    // --- end conditions: proxy is stable now (guaranteed by the loop
    // above); let clients run cleanly and assert some traffic went through ---
    let ok_before_clean = ok_count.load(Ordering::Relaxed);
    tokio::time::sleep(clean_window).await;
    let ok_after_clean = ok_count.load(Ordering::Relaxed);
    assert!(
        ok_after_clean > ok_before_clean,
        "expected successful calls during the trailing clean window: before={ok_before_clean} after={ok_after_clean}"
    );

    // --- drop all clients/channels ---
    let _ = stop_tx.send(true);
    while clients.join_next().await.is_some() {}
    drop(channel);
    drop(proxy);

    // --- no leaks: session TTL + idle detach must reap the session, and
    // the connection registry must drain ---
    let leak_deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let sessions = manager.session_count();
        let connections = registry.len();
        if sessions == 0 && connections == 0 {
            break;
        }
        assert!(
            Instant::now() < leak_deadline,
            "leak detected after soak: session_count={sessions} registry.len()={connections}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(manager.session_count(), 0);
    assert_eq!(registry.len(), 0);

    println!(
        "soak stats: duration={}s breaks={} ok={} failed={}",
        soak_secs,
        break_count,
        ok_count.load(Ordering::Relaxed),
        failed_count.load(Ordering::Relaxed),
    );
}
