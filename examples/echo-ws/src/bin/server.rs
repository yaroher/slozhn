#[tokio::main]
async fn main() {
    let addr = std::env::args().nth(1).unwrap_or_else(|| "127.0.0.1:50052".into());
    let session = std::env::args().nth(2).as_deref() == Some("session");
    let listener = tokio::net::TcpListener::bind(&addr).await.expect("bind");
    let router = if session { echo_ws::router_session() } else { echo_ws::router() };
    println!("echo-ws server on ws://{addr}/rpc (session={session})");
    axum::serve(listener, router).await.expect("serve");
}
