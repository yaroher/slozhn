# browser-app: TS UI + Rust wasm core

A real browser app: a TS interface calls a Rust core (wasm), and the core
talks to a gRPC server over WebSocket with the session layer (breaks are
survived).

## Running

```bash
# 1. server (with the session layer)
cargo run -p echo-ws --bin server -- 127.0.0.1:50052 session

# 2. wasm core
cd examples/browser-app/core
wasm-pack build --target web

# 3. UI
cd ../ui
npm install
npm run dev   # open http://localhost:5173
```

Resume check: while it's running, kill the server (Ctrl-C) and bring it back
up — calls keep working without reloading the page.
