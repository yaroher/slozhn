#!/usr/bin/env bash
# Browser e2e: native echo-ws server + wasm-bindgen-test in headless chrome.
# Requires: wasm-pack, chrome/chromedriver (wasm-pack downloads chromedriver itself).
set -euo pipefail
cd "$(dirname "$0")/../.."

PORT=50123
cargo build -p echo-ws --bin server
./target/debug/server "127.0.0.1:${PORT}" session &
SERVER_PID=$!
trap 'kill ${SERVER_PID} 2>/dev/null || true' EXIT
sleep 1

cd examples/echo-wasm
wasm-pack test --headless --chrome
