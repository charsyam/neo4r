#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HOST="${NEO4R_SDK_LIVE_HOST:-127.0.0.1}"
PORT="${NEO4R_SDK_LIVE_PORT:-17687}"
WEB_PORT="${NEO4R_SDK_LIVE_WEB_PORT:-17474}"
ADDR="$HOST:$PORT"
DATA_DIR="${NEO4R_SDK_LIVE_DATA_DIR:-$ROOT/target/neo4r-sdk-live}"
ADMIN_TOKEN="${NEO4R_SDK_LIVE_ADMIN_TOKEN:-admin:secret}"

cargo build -p neo4r-server -p neo4r-client
rm -rf "$DATA_DIR"
"$ROOT/target/debug/neo4r-server" \
  --bind "$ADDR" \
  --web-bind "$HOST:$WEB_PORT" \
  --web-auth-token "$ADMIN_TOKEN" \
  --data-dir "$DATA_DIR" \
  --shards 1 \
  --partitions 1 &
pid="$!"
cleanup() {
  kill "$pid" 2>/dev/null || true
}
trap cleanup EXIT
sleep 1

cargo run -p neo4r-client --example basic_usage -- "$ADDR"
PYTHONPATH="$ROOT/sdks/python" python3 "$ROOT/sdks/python/examples/basic_usage.py" \
  --host "$HOST" \
  --port "$PORT"
PYTHONPATH="$ROOT/sdks/python" python3 "$ROOT/sdks/python/examples/http_admin_tenant.py" \
  --base-url "http://$HOST:$WEB_PORT" \
  --admin-token "$ADMIN_TOKEN"
