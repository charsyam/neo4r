#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HOST="${NEO4R_SDK_COMPAT_HOST:-127.0.0.1}"
PORT="${NEO4R_SDK_COMPAT_PORT:-17687}"
ADDR="$HOST:$PORT"
DATA_DIR="${NEO4R_SDK_COMPAT_DATA_DIR:-$ROOT/target/neo4r-sdk-compat}"

cargo test -p neo4r-protocol -p neo4r-client
PYTHONPATH="$ROOT/sdks/python" python3 -m unittest discover -s "$ROOT/sdks/python/tests"
scripts/sdk-api-parity.sh

if [[ "${NEO4R_RUN_SDK_COMPAT:-0}" != "1" && "${NEO4R_RUN_SDK_LIVE:-0}" != "1" ]]; then
  echo "sdk compatibility static checks passed; set NEO4R_RUN_SDK_COMPAT=1 for live server examples"
  exit 0
fi

cargo build -p neo4r-server -p neo4r-client
rm -rf "$DATA_DIR"
"$ROOT/target/debug/neo4r-server" --bind "$ADDR" --data-dir "$DATA_DIR" --shards 1 --partitions 1 &
pid="$!"
cleanup() {
  kill "$pid" 2>/dev/null || true
}
trap cleanup EXIT
sleep 1

cargo run -p neo4r-client --example basic_usage -- "$ADDR"
PYTHONPATH="$ROOT/sdks/python" python3 "$ROOT/sdks/python/examples/basic_usage.py" --host "$HOST" --port "$PORT"
