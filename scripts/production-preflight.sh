#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: scripts/production-preflight.sh /path/to/server.yml" >&2
  exit 2
fi

CONFIG="$1"
SERVER_BIN="${NEO4R_SERVER_BIN:-target/debug/neo4r-server}"

if [[ "${NEO4R_PREFLIGHT_SKIP_BUILD:-0}" != "1" ]]; then
  cargo build -p neo4r-server
elif [[ ! -x "$SERVER_BIN" ]]; then
  echo "server binary not found: $SERVER_BIN" >&2
  exit 1
fi

"$SERVER_BIN" --config "$CONFIG" --check-config
"$SERVER_BIN" --config "$CONFIG" --dump-config >/dev/null
"$SERVER_BIN" --config "$CONFIG" --production-check

bash -n scripts/*.sh
scripts/production-artifacts.sh "$CONFIG"
echo "neo4r production preflight passed for $CONFIG"
