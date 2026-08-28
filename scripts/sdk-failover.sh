#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cargo test -p neo4r-client redirect --quiet
PYTHONPATH="$ROOT/sdks/python" python3 -m unittest "$ROOT/sdks/python/tests/test_protocol.py"

if [[ "${NEO4R_RUN_SDK_FAILOVER_LIVE:-0}" != "1" ]]; then
  echo "sdk failover static checks passed; set NEO4R_RUN_SDK_FAILOVER_LIVE=1 for live topology failover checks"
  exit 0
fi

NEO4R_RUN_SDK_LIVE=1 scripts/sdk-live.sh
