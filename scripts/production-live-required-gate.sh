#!/usr/bin/env bash
set -euo pipefail

if [[ "${NEO4R_PRODUCTION_LIVE_REQUIRED:-0}" != "1" ]]; then
  echo "neo4r production live-required gate skipped; set NEO4R_PRODUCTION_LIVE_REQUIRED=1 in production CI"
  exit 0
fi

: "${NEO4R_RUN_MULTI_NODE_LIVE:=1}"
: "${NEO4R_RUN_RDMA_LIVE:=1}"
NEO4R_RUN_MULTI_NODE_LIVE="$NEO4R_RUN_MULTI_NODE_LIVE" scripts/multi-node-live-gate.sh
NEO4R_RUN_RDMA_LIVE="$NEO4R_RUN_RDMA_LIVE" scripts/rdma-live-gate.sh
echo "neo4r production live-required gate passed"
