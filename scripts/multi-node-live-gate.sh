#!/usr/bin/env bash
set -euo pipefail

cargo test -p neo4r-server --test multi_process_cluster_smoke --quiet
cargo test -p neo4r-server --test jepsen_lite_correctness --quiet
scripts/multi-node-integration.sh

if [[ "${NEO4R_RUN_MULTI_NODE_LIVE:-0}" == "1" ]]; then
  NEO4R_RUN_MULTI_NODE=1 scripts/multi-node-integration.sh
  NEO4R_RUN_CLUSTER_SMOKE=1 scripts/multi_process_cluster_smoke.sh
  NEO4R_RUN_JEPSEN_LITE=1 scripts/jepsen-lite-correctness.sh
fi

echo "neo4r multi-node live gate passed"
