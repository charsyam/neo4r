#!/usr/bin/env bash
set -euo pipefail

cargo test -p neo4r-server --test multi_process_cluster_smoke
cargo test -p neo4r-server --test jepsen_lite_correctness
NEO4R_RUN_SDK_COMPAT=0 NEO4R_RUN_SDK_LIVE=0 scripts/sdk-compat.sh
scripts/sdk-failover.sh
scripts/protocol-compat.sh
scripts/read-consistency.sh
scripts/query-plan-golden.sh
scripts/query-result-contract.sh
scripts/storage-atomicity.sh
scripts/failure-injection.sh
scripts/security-regression.sh
scripts/multi-node-live-gate.sh
scripts/membership-automation.sh
scripts/wal-compaction.sh
scripts/transport-fault-model.sh
scripts/multi-node-integration.sh
scripts/bench-regression.sh

if [[ "${NEO4R_RUN_SDK_LIVE:-0}" == "1" ]]; then
  scripts/sdk-live.sh
fi

if [[ "${NEO4R_RUN_CLUSTER_SMOKE:-0}" == "1" ]]; then
  scripts/multi_process_cluster_smoke.sh
fi

if [[ "${NEO4R_RUN_CLUSTER_CHAOS:-0}" == "1" ]]; then
  scripts/cluster-chaos-smoke.sh
fi

if [[ "${NEO4R_RUN_JEPSEN_LITE:-0}" == "1" ]]; then
  scripts/jepsen-lite-correctness.sh
fi
