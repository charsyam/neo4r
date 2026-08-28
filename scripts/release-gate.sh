#!/usr/bin/env bash
set -euo pipefail

scripts/ci-fast.sh
scripts/ci-correctness.sh
scripts/ci-crash.sh
scripts/ci-server.sh
scripts/protocol-compat.sh
scripts/protocol-matrix.sh
scripts/read-consistency.sh
scripts/query-plan-golden.sh
scripts/query-result-contract.sh
scripts/multi-node-live-gate.sh
scripts/membership-automation.sh
scripts/wal-compaction.sh
scripts/transport-fault-model.sh
scripts/sdk-api-parity.sh
scripts/sdk-failover.sh
scripts/storage-atomicity.sh
scripts/failure-injection.sh
scripts/security-regression.sh
scripts/bench-thresholds.sh
scripts/bench-regression.sh

if [[ "${NEO4R_RUN_RELEASE_LIVE:-0}" == "1" ]]; then
  NEO4R_RUN_SDK_LIVE=1 scripts/sdk-live.sh
  NEO4R_RUN_MULTI_NODE_LIVE=1 scripts/multi-node-live-gate.sh
fi

scripts/release-metadata.sh
echo "neo4r release gate passed"
