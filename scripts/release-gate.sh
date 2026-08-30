#!/usr/bin/env bash
set -euo pipefail

scripts/ci-fast.sh
scripts/ci-correctness.sh
scripts/ci-crash.sh
scripts/ci-server.sh
scripts/protocol-compat.sh
scripts/protocol-matrix.sh
scripts/udp-transport.sh
scripts/snapshot-chunks.sh
scripts/session-security.sh
scripts/restore-drain.sh
scripts/read-consistency.sh
scripts/query-plan-golden.sh
scripts/query-statistics-maintenance-gate.sh
scripts/query-result-contract.sh
scripts/multi-node-live-gate.sh
scripts/membership-automation.sh
scripts/wal-compaction.sh
scripts/raft-soak.sh
scripts/transport-fault-model.sh
scripts/sdk-api-parity.sh
scripts/sdk-failover.sh
scripts/storage-atomicity.sh
scripts/failure-injection.sh
scripts/security-regression.sh
scripts/bench-thresholds.sh
scripts/bench-regression.sh
scripts/crash-consistency-gate.sh
scripts/pitr-archive-gate.sh
scripts/pitr-restore-worker-gate.sh
scripts/backup-consistency-gate.sh
scripts/object-storage-smoke.sh
scripts/object-storage-e2e.sh
scripts/production-live-workflow-gate.sh
scripts/query-cost-model-gate.sh
scripts/query-guardrail-gate.sh
scripts/query-spill-gate.sh
scripts/observability-slo-gate.sh
scripts/security-hardening-gate.sh
scripts/explicit-rbac-policy-gate.sh
scripts/tls-rotation-gate.sh
scripts/tls-cert-inventory-gate.sh
scripts/rolling-upgrade-gate.sh
scripts/schema-migration-gate.sh
scripts/schema-progress-gate.sh
scripts/safe-watermark-gc-gate.sh
scripts/gc-executor-gate.sh
scripts/repair-automation-gate.sh
scripts/production-live-required-gate.sh
scripts/production-chaos-longevity-gate.sh
scripts/rdma-live-gate.sh
scripts/production-hardening-gate.sh
scripts/runbook-drill-gate.sh
scripts/grafana-alert-gate.sh
scripts/packaged-observability-gate.sh
scripts/compatibility-matrix-gate.sh
scripts/previous-release-compat-gate.sh
scripts/production-artifacts.sh
scripts/packaging-readiness-gate.sh
scripts/kubernetes-manifest-smoke.sh

if [[ "${NEO4R_RUN_RELEASE_LIVE:-0}" == "1" ]]; then
  NEO4R_RUN_SDK_LIVE=1 scripts/sdk-live.sh
  NEO4R_RUN_MULTI_NODE_LIVE=1 scripts/multi-node-live-gate.sh
fi

scripts/release-metadata.sh
echo "neo4r release gate passed"
