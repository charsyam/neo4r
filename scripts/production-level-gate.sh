#!/usr/bin/env bash
set -euo pipefail

grep -q "snapshot plus WAL tail catch-up" docs/productionization.md
grep -q "gossip-to-membership reconciliation" docs/productionization.md
grep -q "replication backpressure" docs/productionization.md
grep -q "storage crash-point atomicity" docs/productionization.md
grep -q "mixed-version rolling upgrade" docs/productionization.md
grep -q "neo4r_replication_channel_backpressure_rejections_total" docs/prometheus_alerts.yml

scripts/membership-automation.sh
scripts/storage-atomicity.sh
scripts/pitr-restore-worker-gate.sh
scripts/failure-injection.sh
scripts/rolling-upgrade-gate.sh
scripts/bench-thresholds.sh
scripts/explicit-rbac-policy-gate.sh
scripts/production-readiness-gate.sh

echo "neo4r production-level gate passed"
