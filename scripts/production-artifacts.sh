#!/usr/bin/env bash
set -euo pipefail

CONFIG="${1:-packaging/server.production.yml}"

required_files=(
  docs/pitr_restore_drill.yml
  docs/pitr_archive_contract.yml
  docs/backup_consistency_contract.yml
  docs/rolling_upgrade_manifest.yml
  docs/query_regression_corpus.yml
  docs/query_cost_model.yml
  docs/prometheus_alerts.yml
  docs/observability_slo.yml
  docs/security_hardening_contract.yml
  docs/repair_automation_contract.yml
  docs/packaging_readiness.yml
  docs/raft_production_semantics.md
  docs/production_runbook.md
  packaging/neo4r-server.service
  packaging/neo4r-server.env
  packaging/neo4r.logrotate
  packaging/server.production.yml
  packaging/kubernetes/neo4r-configmap.yml
  packaging/kubernetes/neo4r-service.yml
  packaging/kubernetes/neo4r-statefulset.yml
)

for path in "${required_files[@]}"; do
  if [[ ! -s "$path" ]]; then
    echo "missing production artifact: $path" >&2
    exit 1
  fi
done

grep -q "wal_archive_dir:" "$CONFIG"
grep -q "web_tls_mode: required" "$CONFIG"
grep -q "web_tls_cert:" "$CONFIG"
grep -q "web_tls_key:" "$CONFIG"
grep -q "restore_drill_manifest:" "$CONFIG"
grep -q "upgrade_manifest:" "$CONFIG"
grep -q "query_regression_corpus:" "$CONFIG"
grep -q "observability_alerts:" "$CONFIG"
grep -q "repair_check_on_startup: true" "$CONFIG"
grep -q "chaos_gate_required: true" "$CONFIG"
grep -q "systemd_unit:" "$CONFIG"
grep -q "logrotate:" "$CONFIG"
grep -q "Neo4rRaftNoLeader" docs/prometheus_alerts.yml
grep -q "data_format_version: 1" docs/rolling_upgrade_manifest.yml
grep -q "query:" docs/query_regression_corpus.yml
grep -q "tls-web-admin" docs/security_hardening_contract.yml
grep -q "health-probe" docs/packaging_readiness.yml
grep -q "startup_check: required" docs/repair_automation_contract.yml
grep -q "timestamp-targeted-restore-dry-run" docs/pitr_archive_contract.yml
grep -q "backup-restore-e2e-smoke" docs/backup_consistency_contract.yml
grep -q "kubernetes-manifest-smoke" docs/packaging_readiness.yml
grep -q "kind: StatefulSet" packaging/kubernetes/neo4r-statefulset.yml
grep -q "scripts/raft-soak.sh" docs/raft_production_semantics.md

echo "neo4r production artifact checks passed"
