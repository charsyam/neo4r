#!/usr/bin/env bash
set -euo pipefail

CONFIG="${1:-packaging/server.production.yml}"

required_files=(
  docs/pitr_restore_drill.yml
  docs/rolling_upgrade_manifest.yml
  docs/query_regression_corpus.yml
  docs/prometheus_alerts.yml
  docs/production_runbook.md
  packaging/neo4r-server.service
  packaging/neo4r.logrotate
  packaging/server.production.yml
)

for path in "${required_files[@]}"; do
  if [[ ! -s "$path" ]]; then
    echo "missing production artifact: $path" >&2
    exit 1
  fi
done

grep -q "wal_archive_dir:" "$CONFIG"
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

echo "neo4r production artifact checks passed"
