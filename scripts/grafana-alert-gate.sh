#!/usr/bin/env bash
set -euo pipefail

grep -q "Neo4r Production Overview" docs/grafana_dashboard.json
grep -q "neo4r_database_shard_lag" docs/grafana_dashboard.json
grep -q "neo4r_backup_restore_last_success_timestamp_seconds" docs/grafana_dashboard.json
grep -q "neo4r_storage_repair_failures_total" docs/prometheus_alerts.yml

echo "neo4r grafana alert gate passed"
