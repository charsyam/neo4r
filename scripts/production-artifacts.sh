#!/usr/bin/env bash
set -euo pipefail

CONFIG="${1:-packaging/server.production.yml}"

required_files=(
  docs/pitr_restore_drill.yml
  docs/pitr_archive_contract.yml
  docs/backup_consistency_contract.yml
  docs/object_storage_archive.yml
  docs/object_storage_e2e.yml
  docs/rolling_upgrade_manifest.yml
  docs/release_compatibility_matrix.yml
  docs/query_regression_corpus.yml
  docs/query_cost_model.yml
  docs/query_statistics_maintenance.yml
  docs/query_guardrails.yml
  docs/query_spill_plan.yml
  docs/prometheus_alerts.yml
  docs/grafana_dashboard.json
  docs/observability_slo.yml
  docs/security_hardening_contract.yml
  docs/rbac_policy.md
  docs/tls_rotation_runbook.md
  docs/tls_cert_inventory.yml
  docs/schema_migration_contract.yml
  docs/schema_migration_progress_example.json
  docs/safe_watermark_gc.yml
  docs/gc_executor_contract.yml
  docs/repair_automation_contract.yml
  docs/packaging_readiness.yml
  docs/raft_production_semantics.md
  docs/production_runbook.md
  docs/incident_runbook.md
  docs/chaos_longevity_plan.yml
  docs/pitr_restore_apply.md
  docs/previous_release_fixture.yml
  docs/restore_drill_schedule.yml
  docs/slo_dashboard_example.yml
  packaging/neo4r-server.service
  packaging/neo4r-server.env
  packaging/neo4r.logrotate
  packaging/grafana-dashboard.yml
  packaging/server.production.yml
  packaging/kubernetes/neo4r-configmap.yml
  packaging/kubernetes/neo4r-service.yml
  packaging/kubernetes/neo4r-statefulset.yml
  packaging/kubernetes/neo4r-pdb.yml
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
grep -q "WebAction" docs/rbac_policy.md
grep -q "health-probe" docs/packaging_readiness.yml
grep -q "startup_check: required" docs/repair_automation_contract.yml
grep -q "timestamp-targeted-restore-dry-run" docs/pitr_archive_contract.yml
grep -q "backup-restore-e2e-smoke" docs/backup_consistency_contract.yml
grep -q "provider: s3-compatible" docs/object_storage_archive.yml
grep -q "restore_apply_with_confirmation" docs/object_storage_e2e.yml
grep -q "kubernetes-manifest-smoke" docs/packaging_readiness.yml
grep -q "kind: StatefulSet" packaging/kubernetes/neo4r-statefulset.yml
grep -q "kind: PodDisruptionBudget" packaging/kubernetes/neo4r-pdb.yml
grep -q "scripts/raft-soak.sh" docs/raft_production_semantics.md
grep -q "neo4r_backup_restore_last_success_timestamp_seconds" docs/slo_dashboard_example.yml
grep -q "frequency: daily" docs/restore_drill_schedule.yml
grep -q "RESTORE_PITR" docs/pitr_restore_apply.md
grep -q "node_kill_restart" docs/chaos_longevity_plan.yml
grep -q "operator_memory_budget_bytes" docs/query_guardrails.yml
grep -q "sorted-runs-v1" docs/query_spill_plan.yml
grep -q "durable_progress_key" docs/schema_migration_contract.yml
grep -q '"processed_rows"' docs/schema_migration_progress_example.json
grep -q "all_voters_match_index_above_delete_index" docs/safe_watermark_gc.yml
grep -q "GC_DELETE" docs/gc_executor_contract.yml
grep -q "Neo4r Production Overview" docs/grafana_dashboard.json
grep -q "kind: PrometheusRule" packaging/grafana-dashboard.yml
grep -q "native_protocol_version" docs/release_compatibility_matrix.yml
grep -q "NEO4R_PREVIOUS_RELEASE_DIR" docs/previous_release_fixture.yml
grep -q "Transfer shard leadership" docs/tls_rotation_runbook.md
grep -q "neo4r_tls_cert_not_after_seconds" docs/tls_cert_inventory.yml
grep -q "workflow_dispatch" .github/workflows/production-live.yml

echo "neo4r production artifact checks passed"
