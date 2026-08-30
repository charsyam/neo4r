#!/usr/bin/env bash
set -euo pipefail

grep -q "native_query_p99_ms" docs/observability_slo.yml
grep -q "Neo4rRaftNoLeader" docs/prometheus_alerts.yml
grep -q "neo4r_raft_group_leaders" docs/observability_slo.yml
grep -q "neo4r_query_plan_cost_model_version" crates/neo4r-server/src/backend/backend_web_query_backup.rs
grep -q "neo4r_backup_restore_last_success_timestamp_seconds" crates/neo4r-server/src/backend/backend_web_query_backup.rs
grep -q "neo4r_storage_repair_last_success_timestamp_seconds" crates/neo4r-server/src/backend/backend_web_query_backup.rs
cargo test -p neo4r-server web_console_serves_index_and_graph_api --quiet
cargo test -p neo4r-server slo_burn_rate_rejects_latency_and_lag_regressions --quiet
echo "neo4r observability SLO gate passed"
