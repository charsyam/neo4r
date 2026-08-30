#!/usr/bin/env bash
set -euo pipefail

grep -q "native_query_p99_ms" docs/observability_slo.yml
grep -q "Neo4rRaftNoLeader" docs/prometheus_alerts.yml
grep -q "neo4r_raft_group_leaders" docs/observability_slo.yml
cargo test -p neo4r-server web_console_serves_index_and_graph_api --quiet
echo "neo4r observability SLO gate passed"
