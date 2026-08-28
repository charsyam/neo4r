#!/usr/bin/env bash
set -euo pipefail

cargo test -p neo4r-db query_plan_reports_read_access_path --quiet
cargo test -p neo4r-db query_route_reports_remote_shards --quiet
cargo test -p neo4r-server web_console_serves_index_and_graph_api --quiet

echo "query plan golden checks passed"
