#!/usr/bin/env bash
set -euo pipefail

grep -q "startup_check: required" docs/repair_automation_contract.yml
grep -q "online-repair-smoke" docs/repair_automation_contract.yml
grep -q "neo4r_storage_repair_failures_total" crates/neo4r-server/src/backend/backend_web_query_backup.rs
grep -q "repair_check_on_startup: true" packaging/server.production.yml
scripts/storage-atomicity.sh
cargo test -p neo4r-server --quiet web_console_serves_index_and_graph_api
echo "neo4r repair automation gate passed"
