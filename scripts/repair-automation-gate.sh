#!/usr/bin/env bash
set -euo pipefail

grep -q "startup_check: required" docs/repair_automation_contract.yml
grep -q "repair_check_on_startup: true" packaging/server.production.yml
scripts/storage-atomicity.sh
cargo test -p neo4r-server --quiet web_console_serves_index_and_graph_api
echo "neo4r repair automation gate passed"
