#!/usr/bin/env bash
set -euo pipefail

grep -q "neo4r-cli cluster reconcile" docs/production_runbook.md
grep -q "neo4r-cli cluster bootstrap-manifest" docs/production_runbook.md
grep -q "neo4r-cli cluster bootstrap-safety" docs/production_runbook.md
grep -q "neo4r-cli cluster safety restore_pitr" docs/production_runbook.md
grep -q "scripts/pitr-restore-worker-gate.sh" docs/production_runbook.md
grep -q "neo4r_slo_query_error_rate_ppm" docs/production_runbook.md

cargo test -p neo4r-client parses_cli_subcommands --quiet
cargo test -p neo4r-server topology_reconcile_advances_joining_node_control_loop --quiet

echo "neo4r runbook drill gate passed"
