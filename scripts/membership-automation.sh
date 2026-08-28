#!/usr/bin/env bash
set -euo pipefail

cargo test -p neo4r-db cluster_rebalance_execution_advances_and_persists_status --quiet
cargo test -p neo4r-db cluster_rebalance_reports_snapshot_bootstrap_before_catch_up --quiet
cargo test -p neo4r-db cluster_membership_decommission_plans_primary_transfer_and_replica_removal --quiet
cargo test -p neo4r-server backend_advance_rebalance_runs_auto_pump_for_snapshot_bootstrap --quiet

echo "neo4r membership automation checks passed"
