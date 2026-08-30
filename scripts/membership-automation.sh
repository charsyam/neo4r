#!/usr/bin/env bash
set -euo pipefail

cargo test -p neo4r-db cluster_rebalance_execution_advances_and_persists_status --quiet
cargo test -p neo4r-db cluster_rebalance_reports_snapshot_bootstrap_before_catch_up --quiet
cargo test -p neo4r-db cluster_join_catch_up_plan_requires_snapshot_then_wal_tail --quiet
cargo test -p neo4r-db cluster_bootstrap_manifest_persists_recover_from_data_boundary --quiet
cargo test -p neo4r-db catch_up_executor_replays_plan_and_promotes_caught_up_node --quiet
cargo test -p neo4r-db learner_does_not_vote_until_promoted --quiet
cargo test -p neo4r-db bootstrap_safety_topology_backup_and_chaos_contracts_are_enforced --quiet
cargo test -p neo4r-db snapshot_chunk_resume_token_reports_next_offset --quiet
cargo test -p neo4r-db tcp_snapshot_fetch_serves_primary_snapshot_for_node_catch_up --quiet
cargo test -p neo4r-db cluster_membership_decommission_plans_primary_transfer_and_replica_removal --quiet
cargo test -p neo4r-server backend_advance_rebalance_runs_auto_pump_for_snapshot_bootstrap --quiet
cargo test -p neo4r-server topology_reconcile_advances_joining_node_control_loop --quiet
cargo test -p neo4r-server cluster_bootstrap_and_topology_protocol_commands_execute --quiet
cargo test -p neo4r-server gossip_node_materializes_query_address_book_without_replication_endpoint --quiet
cargo test -p neo4r-server gossip_refresh_from_membership_seeds_address_books --quiet

echo "neo4r membership automation checks passed"
