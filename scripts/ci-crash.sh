#!/usr/bin/env bash
set -euo pipefail

cargo test -p neo4r-storage graph_store::tests::failed_relationship_write_batch_leaves_no_partial_indexes
cargo test -p neo4r-storage graph_store::tests::relationship_create_uses_one_atomic_write_batch
cargo test -p neo4r-db failure_injection_after_commit_before_apply_recovers_on_reopen
cargo test -p neo4r-db pending_restore_manifest_recovers_snapshot_replacement_on_reopen
cargo test -p neo4r-db raft_append_truncates_divergent_segmented_wal_suffix
