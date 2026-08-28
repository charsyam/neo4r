#!/usr/bin/env bash
set -euo pipefail

cargo test -p neo4r-db append_entries_reports_conflict_term_and_first_index
cargo test -p neo4r-db raft_append_truncates_divergent_segmented_wal_suffix
cargo test -p neo4r-db tcp_raft_append_falls_back_to_install_snapshot_on_rejection
cargo test -p neo4r-db --test real_crash_harness real_crash_harness_reopens_child_written_data_after_kill
cargo test -p neo4r-db --test real_crash_harness real_crash_harness_reopens_relationship_and_adjacency_after_kill
