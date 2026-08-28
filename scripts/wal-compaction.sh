#!/usr/bin/env bash
set -euo pipefail

cargo test -p neo4r-storage segmented_shard_log_reports_compaction_candidates_before_retained_index --quiet
cargo test -p neo4r-db raft_snapshot_now_generates_payload_and_compacts_local_raft_log --quiet

echo "neo4r wal compaction checks passed"
