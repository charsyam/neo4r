#!/usr/bin/env bash
set -euo pipefail

grep -q "all_voters_match_index_above_delete_index" docs/safe_watermark_gc.yml
grep -q "no_pending_restore_manifest" docs/safe_watermark_gc.yml

scripts/wal-compaction.sh
cargo test -p neo4r-db raft_append_truncates_divergent_segmented_wal_suffix --quiet

echo "neo4r safe watermark gc gate passed"
