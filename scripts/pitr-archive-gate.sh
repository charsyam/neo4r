#!/usr/bin/env bash
set -euo pipefail

grep -q "segmented-shard-wal" docs/pitr_archive_contract.yml
grep -q "wal_archive_dir:" packaging/server.production.yml
grep -q "restore_drill_manifest:" packaging/server.production.yml
cargo test -p neo4r-db reopens_and_replays_segmented_logs --quiet
cargo test -p neo4r-db uncommitted_wal_entry_is_not_replayed_after_reopen --quiet
echo "neo4r PITR archive gate passed"
