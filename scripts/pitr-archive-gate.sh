#!/usr/bin/env bash
set -euo pipefail

grep -q "segmented-shard-wal" docs/pitr_archive_contract.yml
grep -q "timestamp-targeted-restore-dry-run" docs/pitr_archive_contract.yml
grep -q "wal_archive_dir:" packaging/server.production.yml
grep -q "restore_drill_manifest:" packaging/server.production.yml
cargo test -p neo4r-db reopens_and_replays_segmented_logs --quiet
cargo test -p neo4r-db uncommitted_wal_entry_is_not_replayed_after_reopen --quiet
cargo test -p neo4r-db local_write_entries_include_origin_and_config_metadata --quiet
cargo test -p neo4r-db pitr_timestamp_target_selects_committed_entries_at_or_before_target --quiet
cargo test -p neo4r-server pitr_restore_plan_requires_admin_and_reports_target_indexes --quiet
cargo test -p neo4r-client parses_cli_subcommands --quiet
echo "neo4r PITR archive gate passed"
