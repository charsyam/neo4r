#!/usr/bin/env bash
set -euo pipefail

grep -q "restore-pitr/pending" docs/pitr_restore_apply.md
grep -q "restore-pitr/complete" docs/pitr_restore_apply.md
grep -q "pitr_restore_pending_json" crates/neo4r-server/src/backend/pitr.rs
grep -q "pitr_restore_complete_json" crates/neo4r-server/src/backend/pitr.rs
grep -q "seed-new-cluster-from-restored-data" docs/pitr_restore_drill.yml

cargo test -p neo4r-db pitr_restore_to_timestamp_rewrites_state_and_truncates_wal_suffix --quiet
cargo test -p neo4r-server pitr_restore_apply_requires_confirmation_and_writes_manifest --quiet
cargo test -p neo4r-server restore_drill_manifest_requires_seed_and_query_checks --quiet

echo "neo4r PITR restore worker gate passed"
