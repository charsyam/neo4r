#!/usr/bin/env bash
set -euo pipefail

grep -q "manifest-checksum-verifies" docs/backup_consistency_contract.yml
grep -q "backup-restore-e2e-smoke" docs/backup_consistency_contract.yml
grep -q "checksum=" crates/neo4r-server/src/backend/backend_web_query_backup.rs
cargo test -p neo4r-server restore_maintenance_drains_native_backend_writes --quiet
cargo test -p neo4r-server web_console_serves_index_and_graph_api --quiet
cargo test -p neo4r-db pending_restore_manifest_recovers_snapshot_replacement_on_reopen --quiet
echo "neo4r backup consistency gate passed"
