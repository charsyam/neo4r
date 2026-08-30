#!/usr/bin/env bash
set -euo pipefail

grep -q "durable_progress_key" docs/schema_migration_contract.yml
grep -q "retrying" docs/schema_migration_contract.yml

cargo test -p neo4r-server native_command_rebuilds_vector_indexes --quiet
cargo test -p neo4r-server native_vector_index_status_survives_reopen --quiet
cargo test -p neo4r-server native_command_syncs_index_catalog_from_peer --quiet

echo "neo4r schema migration gate passed"
