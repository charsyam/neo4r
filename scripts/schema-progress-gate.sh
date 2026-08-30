#!/usr/bin/env bash
set -euo pipefail

grep -q '"migration_id"' docs/schema_migration_progress_example.json
grep -q '"processed_rows"' docs/schema_migration_progress_example.json
grep -q "durable_progress_key" docs/schema_migration_contract.yml

cargo test -p neo4r-server schema_migration_progress_store_survives_reopen --quiet
echo "neo4r schema progress artifact gate passed"
