#!/usr/bin/env bash
set -euo pipefail

grep -q "background_refresh:" docs/query_statistics_maintenance.yml
grep -q "histogram-contract-present" docs/query_statistics_maintenance.yml
cargo test -p neo4r-db engine_hardening_persists_statistics_and_metadata_log_across_reopen --quiet
cargo test -p neo4r-db performance_profile_statistics_storage_and_read_cache_are_reported --quiet
echo "neo4r query statistics maintenance gate passed"
