#!/usr/bin/env bash
set -euo pipefail

grep -q "sorted-runs-v1" docs/query_spill_plan.yml
grep -q "hash-partitions-v1" docs/query_spill_plan.yml
grep -q "remove_orphaned_spill_dirs" docs/query_spill_plan.yml

cargo test -p neo4r-query spill_manifest_is_written_when_operator_budget_is_exceeded --quiet
cargo test -p neo4r-query spill_is_skipped_when_rows_fit_budget --quiet
scripts/query-guardrail-gate.sh
echo "neo4r query spill gate passed"
