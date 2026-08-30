#!/usr/bin/env bash
set -euo pipefail

grep -q "sorted-runs-v1" docs/query_spill_plan.yml
grep -q "hash-partitions-v1" docs/query_spill_plan.yml
grep -q "remove_orphaned_spill_dirs" docs/query_spill_plan.yml

scripts/query-guardrail-gate.sh
echo "neo4r query spill gate passed"
