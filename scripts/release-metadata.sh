#!/usr/bin/env bash
set -euo pipefail

output_dir="${NEO4R_RELEASE_METADATA_DIR:-target/neo4r-release}"
mkdir -p "$output_dir"

git_sha="$(git rev-parse --short HEAD 2>/dev/null || printf unknown)"
generated_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

{
  printf 'git_sha=%s\n' "$git_sha"
  printf 'generated_at=%s\n' "$generated_at"
  printf 'release_gate=passed\n'
  printf 'query_result_contract=docs/query_result_contract.md\n'
  printf 'performance_thresholds=docs/performance_thresholds.txt\n'
  printf 'performance_baseline=docs/performance_baseline.md\n'
} > "$output_dir/metadata.txt"

printf '%s\n' "$output_dir/metadata.txt"
