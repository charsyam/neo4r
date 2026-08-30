#!/usr/bin/env bash
set -euo pipefail

threshold_file="${NEO4R_BENCH_THRESHOLDS:-docs/performance_thresholds.txt}"

if [[ ! -f "$threshold_file" ]]; then
  echo "missing benchmark threshold file: $threshold_file" >&2
  exit 1
fi

while IFS='=' read -r key value; do
  [[ -z "${key// }" || "$key" =~ ^# ]] && continue
  if [[ -z "${value:-}" ]]; then
    echo "invalid threshold without value: $key" >&2
    exit 1
  fi
  if ! [[ "$value" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    echo "threshold $key must be numeric, got $value" >&2
    exit 1
  fi
done < "$threshold_file"

cargo test -p neo4r-server perf_baseline_rejects_regressions --quiet
echo "benchmark thresholds validated: $threshold_file"
