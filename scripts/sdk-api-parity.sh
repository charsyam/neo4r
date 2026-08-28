#!/usr/bin/env bash
set -euo pipefail

rust_client="crates/neo4r-client/src/lib.rs"
python_client="sdks/python/neo4r_client/client.py"

required_methods=(
  ping
  close
  query
  execute
  command
  profile
  query_plan
  statistics
  storage_status
  metadata_log
  cluster_status
  cluster_management_status
)

for method in "${required_methods[@]}"; do
  if ! rg -q "pub fn ${method}\\(" "$rust_client"; then
    printf 'missing Rust SDK method: %s\n' "$method" >&2
    exit 1
  fi
  if ! rg -q "def ${method}\\(" "$python_client"; then
    printf 'missing Python SDK method: %s\n' "$method" >&2
    exit 1
  fi
done

printf 'sdk api parity checks passed\n'
