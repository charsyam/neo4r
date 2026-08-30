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
  routing_table
  cluster_registry
  capabilities
  connect_to_cached_target
)

required_admin_methods=(
  create_database
  select_database
  list_databases
  invoke_token
  revoke_token
  grant_role
  revoke_role
  maintenance_mode
  restore_pitr_plan
  restore_pitr_apply
  query
  metrics
  routing_table
  cluster_registry
  capabilities
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

for method in "${required_admin_methods[@]}"; do
  if ! rg -q "pub fn ${method}\\(" "$rust_client"; then
    printf 'missing Rust SDK admin method: %s\n' "$method" >&2
    exit 1
  fi
  if ! rg -q "def ${method}\\(" sdks/python/neo4r_client/http_admin.py; then
    printf 'missing Python SDK admin method: %s\n' "$method" >&2
    exit 1
  fi
done

printf 'sdk api parity checks passed\n'
