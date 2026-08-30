#!/usr/bin/env bash
set -euo pipefail

cargo test -p neo4r-db tcp_install_snapshot_updates_replica_snapshot_payload
cargo test -p neo4r-db tcp_raft_append_falls_back_to_install_snapshot_on_rejection
cargo test -p neo4r-db tcp_snapshot_fetch_serves_primary_snapshot_for_node_catch_up
cargo test -p neo4r-server cluster_control_plane

if [[ "${NEO4R_RUN_MULTI_NODE:-0}" != "1" ]]; then
  printf 'multi-node live harness skipped; set NEO4R_RUN_MULTI_NODE=1 to launch local servers\n'
  exit 0
fi

tmpdir="$(mktemp -d)"
pids=()
cleanup() {
  for pid in "${pids[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  rm -rf "$tmpdir"
}
trap cleanup EXIT

for node in 1 2 3; do
  port=$((17686 + node))
  web_port=$((18080 + node))
  cargo run -p neo4r-server -- \
    --data-dir "$tmpdir/node-$node" \
    --server-id "$node" \
    --listen "127.0.0.1:$port" \
    --web-listen "127.0.0.1:$web_port" \
    >"$tmpdir/node-$node.log" 2>&1 &
  pids+=("$!")
done

sleep "${NEO4R_MULTI_NODE_BOOT_SECONDS:-3}"
PYTHONPATH=sdks/python python3 sdks/python/examples/basic_usage.py
printf 'multi-node live harness completed\n'
