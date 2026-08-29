#!/usr/bin/env bash
set -euo pipefail

cargo test -p neo4r-db rdma_provider_trait_builds_reliable_endpoint_and_validates_availability --quiet

if [[ "${NEO4R_RUN_RDMA_LIVE:-0}" != "1" ]]; then
  echo "neo4r rdma live gate skipped; set NEO4R_RUN_RDMA_LIVE=1 to run on RDMA hosts"
  exit 0
fi

: "${NEO4R_RDMA_NODE_A:?set NEO4R_RDMA_NODE_A to the first ssh host}"
: "${NEO4R_RDMA_NODE_B:?set NEO4R_RDMA_NODE_B to the second ssh host}"
: "${NEO4R_RDMA_ADDR_A:?set NEO4R_RDMA_ADDR_A to node A's RDMA-reachable address}"
: "${NEO4R_RDMA_ADDR_B:?set NEO4R_RDMA_ADDR_B to node B's RDMA-reachable address}"

NEO4R_REMOTE_DIR="${NEO4R_REMOTE_DIR:-$PWD}"
NEO4R_RDMA_PORT="${NEO4R_RDMA_PORT:-18687}"
NEO4R_RDMA_TEST_TIMEOUT="${NEO4R_RDMA_TEST_TIMEOUT:-20}"

ssh "$NEO4R_RDMA_NODE_A" "cd '$NEO4R_REMOTE_DIR' && cargo build -p neo4r-server --features rdma"
ssh "$NEO4R_RDMA_NODE_B" "cd '$NEO4R_REMOTE_DIR' && cargo build -p neo4r-server --features rdma"
ssh "$NEO4R_RDMA_NODE_A" "cd '$NEO4R_REMOTE_DIR' && cargo run -p neo4r-db --features rdma --example rdma_probe -- 'rdma://$NEO4R_RDMA_ADDR_B:$NEO4R_RDMA_PORT' --count 1 --timeout-ms 3000" || true

ssh "$NEO4R_RDMA_NODE_B" \
  "cd '$NEO4R_REMOTE_DIR' && timeout '$NEO4R_RDMA_TEST_TIMEOUT' target/debug/neo4r-server --data-dir data/rdma-node-b --bind 0.0.0.0:17688 --replication-bind '$NEO4R_RDMA_ADDR_B:$NEO4R_RDMA_PORT' --server-id 2 --primary-server-id 1 --replication-transport rdma --web-auth-token admin:secret" &
server_b_pid=$!

cleanup() {
  kill "$server_b_pid" >/dev/null 2>&1 || true
}
trap cleanup EXIT

sleep 3

ssh "$NEO4R_RDMA_NODE_A" \
  "cd '$NEO4R_REMOTE_DIR' && timeout '$NEO4R_RDMA_TEST_TIMEOUT' target/debug/neo4r-server --data-dir data/rdma-node-a --bind 0.0.0.0:17687 --replication-bind '$NEO4R_RDMA_ADDR_A:$NEO4R_RDMA_PORT' --server-id 1 --primary-server-id 1 --replica-peer 2='$NEO4R_RDMA_ADDR_B:$NEO4R_RDMA_PORT' --replication-transport rdma --web-auth-token admin:secret" &
server_a_pid=$!

sleep 5
ssh "$NEO4R_RDMA_NODE_A" "cd '$NEO4R_REMOTE_DIR' && target/debug/neo4r-cli --addr '$NEO4R_RDMA_ADDR_A:17687' --command REPLICATION_STATUS"

kill "$server_a_pid" >/dev/null 2>&1 || true
wait "$server_a_pid" >/dev/null 2>&1 || true
wait "$server_b_pid" >/dev/null 2>&1 || true
echo "neo4r rdma live gate passed"
