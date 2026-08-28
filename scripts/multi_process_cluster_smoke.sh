#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/target/debug/neo4r-server"
BASE="${NEO4R_CLUSTER_SMOKE_DIR:-$ROOT/target/neo4r-cluster-smoke}"

cargo build -p neo4r-server
rm -rf "$BASE"
mkdir -p "$BASE"

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT

"$BIN" --bind 127.0.0.1:17687 --replication-bind 127.0.0.1:18687 --data-dir "$BASE/node1" --server-id 1 --primary-server-id 1 --shards 1 --partitions 1 --web-auth-token admin:secret &
pids+=("$!")
"$BIN" --bind 127.0.0.1:17688 --replication-bind 127.0.0.1:18688 --data-dir "$BASE/node2" --server-id 2 --primary-server-id 1 --shards 1 --partitions 1 --web-auth-token admin:secret &
pids+=("$!")
"$BIN" --bind 127.0.0.1:17689 --replication-bind 127.0.0.1:18689 --data-dir "$BASE/node3" --server-id 3 --primary-server-id 1 --shards 1 --partitions 1 --web-auth-token admin:secret &
pids+=("$!")

sleep 1

printf 'REGISTER_REPLICATION_PEER\t2\t127.0.0.1:18688\nRAFT_STATUS\nQUIT\n' | nc 127.0.0.1 17687
printf 'QUERY\tCREATE (n:Person {name: "ClusterSmoke"}) RETURN n\nSNAPSHOT_NOW\nRAFT_STATUS\nQUIT\n' | nc 127.0.0.1 17687

if [[ "${NEO4R_RUN_CLUSTER_RESTART:-0}" == "1" ]]; then
  kill "${pids[1]}" 2>/dev/null || true
  wait "${pids[1]}" 2>/dev/null || true
  "$BIN" --bind 127.0.0.1:17688 --replication-bind 127.0.0.1:18688 --data-dir "$BASE/node2" --server-id 2 --primary-server-id 1 --shards 1 --partitions 1 --web-auth-token admin:secret &
  pids[1]="$!"
  sleep 1
  printf 'CATCH_UP_FROM_PRIMARIES\nRAFT_STATUS\nQUIT\n' | nc 127.0.0.1 17688
  printf 'QUERY\tMATCH (n:Person) WHERE n.name = "ClusterSmoke" RETURN n.name\nQUIT\n' | nc 127.0.0.1 17688
fi

echo "multi-process cluster smoke completed under $BASE"
