#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/target/debug/neo4r-server"
BASE="${NEO4R_JEPSEN_LITE_DIR:-$ROOT/target/neo4r-jepsen-lite}"

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

"$BIN" --bind 127.0.0.1:17787 --replication-bind 127.0.0.1:18787 --data-dir "$BASE/node1" --server-id 1 --primary-server-id 1 --shards 1 --partitions 1 --web-auth-token admin:secret &
pids+=("$!")
"$BIN" --bind 127.0.0.1:17788 --replication-bind 127.0.0.1:18788 --data-dir "$BASE/node2" --server-id 2 --primary-server-id 1 --shards 1 --partitions 1 --web-auth-token admin:secret &
pids+=("$!")
"$BIN" --bind 127.0.0.1:17789 --replication-bind 127.0.0.1:18789 --data-dir "$BASE/node3" --server-id 3 --primary-server-id 1 --shards 1 --partitions 1 --web-auth-token admin:secret &
pids+=("$!")

sleep 1

PYTHONPATH="$ROOT/sdks/python" python3 - <<'PY'
from neo4r_client import Client

leader = Client.connect("127.0.0.1", 17787, retry_attempts=20, retry_backoff=0.1)
leader.ping()
assert leader.command("REGISTER_REPLICATION_PEER\t2\t127.0.0.1:18788") == "OK"
assert leader.command("REGISTER_REPLICATION_PEER\t3\t127.0.0.1:18789") == "OK"

for i in range(20):
    response = leader.command(f"CREATE_NODE\tJepsenLite\tid=i:{i}\tvalue=s:v{i}")
    assert response.startswith("OK\tNODE\t"), response

for i in range(5):
    assert leader.command(f"SET_NODE_PROPERTY\t{i}\tvalue\ts:updated-{i}") == "OK"

for i in range(5, 10):
    assert leader.command(f"DELETE_NODE\t{i}") == "OK"

assert len(leader.query("MATCH (n:JepsenLite) RETURN n.id")) == 15
assert len(leader.query('MATCH (n:JepsenLite) WHERE n.value = "updated-0" RETURN n.value')) == 1
assert len(leader.query("MATCH (n:JepsenLite) WHERE n.id = 7 RETURN n.id")) == 0
leader.close()
PY

kill "${pids[1]}" 2>/dev/null || true
wait "${pids[1]}" 2>/dev/null || true
"$BIN" --bind 127.0.0.1:17788 --replication-bind 127.0.0.1:18788 --data-dir "$BASE/node2" --server-id 2 --primary-server-id 1 --shards 1 --partitions 1 --web-auth-token admin:secret &
pids[1]="$!"
sleep 1

PYTHONPATH="$ROOT/sdks/python" python3 - <<'PY'
from neo4r_client import Client

follower = Client.connect("127.0.0.1", 17788, retry_attempts=20, retry_backoff=0.1)
follower.ping()
assert follower.command("REGISTER_REPLICATION_PEER\t1\t127.0.0.1:18787") == "OK"
catch_up = follower.command("CATCH_UP_FROM_PRIMARIES")
assert catch_up.startswith("OK\tCATCH_UP\t"), catch_up
follower.close()
PY

echo "jepsen-lite correctness completed under $BASE"
