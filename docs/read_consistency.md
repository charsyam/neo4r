# Neo4r Read Consistency

Neo4r exposes explicit read consistency levels so clients can choose between
latency and freshness.

## Levels

- `local`: read from the selected local database handle without quorum
  confirmation. This is fastest and can be stale on replicas.
- `leader_lease`: read locally only when the Raft leader lease is still valid.
  If the lease is not valid, the read fails instead of silently returning stale
  data.
- `read_index`: ask the Raft group to confirm the leader and committed index
  before serving the read. This is the default strong-read path for replicated
  deployments.
- `follower_stale`: allow a follower to serve from its current applied state.
  This is useful for low-cost dashboards and diagnostics that tolerate lag.

## Contract

Writes become visible after the Raft log entry reaches the committed index and
the graph state applies that entry. A strong read must observe at least the
confirmed committed index. A stale follower read may observe an older applied
index, and the response metadata reports the index used by the read path.

Read-write transactions use read-your-writes behavior for staged writes before
commit. After commit, clients that need monotonic visibility should use
`read_index` or route reads to the leader while the lease is valid.

## Operations

Use the line protocol and HTTP metrics to inspect the state involved in the
contract:

```text
RAFT_STATUS
STORAGE_STATUS
STATISTICS
```

For maintenance and post-restore validation, run:

```text
VERIFY_INVARIANTS
REPAIR_INVARIANTS
```

For query APIs, `ReadConsistency::ReadIndex` is the strong default in Raft
mode. `ReadConsistency::FollowerStale` is the explicit low-latency diagnostic
escape hatch and should not be used for read-your-writes user flows.
Stale-read responses must include applied/committed index metadata so callers
can measure how far behind the serving replica is.

The web equivalents are:

```bash
curl -X POST -H 'authorization: Bearer admin:secret' \
  http://127.0.0.1:18080/api/admin/verify-invariants

curl -X POST -H 'authorization: Bearer admin:secret' \
  http://127.0.0.1:18080/api/admin/repair-invariants
```
