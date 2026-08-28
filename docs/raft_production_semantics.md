# Raft Production Semantics

Neo4r's Raft path now persists term/voted-for, performs RequestVote and
AppendEntries consistency checks, repairs divergent suffixes, advances commit
after quorum match indexes, supports read-index and leader-lease reads, and
applies committed routing-table install commands into local cluster metadata.

## Lease Reads

Leader lease reads expose remaining lease time through:

- `RAFT_STATUS`
- `/api/admin/raft-status`
- `neo4r_raft_shard_leader_lease_remaining_ms`

The remaining production work is validating clock-bound assumptions across
hosts. Until that is complete, operators should prefer read-index reads when
machine clocks or scheduling latency are not tightly controlled.

## Config Changes

Committed `ClusterConfigChange` commands with phase `install` update durable
routing metadata, replicator routing, cluster config epoch, and local Raft group
membership reconstruction. Joint-consensus enter/finalize commands are still
kept as explicit transition phases so operators can inspect the change path.

## Release Gate

Run:

```bash
scripts/membership-automation.sh
scripts/multi-node-live-gate.sh
```
