# Raft Production Semantics

Neo4r's Raft path now persists term/voted-for, performs RequestVote and
AppendEntries consistency checks, repairs divergent suffixes, advances commit
after quorum match indexes, supports read-index and leader-lease reads, and
applies committed routing-table install commands into local cluster metadata.
PreVote checks, leader-transfer preconditions, and snapshot chunk boundaries are
implemented in RaftCore and covered by focused unit tests.

## Lease Reads

Leader lease reads expose remaining lease time through:

- `RAFT_STATUS`
- `/api/admin/raft-status`
- `neo4r_raft_shard_leader_lease_remaining_ms`

Lease reads are enabled only when the configured lease duration is greater than
the configured clock plus message-delay bound. Operators still need to set those
bounds from measured host and network behavior; otherwise use read-index reads
for strong semantics.

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

## Soak Contract

The production soak gate covers repeated conflict repair, snapshot fallback,
election, catch-up after peer reload, and optional live process restart smoke.
The local CI entry point is:

```bash
scripts/raft-soak.sh
```

Set `NEO4R_RUN_RAFT_SOAK_LIVE=1` on a live topology runner to include process
restart coverage.
## Quorum Semi-Sync Safety

Neo4r uses quorum semi-sync semantics for Raft-backed shard writes. A leader may
append a client write locally, but the write is not committed and must not be
applied as durable graph state until a current-term voter quorum has durably
accepted the log entry.

This keeps the split-brain case safe:

- an old leader in a minority partition can append an uncommitted suffix locally,
  but cannot advance `commit_index`.
- read-index and leader-lease reads require an explicitly matched voter quorum;
  missing follower match indexes are not treated as implicit matches at
  `commit_index=0`.
- after a new leader is elected, it can overwrite only the old leader's
  uncommitted suffix.
- committed entries are fenced: followers reject attempts to overwrite or
  truncate entries at or below their committed index.

Committed data must never roll back. Only uncommitted Raft log suffixes are
repairable.
