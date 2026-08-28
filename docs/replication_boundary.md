# Replication Boundary

Neo4r has two replication paths during the transition to full Raft:

- Raft transport path: enabled when a routing table is configured and
  `DatabaseConfig::with_raft_enabled(true)` is used. This path persists term and
  vote, handles `RequestVote`, uses `AppendEntries` consistency checks, repairs
  divergent suffixes, and advances commit through the Raft group state.
- Legacy static replication path: used by `InProcessShardReplicator` and the
  non-Raft TCP replication sender. This path is still useful for local tests and
  simple bootstrap smoke checks, but it must not be described as consensus.

Membership-changing operations must be represented as committed config-change
commands before they affect shard authority. Direct local rewrites are allowed
only for bootstrap defaults and explicitly labeled legacy compatibility paths.

Operational checks:

```text
RAFT_STATUS
CLUSTER_STATUS
METADATA_LOG
```

When a bug report involves write visibility or primary authority, record whether
the database was opened with Raft enabled and include the current routing table
version.
