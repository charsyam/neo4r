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

Gossip discovery is a pre-Raft address and liveness layer, not an authority
layer. `GOSSIP_NODE` records may populate query and replication address books so
owner errors can always carry a target address, but they do not install shard
membership, do not make a node a voter, and do not bypass replication endpoint
negotiation. Raft metadata remains the source of truth for which server owns a
shard and whether a joining node can vote.

Operational checks:

```text
RAFT_STATUS
CLUSTER_STATUS
METADATA_LOG
LIST_GOSSIP_NODES
```

When a bug report involves write visibility or primary authority, record whether
the database was opened with Raft enabled and include the current routing table
version.

## Endpoint Identity

Replication peers are negotiated as endpoints, not bare socket addresses. The
native command keeps the legacy form and also accepts endpoint identity fields:

```text
REGISTER_REPLICATION_PEER server_id address
REGISTER_REPLICATION_PEER server_id address node_id transport
NEGOTIATE_REPLICATION_PEER server_id address
NEGOTIATE_REPLICATION_PEER server_id address node_id
```

`node_id` identifies the remote node that owns the endpoint. It lets the receiver
reject self-loop registrations before any replication request is sent, including
cases where a different `server_id` is accidentally mapped to the local node.
`transport` is one of `tcp`, `udp`, `rdma`, or `custom`; only `tcp` is currently
usable for raft delivery. UDP is a negotiated prototype boundary and RDMA/custom
require provider implementations.

`NEGOTIATE_REPLICATION_PEER` opens the TCP replication endpoint first, reads the
remote hello identity, verifies server id, optional node id, cluster id,
database id, and routing-table membership, then persists the accepted identity.
Persisted identities are stored separately from the legacy address list so a
restart can rebuild typed endpoints and reject indirect identity cycles.

The endpoint identity check is intentionally local and conservative. It prevents
direct and persisted indirect cycles at registration time; richer topology
validation still belongs in committed cluster membership changes.
