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

## Bootstrap Ownership

Explicit routing table files remain the source of truth when supplied. Without
one, clustered bootstrap builds a deterministic server ring from
`--primary-server-id`, the local `--server-id`, `--replica-peer` entries, and
`--peer` entries. Shard primaries are assigned by walking that ring, and every
other ring member is listed as a replica for that shard. This makes ownership
range-based rather than node-role-based: a server can be primary for shard A and
a replica for shard B.

Shard 0 still starts at `--primary-server-id` so existing metadata bootstrap and
admin discovery have a stable authority. Operators must provide the same peer
membership on each node until committed membership changes install the next
routing table; gossip may discover addresses, but it does not change ownership
by itself.

## Effective Write Authority

The routing table records bootstrap placement and fallback ownership. When Raft
is enabled, the effective write authority for a shard is the current Raft leader
for that shard. If the routing-table primary fails and a replica wins an
election, `CLUSTER_STATUS` reports that elected leader as the shard primary and
local write guards accept writes on that node. If no Raft leader is known yet,
the server falls back to the routing-table primary for redirects and diagnostics.

Leader eligibility is also readiness-gated. A server can start an election or
accept local writes only when the routing table says it hosts the shard and its
materialized state has applied at least through the local committed index. A
joining or lagging replica must finish snapshot/WAL catch-up before it can enter
the routing table or become an effective write authority.

This keeps failover tied to Raft term and role state instead of requiring an
immediate routing-table rewrite for every leadership change. Durable membership
or replica-set changes still require committed config-change commands.

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
