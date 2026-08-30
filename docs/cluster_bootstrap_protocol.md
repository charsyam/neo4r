# Cluster Bootstrap Protocol

## Join Existing Cluster

1. A new node sends a join request with `server_id`, address, protocol version,
   storage version, and shard count.
2. The metadata authority records the node as `negotiating` or `rejected`.
3. Accepted nodes become `joining`. Rebalance planning creates `AddReplica`
   steps and records per-shard assignments as `catching_up`.
4. `plan_node_catch_up(server_id)` returns a durable execution contract:
   shard id, primary server id/address, whether a snapshot is required, replay
   start index, target commit index, and current match index.
5. `execute_node_catch_up_plan` installs the snapshot when required, replays WAL
   entries through the target index, and returns per-shard match indexes.
6. The authority records the reported match indexes with `mark_shard_caught_up`.
7. Once every assigned shard has `match_index >= target_index`,
   `promote_caught_up_node_to_voter` applies the routing/Raft membership change
   and moves the assignment from learner catch-up to serving replica.

The executor accepts a `NodeCatchUpDataSource` so TCP, UDP, RDMA, or fixture
sources use the same state-machine apply path.

The native protocol exposes the same flow through:

- `CATCH_UP_PLAN_PRIMARY <server_id>` to inspect per-shard snapshot/WAL targets.
- `PROMOTE_CAUGHT_UP_NODE <server_id>` to promote a fully caught-up learner.
- `TOPOLOGY_OBSERVE` to ask the controller for the next control-loop action.
- `TOPOLOGY_RECONCILE [max_entries_per_request]` to run one controller step:
  catch up from primaries when assignments are catching up, otherwise advance
  rebalance when membership is ready to move.
- `CHAOS_CHECKS` to expose join, retry, and promotion invariants for smoke
  gates.

`neo4r-cli cluster topology`, `neo4r-cli cluster reconcile [limit]`,
`neo4r-cli cluster chaos`, and `neo4r-cli cluster promote <server_id>` wrap
these commands for operators. The TCP catch-up data source can fetch primary
snapshots and request bounded WAL tails through the same replication listener.
Snapshot fetch protocol v2 returns chunks with `offset`, `resume_offset`,
`total_len`, and checksum metadata so interrupted installs can resume without
discarding already transferred bytes.

## Recover From Data

Data-only cluster recovery must not infer safety from graph records alone. The
seed node writes `cluster/bootstrap-manifest.txt` with:

- cluster id and database id selected for the new cluster.
- seed server id, shard count, routing version, metadata term, and config epoch.
- every shard commit index.
- snapshot index, snapshot term, and snapshot payload checksum.
- membership and in-flight shard assignment metadata.

`RecoverFromData` manifests always set `force_new_cluster_required=true`. A
node must validate the manifest against local commit and snapshot metadata
before using the data directory as a new seed. Operators should only use this
mode with an explicit force-new-cluster workflow, because it rewrites cluster
identity and must not race with a still-live old cluster.

## Safety And Operations

- `bootstrap_safety_decision` blocks recover-from-data unless the expected
  cluster id matches and the caller supplies a force-new-cluster confirmation.
- `operational_safety_decision` returns a confirmation token derived from the
  operation, config epoch, and routing version for destructive operations.
- `backup_bootstrap_link` ties a backup manifest to a bootstrap manifest before
  a backup can seed a new cluster.
- `topology_observation` drives the control loop: `execute_catch_up`,
  `advance_rebalance`, or `idle`.
- `chaos_checks_for_join_catch_up` exposes invariants for join, snapshot retry,
  and rebalance control-loop smoke gates.
- `SnapshotChunkAssembler::resume_token` reports the next byte offset and
  snapshot boundary for interrupted snapshot streaming.
- `WRITE_BOOTSTRAP_MANIFEST <mode> <cluster_id> <database_id>` writes the
  recovery manifest through the DB API.
- `BOOTSTRAP_SAFETY <expected_cluster_id> <force_new_cluster>` verifies whether
  a data-only recovery can proceed.
- `OPERATIONAL_SAFETY <operation> [confirmation_token]` returns or verifies the
  confirmation token for destructive operations.
