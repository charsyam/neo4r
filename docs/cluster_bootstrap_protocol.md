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
5. The joining node installs the snapshot when required, replays WAL entries
   through the target index, then reports the observed match index.
6. Once every assigned shard has `match_index >= target_index`, the plan is
   `ready_to_promote` and the membership change can be finalized through the
   routing table/Raft configuration path.

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
