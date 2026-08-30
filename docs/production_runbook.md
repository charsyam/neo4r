# Neo4r Production Runbook

## Required Before Deployment

- Run `scripts/release-gate.sh`.
- Run `scripts/production-preflight.sh packaging/server.production.yml`.
- Verify external or in-process TLS boundaries for native, web, and replication traffic.
- Verify WAL archive storage and restore drill manifest are reachable.
- Verify Prometheus alert rules are loaded.
- Verify every clustered node has `gossip.seed_peers`, `gossip.interval_ms`, and
  `gossip.auth_token` configured.

## Incident Checks

- Inspect `/api/metrics` for raft leader count, shard lag, auth failures, and HTTP errors.
- Inspect `/api/admin/gossip` and `/metrics` for live gossip nodes, expired
  records, fanout failures, and pending replication negotiation.
- Run the query regression corpus against a restored copy before destructive recovery.
- Prefer snapshot restore plus WAL archive replay once PITR replay is enabled.

## Drill Commands

- Cluster join drill: `neo4r-cli cluster topology`, then
  `neo4r-cli cluster reconcile 128`, then `neo4r-cli cluster chaos`.
- New node catch-up drill: gossip the node address, accept the join, inspect
  `CATCH_UP_PLAN_PRIMARY`, run `TOPOLOGY_RECONCILE`, verify snapshot plus WAL
  tail replay, then promote only after every assigned shard reports caught up.
- Gossip drill: `neo4r-cli cluster gossip-list`, add or restart a seed node,
  then confirm `neo4r_gossip_live_nodes` increases and expired records do not
  remove Raft membership.
- Replacement drill: start the replacement with the same Raft join flow, gossip
  its query/replication address first, run catch-up, then promote only after
  `neo4r_gossip_replication_negotiation_pending` reaches zero.
- Decommission drill: drain through committed membership changes first; gossip
  expiry is only a liveness signal and must not remove a voter by itself.
- Data-only recovery drill: `neo4r-cli cluster bootstrap-manifest recover_from_data <cluster-id> <database-id>`,
  then `neo4r-cli cluster bootstrap-safety <cluster-id> true`.
- Destructive operation confirmation drill: `neo4r-cli cluster safety restore_pitr`,
  then rerun with the returned confirmation token.
- Restore drill: run `scripts/pitr-restore-worker-gate.sh` and confirm
  `seed-new-cluster-from-restored-data` is present in the restore drill manifest.
- SLO drill: run `scripts/observability-slo-gate.sh` and confirm
  `neo4r_slo_query_error_rate_ppm` and `neo4r_slo_replication_lag_entries`
  are scraped.
