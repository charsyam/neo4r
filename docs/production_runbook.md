# Neo4r Production Runbook

## Required Before Deployment

- Run `scripts/release-gate.sh`.
- Run `scripts/production-preflight.sh packaging/server.production.yml`.
- Verify external or in-process TLS boundaries for native, web, and replication traffic.
- Verify WAL archive storage and restore drill manifest are reachable.
- Verify Prometheus alert rules are loaded.

## Incident Checks

- Inspect `/api/metrics` for raft leader count, shard lag, auth failures, and HTTP errors.
- Run the query regression corpus against a restored copy before destructive recovery.
- Prefer snapshot restore plus WAL archive replay once PITR replay is enabled.

## Drill Commands

- Cluster join drill: `neo4r-cli cluster topology`, then
  `neo4r-cli cluster reconcile 128`, then `neo4r-cli cluster chaos`.
- Data-only recovery drill: `neo4r-cli cluster bootstrap-manifest recover_from_data <cluster-id> <database-id>`,
  then `neo4r-cli cluster bootstrap-safety <cluster-id> true`.
- Destructive operation confirmation drill: `neo4r-cli cluster safety restore_pitr`,
  then rerun with the returned confirmation token.
- Restore drill: run `scripts/pitr-restore-worker-gate.sh` and confirm
  `seed-new-cluster-from-restored-data` is present in the restore drill manifest.
- SLO drill: run `scripts/observability-slo-gate.sh` and confirm
  `neo4r_slo_query_error_rate_ppm` and `neo4r_slo_replication_lag_entries`
  are scraped.
