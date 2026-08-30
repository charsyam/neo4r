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
