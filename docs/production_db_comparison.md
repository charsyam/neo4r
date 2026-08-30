# Neo4r Production DB Comparison

Neo4r is currently closer to a feature-rich distributed graph database prototype
than to a production database. It has real persistence, native/client protocols,
tenant metadata, auth tokens, Raft-oriented replication code, RDMA transport
plumbing, web admin APIs, and correctness tests. The remaining gap is mostly in
operational hardening, failure semantics, and long-running compatibility.

## Maturity Scorecard

| Area | Current Level | Production Expectation |
| --- | --- | --- |
| Storage durability | Medium | Crash-point verified atomic apply, online repair, corruption handling |
| Query correctness | Medium | Broad language coverage, planner stability, regression corpus |
| Transactions | Medium-low | Isolation contract, deadlock handling, recovery and observability |
| Replication | Medium-low | Proven Raft behavior under partitions, lag, replay, and membership churn |
| RDMA transport | Low-medium | Real transport path exists, but needs sustained live/chaos testing |
| Multi-tenancy | Medium | Per-DB lifecycle, auth scope, backup/restore boundaries are present |
| Security | Medium | Token expiry/revoke and native/replication/web TLS exist; needs deeper RBAC hardening |
| Observability | Medium | Metrics and alert rules exist; needs SLO dashboards and log correlation |
| Backup/restore | Medium | API, checksum manifest, and PITR archive gates exist; needs continuous archive replay |
| Operations | Medium-high | Config, preflight, release gates, systemd, logrotate, and runbook exist |

## Practical Status

Neo4r is suitable for:

- Development and architectural experiments.
- Single-node graph storage/query correctness validation.
- Controlled multi-node replication experiments.
- RDMA transport prototyping on known hardware.

Neo4r is not yet suitable for:

- Unattended production workloads.
- Untrusted networks.
- Strong availability guarantees under partitions.
- Irreversible data without external backups and restore drills.

## Priority To Reach Production

1. Keep `--production-check` green for every deployable configuration.
2. Make CLI the primary operational surface for query, transaction, auth, and
   backup workflows.
3. Run live RDMA and TCP replication tests in CI/lab environments.
4. Add transport-specific observability.
5. Expand crash consistency tests around storage materialization.
6. Implement continuous WAL archive shipping and timestamp-targeted PITR replay.
7. Run opt-in live chaos/RDMA gates continuously in lab CI.

## Production Preflight

`neo4r-server --production-check` is the deployment gate. It intentionally
rejects settings that are useful for development but unsafe for production:

- loopback-only native or web binds
- relative, `/tmp`, `/var/tmp`, or default `data` directories
- missing/default/short web admin tokens
- async replication ACK policy
- replica reads without query peers
- clustered routing without replication bind, peers, startup catch-up, interval,
  and batch size controls
- missing TLS/mTLS boundary declaration
- unpinned native protocol compatibility window
- missing backup drill, audit retention, secret rotation, or tenant quota policy
- missing WAL archive, restore drill, upgrade manifest, alert rules, repair
  check, query corpus, chaos gate, runbook, systemd unit, or logrotate policy
