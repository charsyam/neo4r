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
| Security | Medium-low | Token expiry/revoke exists; needs TLS, audit retention, secret rotation |
| Observability | Medium-low | Metrics exist; needs transport labels, SLO dashboards, log correlation |
| Backup/restore | Medium | API exists; needs CLI workflow, restore drills, cross-version tests |
| Operations | Medium | Config, preflight, scripts, and packaging exist; needs upgrade drills |

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
6. Document and rehearse backup/restore operations.
7. Package server and CLI with example config and systemd units.

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
