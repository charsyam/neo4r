# Neo4r Productionization Contract

Neo4r productionization is enforced through deployment gates rather than by
operator convention.

## Required Gates

Run these before a production rollout:

```bash
scripts/production-preflight.sh /etc/neo4r/server.yml
scripts/production-hardening-gate.sh
scripts/crash-consistency-gate.sh
scripts/release-gate.sh
```

## Security

Production configs must declare encrypted network boundaries:

- `production.tls_mode`: `external` or `required`
- `production.replication_tls_mode`: `external` or `required`. Use `required`
  for the built-in TCP replication TLS channel and configure peer CA validation.
- `production.web_tls_mode`: `external` or `required` for the web admin listener.

`external` means TLS/mTLS is terminated by a sidecar, load balancer, service
mesh, stunnel, or host-level transport policy. `required` enables in-process
TLS for the native client protocol, web admin listener, or TCP replication
protocol. Native TLS requires `native_tls_cert` plus `native_tls_key`; web TLS
requires `web_tls_cert` plus `web_tls_key`; replication TLS requires
`replication_tls_cert`, `replication_tls_key`, `replication_tls_ca`, and
`replication_tls_server_name`. Set the corresponding `*_require_client_auth`
field with a client CA to require mTLS client certificates. `disabled` is
rejected by `--production-check`.

The native server path is split at `IntoNativeStreamParts`, and the Rust client
stores a boxed `ClientTransport`. The TCP replication provider uses a matching
`TlsReplicationChannel`, so frame encoding, raft RPCs, catch-up, and query
execution stay transport-neutral above the stream boundary.

## Upgrade Compatibility

Every production config must pin the supported native protocol window, data
format version, and mixed-version upgrade manifest:

```yaml
production:
  min_native_protocol_version: 1
  max_native_protocol_version: 1
  data_format_version: 1
  upgrade_manifest: docs/rolling_upgrade_manifest.yml
```

Rolling upgrades must pass protocol and SDK compatibility gates before any data
format change is deployed.

## Backup And PITR

Backups are production-acceptable only when a restore drill policy is configured
and exercised:

```yaml
production:
  backup_drill_max_age_hours: 24
  wal_archive_dir: /var/lib/neo4r/wal-archive
  restore_drill_manifest: docs/pitr_restore_drill.yml
```

Point-in-time recovery now has a production gate for the declared archive
contract, segmented WAL replay, and uncommitted WAL rejection. Continuous archive
shipping and timestamp-targeted replay remain the next PITR runtime expansion.

## Audit And Secret Rotation

Production configs must define audit retention and token rotation windows:

```yaml
production:
  audit_retention_days: 90
  secret_rotation_days: 30
```

## Tenant Quota

Tenant quota policy is required before production:

```yaml
production:
  tenant_max_concurrent_queries: 128
  tenant_max_result_rows: 100000
```

The current runtime enforces result row limits across HTTP, native, distributed,
shard, staged shard, write shard, and transaction read paths. Concurrent query
limits are acquired at execution entry for HTTP and native query paths. Native
cursor queries retain the concurrency permit until the cursor is exhausted,
closed, or its session ends.

## Operational Readiness

Production configs must point at release-reviewed operational artifacts:

```yaml
production:
  raft_lease_clock_drift_bound_ms: 50
  raft_lease_message_delay_bound_ms: 200
  observability_alerts: docs/prometheus_alerts.yml
  repair_check_on_startup: true
  query_regression_corpus: docs/query_regression_corpus.yml
  chaos_gate_required: true
  runbook: docs/production_runbook.md
  systemd_unit: packaging/neo4r-server.service
  logrotate: packaging/neo4r.logrotate
```

## Production-Level Gate

The production-level gate ties the next database-hardening work to executable
checks:

- snapshot plus WAL tail catch-up is validated by membership automation and
  snapshot fetch tests.
- gossip-to-membership reconciliation is validated by gossip discovery tests and
  topology reconcile smoke checks.
- transport security is validated by TLS rotation, certificate inventory, and
  gossip shared-token production checks.
- replication backpressure is exposed through JSON/Prometheus metrics and
  bounded by `replication.max_in_flight_batches`.
- PITR restore drills require snapshot, WAL replay, query corpus, and
  seed-new-cluster evidence.
- storage crash-point atomicity is validated by atomic batch tests and real
  crash harnesses.
- network chaos checks cover partition, delay, dropped batches, interrupted
  snapshot fetch, and rolling restart contracts.
- mixed-version rolling upgrade is gated by protocol, snapshot fetch, SDK, and
  previous-release fixture compatibility.
- admin API authorization is centralized in RBAC route mapping and audit events
  cover token, tenant, backup, restore, repair, and cluster actions.
- benchmark thresholds are parsed from `docs/performance_thresholds.txt` and
  enforced by `scripts/bench-thresholds.sh`.

Run:

```bash
scripts/production-level-gate.sh
```

## Production Readiness Contract

`docs/production_readiness_contract.yml` is the explicit checklist for the ten
production readiness axes:

1. Raft/metadata-log authority for membership and shard ownership.
2. seed-array client bootstrap through `CLUSTER_REGISTRY`.
3. automated three-or-more-node failover coverage.
4. replica replacement gated by replay completion.
5. explicit read consistency modes.
6. restore drill that seeds a new cluster from restored data.
7. storage crash-point atomicity gates.
8. shard health, replay, lag, election, and snapshot observability.
9. TLS/mTLS, inter-node auth, RBAC, token, audit, and admin hardening gates.
10. executable production test matrix in the release gate.

`scripts/production-readiness-gate.sh` validates that these contract entries,
their evidence files, and the server-side `ProductionReadinessContract` test stay
in sync. The release gate invokes it before packaging checks.

The same gate also enforces maturity depth for the highest-risk eight blockers:
at least two automated three-node failover tests, three authoritative
membership/ownership tests, two replica replacement tests, two replay write
rejection tests, five storage crash points, two PITR restore drills, five rolling
upgrade checks, and four performance/chaos gates. These thresholds are still
pre-production thresholds, but they prevent the project from claiming production
readiness using documentation-only evidence.

## Remaining Production Blockers

- continuous WAL archive shipping and timestamp-targeted PITR replay
- sustained multi-node chaos tests with real network faults enabled in CI
