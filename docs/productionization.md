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

## Remaining Production Blockers

- continuous WAL archive shipping and timestamp-targeted PITR replay
- sustained multi-node chaos tests with real network faults enabled in CI
