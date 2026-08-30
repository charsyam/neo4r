# Release Checklist

Use this checklist before publishing a neo4r build or SDK package.

## Required Metadata

- Git commit SHA
- Rust crate versions
- Python SDK version
- protocol capability string from `CAPABILITIES`
- supported native wire result contract version
- RocksDB format compatibility note
- migration and backup/restore notes

## Required Gates

```bash
scripts/production-preflight.sh /etc/neo4r/server.yml
scripts/production-hardening-gate.sh
scripts/release-gate.sh
```

For live cluster validation:

```bash
NEO4R_RUN_RELEASE_LIVE=1 scripts/release-gate.sh
```

## Operator Checks

- Run a dry-run restore from the target backup before destructive restore.
- Confirm no `system/restore.lock` exists unless an active restore owns it.
- Confirm Prometheus scraping includes database, shard, server, and role labels.
- Confirm `production.tls_mode` is `external` or `required`. If it is
  `required`, verify `native_tls_cert` and `native_tls_key`, and verify
  `native_tls_client_ca` when mTLS is required.
- Confirm `production.replication_tls_mode` is `external` or `required`. If it is
  `required`, verify `replication_tls_cert`, `replication_tls_key`,
  `replication_tls_ca`, and `replication_tls_server_name`; also verify
  `replication_tls_client_ca` when peer mTLS is required.
- Confirm `production.min_native_protocol_version` and
  `production.max_native_protocol_version` match `CAPABILITIES`.
- Confirm backup drill, audit retention, secret rotation, and tenant quota
  policy are set in the deployed YAML.
- Confirm WAL archive, restore drill manifest, upgrade manifest, alert rules,
  query regression corpus, runbook, systemd unit, and logrotate policy are set
  in the deployed YAML.
- Confirm admin users have scoped database permissions, expiry, and revocation
  paths tested.
- Confirm SDK examples can connect, query, and recover from topology redirects.
- Confirm the nightly live workflow passed for the release candidate SHA.
- Attach performance baseline output for query, write, index, vector, cursor,
  and reopen paths.

## Artifact Notes

Every release artifact should include:

- `README.md`
- `docs/query_result_contract.md`
- `docs/backup_restore.md`
- `docs/security.md`
- `docs/productionization.md`
- `docs/replication_boundary.md`
- output from `scripts/release-gate.sh`
