# Security Notes

Current hardening:

- bootstrap web tokens use constant-time comparison.
- user token lookup keys are digest based, with legacy plaintext lookup cleanup
  on token replacement/revoke/delete.
- tokens can be scoped per database and can expire.
- admin mutations append audit events.
- web authentication failures are exported as `auth_failures` in JSON metrics
  and `neo4r_auth_failures_total` in Prometheus metrics.
- repeated web authentication failures are rate limited and exported as
  `auth_rate_limited` / `neo4r_auth_rate_limited_total`.
- token lookup keys use a stable keyed digest instead of Rust's randomized
  `DefaultHasher`.
- new token lookups support a primary digest key with legacy digest lookup during
  rotation.
- browser session cookies include a bounded `Max-Age`, and cookie-authenticated
  mutation requests require `X-Neo4r-Csrf: neo4r-admin`.
- browser login exchanges bearer tokens for opaque `sid:` records stored under
  `system/web-session-rocksdb`.

Next hardening targets:

- replace the built-in stable digest with a dedicated KDF or keyed MAC.
- redact token-like values from audit and slow query output.
- replace the static CSRF marker with per-session CSRF secrets.

Until token storage is fully migrated, operators should rotate tokens after
backup/restore and avoid sharing tenant admin tokens across environments.
