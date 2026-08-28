# Security Notes

Current hardening:

- bootstrap web tokens use constant-time comparison.
- user token lookup keys are digest based, with legacy plaintext lookup cleanup
  on token replacement/revoke/delete.
- tokens can be scoped per database and can expire.
- admin mutations append audit events.
- web authentication failures are exported as `auth_failures` in JSON metrics
  and `neo4r_auth_failures_total` in Prometheus metrics.

Next hardening targets:

- replace `DefaultHasher` token digest with a stable KDF or keyed digest.
- avoid storing plaintext token material in user records.
- add rate limiting to auth failures.
- redact token-like values from audit and slow query output.
- separate session cookies from bearer tokens for browser admin usage.

Until token storage is fully migrated, operators should rotate tokens after
backup/restore and avoid sharing tenant admin tokens across environments.
