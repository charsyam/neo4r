# RBAC Policy

Neo4r web/admin RBAC is action-based. HTTP routes do not compare raw role names
as the source of truth; routes are assigned to `WebAction`, and
`web_role_allows_action(role, action)` evaluates the minimum required role.

## Roles

- `reader`: graph reads, query plans, profile, metrics, system read endpoints.
- `writer`: all reader permissions plus mutating graph queries.
- `admin`: all writer permissions plus token, tenant, backup, restore, Raft,
  repair, audit, and cluster management actions.

## Actions

- `GraphRead`: `/api/graph`, `/api/query-plan`, `/api/profile`.
- `GraphWrite`: `/api/query`.
- `MetricsRead`: `/api/metrics`, `/metrics`, `/api/slow-queries`.
- `SystemRead`: `/api/capabilities`, `/api/admin/system-policy`,
  `/api/admin/distributed-query`, `/api/statistics`, `/api/storage`,
  `/api/metadata-log`, cluster status/registry/routing views, database
  selection, and examples.
- `TokenAdmin`: user and token lifecycle endpoints.
- `TenantAdmin`: database create, enable, disable, delete, role update, and
  list endpoints.
- `AuditAdmin`: audit log and audit pruning.
- `BackupAdmin`: backup creation.
- `RestoreAdmin`: restore, PITR restore planning, and maintenance mode.
- `RaftAdmin`: raft status, snapshot, and leader transfer.
- `RepairAdmin`: storage invariant verify and repair.
- `ClusterAdmin`: rebalance and migration operations.

## Enforcement

1. Authentication selects a database-scoped role from session, bearer token, or
   configured static token.
2. The route maps to a `WebAction`.
3. The central policy maps action to a minimum role.
4. The request is denied with `403` when the selected role does not satisfy the
   action requirement.

Database-scoped token checks happen before action authorization. A token scoped
to `tenant_a` cannot acquire any role for `default`, even if its global role is
higher.

## Grants And Revokes

Database-scoped grants are token-scoped and explicit:

- `POST /api/admin/grant-role` requires `TokenAdmin` and accepts `name`,
  `token_id`, `database`, `role`, and optional `reason`.
- `POST /api/admin/revoke-role` requires `TokenAdmin` and accepts `name`,
  `token_id`, `database`, and optional `reason`.

Both operations append audit events (`rbac.grant` or `rbac.revoke`) with the
token, database, granted role when present, and reason. Authorization state stays
in RocksDB through the token record's `database_roles` field.

When `database_roles` is empty, the token's base role applies to every database.
Revoking a database-scoped role removes only that override; it does not revoke
the token itself. To remove all access, call the token revoke endpoint.
