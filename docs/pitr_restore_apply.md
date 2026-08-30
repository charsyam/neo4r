# PITR Restore Apply

PITR restore has two HTTP phases.

1. `POST /api/admin/restore-pitr` computes the target shard indexes for a
   hybrid timestamp and returns a dry-run plan.
2. `POST /api/admin/restore-pitr/apply` requires `confirm: "RESTORE_PITR"` and
   writes `system/pitr-restore.pending` under the selected database directory.

The pending manifest is durable and contains the selected database, target
hybrid timestamp, and target shard indexes. Restore application workers must
read this manifest before replacing materialized state so interrupted PITR can
resume from the same target.

The apply endpoint is admin-only through `WebAction::RestoreAdmin` and appends a
`restore.pitr.apply` audit event.
