# PITR Restore Apply

PITR restore has two HTTP phases.

1. `POST /api/admin/restore-pitr` computes the target shard indexes for a
   hybrid timestamp and returns a dry-run plan.
2. `POST /api/admin/restore-pitr/apply` requires `confirm: "RESTORE_PITR"`,
   writes `system/pitr-restore.pending` under the selected database directory,
   rebuilds each shard from WAL entries at or before the target timestamp, and
   truncates WAL suffixes after the target index.
3. `GET /api/admin/restore-pitr/pending` returns the durable pending manifest so
   an operator or restore worker can verify the exact target before replay.
4. `POST /api/admin/restore-pitr/complete` requires `confirm: "PITR_COMPLETE"`
   and removes the pending manifest after the restore worker has completed.

The pending manifest is durable and contains the selected database, target
hybrid timestamp, and target shard indexes. The apply worker replaces
materialized state through the same snapshot-state replacement path used by
snapshot restore, then the completion endpoint clears the manifest.

The apply endpoint is admin-only through `WebAction::RestoreAdmin` and appends a
`restore.pitr.apply` audit event.
