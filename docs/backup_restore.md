# Backup And Restore Contract

Backups copy one selected database directory and write a manifest containing:

- manifest version
- selected database name
- source path
- target path
- file count
- total bytes
- deterministic payload checksum
- committed shard indexes at backup time

Restore first verifies the manifest against the copied payload. A dry run
performs the same verification without writing into the target database.
The selected restore database must match the manifest database.

Operational constraints:

- restore requires an existing manifest.
- manifest file itself is excluded from file count and checksum.
- tenant graph backups do not restore system auth/audit state.
- interrupted restore is recovered through `system/restore.pending`.
- destructive restore requires explicit restore maintenance mode.
- destructive restore acquires `system/restore.lock` with create-new semantics.
  A second restore fails before copying any backup payload.
- backup creation and restore verification/application append audit events.

Recommended validation after restore:

```text
STORAGE_STATUS
VERIFY_INVARIANTS
STATISTICS
```
