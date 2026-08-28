# Backup And Restore Contract

Backups copy one selected database directory and write a manifest containing:

- manifest version
- source path
- target path
- file count
- total bytes
- deterministic payload checksum

Restore first verifies the manifest against the copied payload. A dry run
performs the same verification without writing into the target database.

Operational constraints:

- restore requires an existing manifest.
- manifest file itself is excluded from file count and checksum.
- tenant graph backups do not restore system auth/audit state.
- interrupted restore is recovered through `system/restore.pending`.

Recommended validation after restore:

```text
STORAGE_STATUS
VERIFY_INVARIANTS
STATISTICS
```
