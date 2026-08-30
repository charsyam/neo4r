# Incident Runbook

## No Raft Leader

1. Check `neo4r_raft_leaders` and `neo4r_raft_shard_leader_lease_remaining_ms`.
2. Confirm a majority of replicas are reachable.
3. Run `scripts/raft-soak.sh` on a restored copy if log divergence is suspected.
4. Prefer leader transfer before planned node drain.

## Restore Or Backup Failure

1. Check `neo4r_backup_restore_last_success_timestamp_seconds`.
2. Run restore dry-run before destructive restore.
3. Verify `neo4r-backup-manifest.txt` checksum and database name.
4. Keep maintenance mode enabled until post-restore query checks pass.

## Storage Invariant Failure

1. Run `/api/admin/verify-invariants`.
2. Enable a maintenance window before `/api/admin/repair-invariants`.
3. Check `neo4r_storage_repair_failures_total`.
4. Keep the original data directory until query correctness checks pass.
