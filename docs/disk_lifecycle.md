# Disk Lifecycle Policy

Neo4r keeps durability state in three layers:

- RocksDB materialized graph/index state.
- Per-shard segmented WAL entries.
- Per-shard snapshots and backup manifests.

## Checkpoints

Snapshots are the compaction boundary for a shard. A snapshot may only be
installed or generated at a committed log index. After snapshot install, Raft
AppendEntries validation must accept the snapshot metadata as the previous-log
boundary when the concrete WAL entry has already been compacted.

## Cleanup

Old WAL segments must not be deleted until every local recovery path can start
from a snapshot whose `last_included_index` is greater than or equal to the
deleted segment's max index. Replica migration and catch-up should prefer log
catch-up when the previous index is available, and install a snapshot when the
replica is behind the retained log window.

## RocksDB

Graph mutations are applied with RocksDB write batches so node, relationship,
label, property, and adjacency indexes move atomically. Operational compaction
must preserve this invariant: compaction changes physical layout only and must
not be used as a logical repair step.

## Backups

Backup manifests are versioned. Version 1 records the database name, source,
target, file count, total bytes, and checksum. Restore must verify the manifest
against the selected database before copying data.

## Release Checks

Run these before release:

```bash
scripts/storage-atomicity.sh
scripts/failure-injection.sh
scripts/release-gate.sh
```
