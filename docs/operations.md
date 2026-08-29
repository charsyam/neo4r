# Neo4r Operations Guide

## Cluster Bootstrap

Build the server first:

```bash
cargo build -p neo4r-server
```

Validate and render a normalized YAML summary before starting a node:

```bash
target/debug/neo4r-server --config packaging/server.example.yml --check-config
target/debug/neo4r-server --config packaging/server.example.yml --dump-config
```

Run the stricter production preflight after replacing example secrets and paths:

```bash
target/debug/neo4r-server --config /etc/neo4r/server.yml --production-check
scripts/production-preflight.sh /etc/neo4r/server.yml
```

The production check rejects development defaults such as loopback-only binds,
relative or temp data directories, missing/weak web admin tokens, async
replication ACKs, and clustered configs without catch-up controls.

Start three local nodes with separate data directories and replication ports:

```bash
target/debug/neo4r-server --bind 127.0.0.1:17687 --replication-bind 127.0.0.1:18687 --data-dir data/node1 --server-id 1 --primary-server-id 1 --shards 1 --partitions 1 --web-auth-token admin:secret
target/debug/neo4r-server --bind 127.0.0.1:17688 --replication-bind 127.0.0.1:18688 --data-dir data/node2 --server-id 2 --primary-server-id 1 --shards 1 --partitions 1 --web-auth-token admin:secret
target/debug/neo4r-server --bind 127.0.0.1:17689 --replication-bind 127.0.0.1:18689 --data-dir data/node3 --server-id 3 --primary-server-id 1 --shards 1 --partitions 1 --web-auth-token admin:secret
```

Register replication peers on the primary:

```text
REGISTER_REPLICATION_PEER	2	127.0.0.1:18688	2	tcp
REGISTER_REPLICATION_PEER	3	127.0.0.1:18689	3	tcp
RAFT_STATUS
```

## Backup And Restore

Create a snapshot/backup artifact from the line protocol:

```text
SNAPSHOT_NOW
BACKUP_NOW
```

The returned `safety_manifest` is versioned and includes shard, term, index,
payload bytes, and checksum fields. Restore a local shard from the saved
snapshot payload:

```text
RESTORE_SNAPSHOT	0
```

The web backup and restore API operates on the selected database. Select the
target database with `db=NAME`, `x-neo4r-database`, a JSON `database` field, or
`USE NAME` in the submitted query. A tenant backup contains that tenant
database's data directory only. Web users, token lookup records, and audit logs
live under the server system area and are not restored as tenant graph data.
Operators should recreate or rotate scoped tokens after tenant restore unless
the whole server data directory is restored intentionally.

Dry-run restore verifies the backup manifest and reports file count/bytes
without copying files into the selected database:

```bash
curl -X POST -H 'authorization: Bearer admin:secret' \
  -H 'content-type: application/json' \
  -d '{"database":"tenant_a","path":"data/backups/tenant_a","dry_run":true}' \
  http://127.0.0.1:18080/api/restore
```

Destructive restore requires an explicit confirmation field:

```bash
curl -X POST -H 'authorization: Bearer admin:secret' \
  -H 'content-type: application/json' \
  -d '{"database":"tenant_a","path":"data/backups/tenant_a","dry_run":false,"confirm":"RESTORE"}' \
  http://127.0.0.1:18080/api/restore
```

If restore is interrupted, Neo4r writes `DATA_DIR/system/restore.pending` before
materializing the snapshot. On the next open, the pending manifest is replayed
from the local snapshot payload and cleared after successful metadata install.

## Tenant Auth

Use the admin web API to create scoped tokens:

```bash
curl -H 'authorization: Bearer admin:secret' \
  -H 'content-type: application/json' \
  -d '{"name":"operator","token_id":"main","role":"writer","token":"writer:operator-token","database":"default","database_role":"writer","expired_at":"0"}' \
  http://127.0.0.1:18080/api/admin/invoke-token
```

Expired tokens are rejected during authorization. Operators can remove expired
records and token lookup keys:

```bash
curl -X POST -H 'authorization: Bearer admin:secret' \
  http://127.0.0.1:18080/api/admin/cleanup-expired-tokens
```

Audit records are available from `/api/admin/audit-log` and in the web console
through the `Audit` button.

The CLI wraps the same admin API:

```bash
target/debug/neo4r-cli --http-host 127.0.0.1 --http-port 18080 \
  --admin-token admin:secret --list-users
target/debug/neo4r-cli --admin-token admin:secret --database default \
  --invoke-user operator --invoke-token-id main --invoke-token writer:token \
  --invoke-role writer --invoke-database-role writer --invoke-expired-at 4102444800
target/debug/neo4r-cli --admin-token admin:secret \
  --revoke-user operator --revoke-token-id main
```

## CLI Operation

Use the interactive CLI for query history, transaction checks, and operator
status:

```bash
target/debug/neo4r-cli --addr 127.0.0.1:17687
```

Useful REPL commands:

```text
:begin
MATCH (n) RETURN n;
:plan MATCH (n) RETURN n
:profile MATCH (n) RETURN n
:cluster
:storage
:routing
:replication
:commit
```

Single-shot backup and restore:

```bash
target/debug/neo4r-cli --admin-token admin:secret --database tenant_a \
  --backup data/backups/tenant_a
target/debug/neo4r-cli --admin-token admin:secret --database tenant_a \
  --restore data/backups/tenant_a --restore-dry-run
target/debug/neo4r-cli --admin-token admin:secret --database tenant_a \
  --restore data/backups/tenant_a --restore-confirm RESTORE
```

## Recovery Checks

After restart or restore, check:

```text
STORAGE_STATUS
RAFT_STATUS
STATISTICS
```

For web deployments, also inspect:

```bash
curl -H 'authorization: Bearer admin:secret' http://127.0.0.1:18080/api/metrics
curl -H 'authorization: Bearer admin:secret' http://127.0.0.1:18080/api/admin/audit-log
```

Run storage invariant checks after restore, crash recovery, or a suspicious
restart:

```text
VERIFY_INVARIANTS
REPAIR_INVARIANTS
```

The read freshness contract is documented in
[read_consistency.md](read_consistency.md).

## RDMA Live Gate

The default RDMA gate validates provider abstraction without requiring RDMA
libraries:

```bash
scripts/rdma-live-gate.sh
```

Run it on RDMA hosts only when `neo4r-server --features rdma` can link on both
machines:

```bash
NEO4R_RUN_RDMA_LIVE=1 \
NEO4R_RDMA_NODE_A=192.168.0.175 \
NEO4R_RDMA_NODE_B=192.168.0.176 \
NEO4R_RDMA_ADDR_A=192.168.0.175 \
NEO4R_RDMA_ADDR_B=192.168.0.176 \
scripts/rdma-live-gate.sh
```

## Packaging

Example deployment files live under `packaging/`:

- `server.example.yml`
- `neo4r-server.env`
- `neo4r-server.service`

For local installation:

```bash
sudo scripts/install-local.sh
sudo systemctl enable --now neo4r-server
```

The systemd unit runs `neo4r-server --production-check` as `ExecStartPre`.
`packaging/server.example.yml` intentionally ships with a placeholder admin
token and must be edited before the service can start.

## Production Readiness

Neo4r is still below production DB maturity for high-stakes data. The current
status and gap list are tracked in
[production_db_comparison.md](production_db_comparison.md). Treat every release
candidate as needing at least:

```bash
scripts/release-gate.sh
scripts/crash-consistency-gate.sh
```
