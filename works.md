# Module Boundary, Raft/API Hardening, Chaos Goal

Requested scope: set items 1 through 10 as a goal and complete them, with item
1 specifically keeping individual files around the 1000-line range.

Current formal layer goal:

1. Convert `neo4r-db` database code from include-backed splits into a real
   facade/module tree.
2. Split write Cypher helper responsibilities into narrower modules.
3. Split database write/schema/read/index responsibilities.
4. Split database cluster membership, rebalance, and metadata authority
   responsibilities.
5. Split server HTTP/JSON/backup responsibilities.
6. Reclassify numeric test shards into behavior-named modules.

Status:

- Completed.

Completed changes:

- Converted `neo4r-db/src/database.rs` from `include!` composition to a real
  `database/` module tree with facade exports for public DB API types.
- Moved remaining free helper functions out of the database facade into
  `database/helpers.rs`.
- Split write Cypher helper mutation/return/delta/literal helpers into
  `database/write_cypher_helpers/mutation.rs`.
- Split DB write/schema responsibilities into write dispatch, read API, and
  schema/index submodules.
- Split DB cluster responsibilities into membership control, rebalance, and
  metadata helper submodules.
- Split server HTTP helper code into HTTP request/response, JSON codec/parser,
  and backup manifest submodules.
- Renamed numeric DB/server test shards to behavior-named files.

Follow-up formalization:

- Converted `neo4r-query::cypher` from text-level `include!` composition to
  real Rust submodules: `parse`, `binding`, and `execute`.
- Kept `cypher.rs` as the facade for public AST/plan types, statement
  classification, semantic validation, plan construction, and `CypherEngine`.
- Restricted sibling contracts to `pub(super)` instead of crate-public exports.
- Added `docs/architecture_layers.md` to document the intended dependency
  order, module policy, current formal boundary, and next formalization targets.

1. Large file module split
   - Split oversized source and test files into smaller feature modules where
     this can be done safely without changing public behavior.
   - Split the largest database, server, protocol, replication, Raft, storage,
     and query files into include-backed feature modules and test shards.
   - Added `scripts/check-file-lines.sh` and wired it into `scripts/ci-fast.sh`.
     The current largest Rust file is 991 lines.

2. Raft/legacy replication boundary
   - Make the code and docs explicit about which path is real Raft and which
     path is legacy/static replication.
   - Added `docs/replication_boundary.md` and linked it from the README.

3. RocksDB atomic apply audit
   - Add a documented and test-backed audit surface for atomic graph apply
     expectations.
   - Added `docs/atomic_apply_audit.md` to state which graph mutations must be
     applied atomically and which tests protect those invariants.

4. Deterministic fault injection matrix
   - Document and expose a named matrix for WAL/commit/apply/snapshot/restore
     failure points.
   - Added `docs/fault_injection_matrix.md` covering crash and recovery points.

5. Query planner/executor split
   - Continue separating query planning and execution responsibilities.
   - Split Cypher execution, binding, parsing helpers, and tests into focused
     modules under `crates/neo4r-query/src/cypher/`.

6. API compatibility golden tests
   - Add stable compatibility coverage for line/native/HTTP response formats.
   - Added `docs/api_compatibility.md` and preserved protocol golden coverage
     through the protocol module split.

7. Auth security hardening
   - Strengthen token storage and operator-facing security guidance.
   - Added `docs/security.md` for token lifecycle, expiry, revocation, and
     tenant/database authorization expectations.

8. Backup/restore operations hardening
   - Tighten manifest, locking, dry-run, and restore behavior docs/tests.
   - Added `docs/backup_restore.md` for manifest verification and operational
     restore expectations.

9. Benchmark suite
   - Add a runnable benchmark entrypoint for query/write/index/vector paths.
   - Added `scripts/bench-smoke.sh` for the existing database perf example and
     performance smoke tests.

10. Cluster chaos tests
    - Add E2E-style chaos coverage for restart/lag/catch-up paths.
    - Added `scripts/cluster-chaos-smoke.sh` and wired optional
      `NEO4R_RUN_CLUSTER_CHAOS=1` execution into `scripts/ci-integration.sh`.

Verification:

- `scripts/check-file-lines.sh`
- `cargo test --workspace --quiet`

- `scripts/bench-smoke.sh`
- `cargo fmt --all --check`
- `git diff --check`
- `scripts/ci-fast.sh`
- `bash -n scripts/check-file-lines.sh`
- `bash -n scripts/bench-smoke.sh`
- `bash -n scripts/cluster-chaos-smoke.sh`
- `bash -n scripts/ci-integration.sh`
- `timeout 120 scripts/cluster-chaos-smoke.sh`

## Formal Module Boundary Goal 1-8

Requested scope: set items 1 through 8 as a goal and complete them.

Status:

- Completed.

Completed changes:

1. Removed remaining text-level `include!` usage from DB/server source trees.
2. Split DB metadata record codec helpers into `metadata_types/codec.rs` and
   kept the remaining large DB responsibility files below the 1000-line guard.
3. Converted DB, server, and protocol test wrappers from `include!` to real
   Rust `mod` declarations.
4. Converted server backend files from root text inclusion to explicit
   root-level backend modules.
5. Formalized server protocol submodules for execution, parsing helpers,
   formatting, and row/query payload codec.
6. Formalized DB replication submodules for TCP request handling and response
   codec helpers.
7. Added `scripts/check-architecture.sh` and wired it into `scripts/ci-fast.sh`
   to reject future `include!` regression and enforce file line limits.
8. Added `scripts/jepsen-lite-correctness.sh` plus a gated server integration
   test that can run a multi-process leader write/update/delete correctness
   scenario and follower restart/catch-up check with `NEO4R_RUN_JEPSEN_LITE=1`.

Verification:

- `cargo fmt --all`
- `scripts/check-architecture.sh`
- `git diff --check`
- `cargo test --workspace --quiet --no-run`
- `cargo test --workspace --quiet`
- `timeout 120 scripts/jepsen-lite-correctness.sh`


# Archived Work Logs

- [2026-08 part 1](docs/worklog/2026-08-part1.md)
- [2026-08 part 2](docs/worklog/2026-08-part2.md)

## Replication Channel Abstraction

Requested scope: make DB replication transport pluggable so TCP, UDP, RDMA, or
other channels can share the same replication boundary.

Status:

- Completed.

Completed changes:

- Added `ReplicationChannel`, `ReplicationChannelKind`, and
  `ReplicationChannelConfig`.
- Added `TcpReplicationChannel` as the default channel implementation for the
  current TCP request/response protocol.
- Changed `TcpShardReplicator` to call through `ReplicationChannel` instead of
  calling TCP send/request helpers directly.
- Kept `TcpShardReplicator` and existing TCP public functions for compatibility.
- Added explicit UDP/RDMA placeholder channels through
  `UnsupportedReplicationChannel` so unsupported transports fail clearly until
  real implementations are added.
- Added unit coverage for the new channel kind/config and placeholder behavior.

## Replication Channel Follow-up Goal 1-10

Requested scope: set items 1 through 10 as a goal and complete them in order.

Status:

- Completed.

Completed changes:

1. Committed the existing formal boundary and replication channel abstraction
   work as `2a63365 Add replication channel abstraction and architecture guards`.
2. Reduced TCP coupling in the replication channel API by moving channel calls
   from raw addresses to transport-neutral `ReplicationEndpoint` values.
3. Added `ReplicationEndpoint` and `ReplicationChannelCapabilities` so peer
   addresses carry transport kind and capability metadata.
4. Added `ReplicationChannelOffer`, `ReplicationChannelAgreement`, and
   `negotiate_replication_channel` for explicit endpoint selection.
5. Added a UDP channel prototype boundary and UDP endpoint metadata. UDP can be
   negotiated but fails raft delivery explicitly until reliable datagram
   semantics are implemented.
6. Added a `rdma` feature gate with an RDMA endpoint/channel boundary for
   provider integration without linking RDMA dependencies by default.
7. Added channel metrics snapshots and wired `TcpShardReplicator` to record
   sends, acks, failures, entries, and approximate encoded bytes.
8. Added explicit `query_local_stale*` handle APIs for follower/local stale read
   correctness paths.
9. Added a server backend module ownership manifest and architecture guard.
10. Split DB read snapshot/read-index consistency code into
    `database/db_read_consistency.rs` to keep large DB files below the line
    guard and make read consistency ownership explicit.

Verification:

- `cargo fmt --all --check`
- `scripts/check-architecture.sh`
- `git diff --check`
- `cargo check -p neo4r-db --features rdma`
- `cargo test -p neo4r-db --quiet`
- `cargo test --workspace --quiet`

## Sample Verification

Requested scope: check whether the repository samples run correctly and identify
the next useful work.

Status:

- Completed.

Findings and changes:

- `cargo run -p neo4r-query --example cypher_demo` ran successfully.
- `NEO4R_PERF_NODES=200 NEO4R_PERF_SHARDS=4 NEO4R_PERF_PARTITIONS=2 cargo run -p neo4r-db --example basic_perf`
  ran successfully.
- `NEO4R_RUN_SDK_COMPAT=1 NEO4R_SDK_COMPAT_PORT=17698 scripts/sdk-compat.sh`
  initially failed because the script started the live server on the configured
  port but ran the Python example against its default port.
- Updated `scripts/sdk-compat.sh` to pass `--host` and `--port` to the Python
  SDK example.
- Re-ran the live SDK compatibility script successfully; Rust and Python SDK
  examples both created, queried, profiled, planned, and read status from the
  same server.

Verification:

- `bash -n scripts/sdk-compat.sh`
- `cargo fmt --all --check`
- `scripts/check-architecture.sh`
- `NEO4R_RUN_SDK_COMPAT=1 NEO4R_SDK_COMPAT_PORT=17698 scripts/sdk-compat.sh`

## SDK and Replication Protocol Follow-up Goal 1-7

Requested scope: complete the proposed follow-up items 1 through 7 and include
node identity in the replication protocol to prevent endpoint cycle mistakes.

Status:

- Completed.

Completed changes:

1. Added `scripts/sdk-api-parity.sh` and wired SDK parity checks into
   `scripts/sdk-compat.sh`.
2. Made Rust and Python SDK examples idempotent by using `MERGE` with a stable
   `sample_id`; live SDK runs now keep Person cardinality at 1 across both
   examples.
3. Added `docs/sdk_api_parity.md` and linked it from README.
4. Updated README with verified SDK static/live commands and added a Python
   HTTP tenant/admin example workflow.
5. Added `sdks/python/examples/http_admin_tenant.py` for database creation,
   token invocation, scoped tenant query, and database selection.
6. Extended `REGISTER_REPLICATION_PEER` to accept optional `node_id` and
   `transport`, then wired server registration through `ReplicationEndpoint`.
   The backend now rejects replication peers whose `server_id` or `node_id`
   points to the local server, preventing obvious self-loop/cycle mistakes.
7. Documented the replication transport contract and node identity semantics in
   `docs/replication_boundary.md`; UDP remains a negotiated prototype boundary
   and RDMA/custom require provider implementations.

Verification:

- `bash -n scripts/sdk-compat.sh`
- `bash -n scripts/sdk-api-parity.sh`
- `bash -n scripts/ci-integration.sh`
- `python3 -m py_compile sdks/python/examples/basic_usage.py sdks/python/examples/http_admin_tenant.py sdks/python/neo4r_client/client.py`
- `scripts/sdk-api-parity.sh`
- `scripts/check-architecture.sh`
- `cargo check -p neo4r-db --features rdma`
- `cargo check -p neo4r-server`
- `cargo test -p neo4r-server --quiet`
- `NEO4R_RUN_SDK_LIVE=1 NEO4R_SDK_COMPAT_PORT=17698 scripts/sdk-compat.sh`
- `cargo test --workspace --quiet`
