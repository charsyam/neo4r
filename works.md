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

## Replication Identity, Negotiation, SDK/API Goal 1-10

Requested scope: set items 1 through 10 as a goal and complete them in order.

Status:

- Completed.

Completed changes:

1. Persisted replication peer identity separately from address-only peer state.
2. Extended replication topology cycle detection beyond direct self loops.
3. Added explicit `NEGOTIATE_REPLICATION_PEER` endpoint handshake before
   negotiated peer add.
4. Introduced cluster/database identity in TCP replication hello metadata.
5. Connected negotiated peer registration to routing membership validation.
6. Strengthened durable apply crash-point/atomicity coverage with a dedicated
   storage atomicity script.
7. Expanded WriteBatch-backed graph mutation verification through the same
   script.
8. Added Rust and Python SDK helpers for tenant/database/auth administration
   flows.
9. Exposed replication channel metrics through the database handle and
   `REPLICATION_STATUS`.
10. Added CI matrix entrypoints for SDK/live, storage atomicity, and replication
    negotiation checks.

Verification:

- `cargo test -p neo4r-server replication_peer --quiet`
- `scripts/sdk-api-parity.sh`
- `scripts/storage-atomicity.sh`

## Client Redirect And Routing Discovery

Requested scope: add a broker-like routing/discovery protocol and make clients
handle redirects automatically.

Status:

- Completed.

Completed changes:

- Added typed backend redirect responses with `MOVED`, `NOT_LEADER`, and
  `STALE_ROUTING` wire names.
- Added `ROUTING_TABLE` native command and `/api/cluster/routing-table` HTTP
  discovery endpoint.
- Added `CLUSTER_REGISTRY` native command and `/api/cluster/registry` HTTP
  broker-style discovery endpoint for routing plus peer address metadata.
- Added registry freshness metadata: `ownership_epoch`, membership index,
  generated timestamp, TTL, and migration state.
- Changed missing-primary-address shard forwarding failures to return a
  structured `ERR MOVED` redirect payload.
- Added Rust SDK redirect parsing, bounded automatic reconnect/retry, redirect
  loop detection, and topology cache updates.
- Added Python SDK redirect parsing, bounded automatic reconnect/retry, public
  `RedirectError`, and native/HTTP topology cache updates.
- Added protocol/SDK tests for redirect formatting, parsing, Rust automatic
  retry, redirect loop detection, HTTP stale epoch rejection, and registry
  freshness metadata.

## Ownership Epoch And Cluster Topology

Completed changes:

- Exposed shard ownership epoch as the routing version alias across native
  routing output, redirect payloads, HTTP routing JSON, and registry JSON.
- Added HTTP stale ownership epoch rejection for query/graph data-path
  endpoints using `x-neo4r-ownership-epoch` or `x-neo4r-routing-epoch`.
- Surfaced rebalance execution state as migration state in cluster management
  and registry responses.
- Added a web console topology action that loads registry and raft status
  together for cluster operations.

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

## Cluster Metadata, Migration, And Capability Hardening

Requested scope: implement follow-up items 1 through 10 for shard migration,
metadata consensus linkage, snapshot bootstrap, read consistency, transaction
epoch validation, failure simulation, admin API cleanup, protocol negotiation,
metrics, and storage crash-point coverage.

Status:

- Completed.

Completed changes:

1. Exposed recoverable migration state through cluster management, registry JSON,
   and metrics using the existing rebalance execution state machine.
2. Linked cluster metadata visibility to routing ownership by adding
   `metadata_index`, membership index, routing version, and ownership epoch to
   registry/control-plane responses.
3. Strengthened snapshot bootstrap coverage by retrying an install snapshot after
   an injected payload-before-metadata crash point.
4. Exposed HTTP read consistency options for read queries:
   `strong`, `follower_stale`, and `bounded_staleness`.
5. Added transaction ownership epoch capture at `BEGIN_TX` and stale epoch
   rejection at `COMMIT_TX`.
6. Added failure simulation tests for HTTP stale ownership epoch rejection,
   registry freshness metadata, capability discovery, bounded-staleness query
   options, redirect loop detection, and snapshot crash-point retry.
7. Added admin cluster operation aliases under `/api/admin/cluster/*` for
   snapshot and migration/rebalance operations.
8. Added native `CAPABILITIES`, HTTP `/api/capabilities`, and Rust/Python SDK
   capability helpers.
9. Added HTTP metrics for registry requests, stale epoch rejections, redirect
   surface, and migration state.
10. Updated README/API documentation and SDK parity checks for the new surfaces.

Verification:

- `cargo fmt --all`
- `cargo test -p neo4r-server capabilities --quiet`
- `cargo test -p neo4r-server stale_ownership_epoch --quiet`
- `cargo test -p neo4r-server http_query_accepts_bounded_staleness_read_consistency --quiet`
- `cargo test -p neo4r-server native_read_write_transaction_rejects_stale_ownership_epoch_on_commit --quiet`
- `cargo test -p neo4r-db raft_snapshot_fault_injection_persists_payload_before_metadata --quiet`
- `cargo test -p neo4r-client --quiet`
- `PYTHONPATH=sdks/python python3 -m unittest discover -s sdks/python/tests`
- `scripts/sdk-api-parity.sh`

## Protocol, SDK Routing, And Regression Harness Hardening

Requested scope: implement follow-up items 1 through 10 covering shard migration
snapshot transfer wiring, multi-node harnesses, Raft metadata/routing policy,
typed transaction epoch conflicts, SDK topology-cache use, storage atomicity
regression, protocol versioning, security hardening, and benchmark regression.

Status:

- Completed.

Completed changes:

1. Added a migration action that reports `snapshot_bootstrap_required` when a
   joining replica has no observed match index while the shard already has
   committed data.
2. Added `scripts/multi-node-integration.sh` to run snapshot/catch-up/control
   plane checks, with an opt-in `NEO4R_RUN_MULTI_NODE=1` live server harness.
3. Exposed Raft-aware metadata/write policy through native/HTTP capabilities and
   cluster registry responses as `shard_primary_and_raft_leader`.
4. Changed stale transaction ownership conflicts to a typed native
   `ERR	STALE_EPOCH`-style protocol response with current routing epoch and
   retryability metadata.
5. Added Rust/Python SDK `connect_to_cached_target` APIs and parity checks so
   clients can explicitly reconnect using fresh topology-cache targets.
6. Documented the write authority policy and typed epoch conflict behavior in
   README.
7. Added `scripts/storage-atomicity.sh` to the integration pipeline and kept
   relationship/property/label WriteBatch invariants independently runnable.
8. Added protocol min/max capability fields for native and HTTP protocol
   version negotiation.
9. Added `scripts/security-regression.sh` for admin auth, tenant scoping, token
   invoke/revoke, and token expiry regression tests.
10. Added `scripts/bench-regression.sh` to combine smoke performance checks with
    the 1000+ assertion data correctness suite.

Verification:

- `cargo fmt --all`
- `cargo test -p neo4r-db cluster_rebalance_reports_snapshot_bootstrap_before_catch_up --quiet`
- `cargo test -p neo4r-server native_read_write_transaction_rejects_stale_ownership_epoch_on_commit --quiet`
- `cargo test -p neo4r-client client_parses_typed_stale_epoch_response --quiet`
- `PYTHONPATH=sdks/python python3 -m unittest sdks/python/tests/test_protocol.py`
- `scripts/sdk-api-parity.sh`
- `scripts/multi-node-integration.sh`
- `scripts/storage-atomicity.sh`
- `scripts/security-regression.sh`
- `scripts/bench-regression.sh`

## CI, Operations, And Observability Follow-up

Requested scope: continue the 1 through 10 follow-up plan and address GitHub CI
link failures.

Status:

- Completed.

Completed changes:

1. Updated GitHub Actions to install `librocksdb-dev`, `pkg-config`, and
   `clang` before `cargo build`, fixing the likely `cc` link failure for the
   direct RocksDB C API binding.
2. Added a `ShardReplicator::run_replication_pump` hook and wired TCP Raft
   replication pump execution into server-side `ADVANCE_REBALANCE` when a
   migration is waiting for snapshot bootstrap or catch-up.
3. Confirmed replicated `ClusterConfigChange` already applies routing table,
   config epoch, replicator routing, and Raft group rebuild through
   `apply_cluster_config_change`.
4. Added protocol version source-of-truth helpers in `neo4r-protocol` and
   `scripts/protocol-compat.sh`.
5. Extended Python HTTP SDK topology cache helpers with `cached_topology` and
   `refresh_topology_if_stale`.
6. Hardened tenant-scoped maintenance permissions by testing that scoped writer
   tokens cannot run tenant backup/snapshot/migration admin operations.
7. Added a Raft lease/read-index test showing expired leader lease falls back to
   quorum-confirmed read-index validation.
8. Extended `scripts/storage-atomicity.sh` to include the real crash harness.
9. Added Prometheus text metrics at `GET /metrics`.
10. Extended RocksDB-backed audit log access with `action`, `target`, and
    `limit` filters.

Verification:

- `cargo check --workspace`
- `cargo test -p neo4r-server backend_advance_rebalance_runs_auto_pump_for_snapshot_bootstrap --quiet`
- `cargo test -p neo4r-db expired_leader_lease_falls_back_to_quorum_read_index --quiet`
- `cargo test -p neo4r-server web_console_isolates_tenant_databases_and_scopes_tokens --quiet`
- `cargo test -p neo4r-server web_console_serves_index_and_graph_api --quiet`
- `scripts/protocol-compat.sh`
- `scripts/security-regression.sh`
- `scripts/storage-atomicity.sh`

## CI, Consensus, Storage, SDK, And Failure-Injection Goal 1-10

Requested scope: set the next proposed items 1 through 10 as a goal and
complete them in order.

Status:

- Completed.

Completed changes:

1. Hardened GitHub CI dependency setup with RocksDB and compression library
   development packages, plus shell syntax checks for helper scripts.
2. Fixed joint-consensus elections to require both current and outgoing voter
   quorums before a candidate can become leader.
3. Added Raft membership regression coverage for joint election quorum and
   local voter removal step-down behavior.
4. Fixed follower AppendEntries validation at an installed snapshot boundary by
   falling back to Raft snapshot metadata when the previous log entry has been
   compacted.
5. Added snapshot install -> append -> reopen coverage to verify materialized
   graph state survives catch-up from a snapshot boundary.
6. Extended storage crash coverage with a relationship/adjacency child-process
   kill harness and wired it into `scripts/storage-atomicity.sh`.
7. Added tenant restore authorization coverage so tenant-scoped tokens cannot
   run backup/restore/snapshot/migration maintenance operations.
8. Extended Python SDK native topology cache parsing to derive a reconnect
   target from `CLUSTER_REGISTRY` `query_peers` or active `nodes`.
9. Added replication channel capability negotiation so Raft-required operations
   do not select transports that only match by kind.
10. Expanded Prometheus metrics and added `scripts/failure-injection.sh` to
    group divergent log, snapshot fallback, and real crash-point checks.

Verification:

- `cargo fmt --all`
- `python3 -m py_compile sdks/python/neo4r_client/client.py sdks/python/tests/test_protocol.py`
- `PYTHONPATH=sdks/python python3 -m unittest sdks/python/tests/test_protocol.py`
- `cargo test -p neo4r-db joint_consensus --quiet`
- `cargo test -p neo4r-db replication_channel_negotiation --quiet`
- `cargo test -p neo4r-db raft_snapshot_install_then_append_survives_reopen --quiet`
- `cargo test -p neo4r-db query_plan_reports_read_access_path --quiet`
- `cargo test -p neo4r-server web_console_serves_index_and_graph_api --quiet`
- `cargo test -p neo4r-server web_console_isolates_tenant_databases_and_scopes_tokens --quiet`
- `scripts/storage-atomicity.sh`
- `scripts/failure-injection.sh`

## Multi-Node Operations And Release Gate Goal 1-10

Requested scope: set the next proposed items 1 through 10 as a goal and
complete them.

Status:

- Completed.

Completed changes:

1. Kept multi-process Raft and Jepsen-lite integration runners in the release
   path through `scripts/release-gate.sh`, with live execution gated by
   `NEO4R_RUN_RELEASE_LIVE=1`.
2. Tightened membership/read/snapshot release coverage by adding
   `scripts/read-consistency.sh` to the integration and release gates.
3. Documented the read consistency API contract around strong read-index,
   leader lease, and follower-stale reads.
4. Versioned backup manifests now include the selected database name, and
   restore rejects manifests for a different database.
5. Added `scripts/sdk-failover.sh` for Rust/Python redirect and topology-cache
   failover checks, with an optional live mode.
6. Added `scripts/query-plan-golden.sh` to pin optimizer access-plan, remote
   route, and HTTP query-plan coverage.
7. Added admin console controls for backup, restore verification, raft status,
   snapshots, invariant verification, and invariant repair.
8. Extended Prometheus output with database-labeled companion metrics for node,
   relationship, committed index, applied index, and Raft group gauges.
9. Added `docs/disk_lifecycle.md` for checkpoint, WAL cleanup, RocksDB
   compaction, backup, and release-check policy.
10. Added `scripts/release-gate.sh` to tie fast, correctness, crash, server,
    protocol, read consistency, query plan, SDK, storage, failure injection,
    security, and benchmark regression checks together.

Verification:

- `cargo fmt --all`
- `bash -n scripts/*.sh`
- `scripts/check-file-lines.sh`
- `cargo check --workspace`
- `scripts/read-consistency.sh`
- `scripts/query-plan-golden.sh`
- `scripts/sdk-failover.sh`
- `cargo test --workspace --quiet`
- `scripts/release-gate.sh`
