# Cluster Control Work Plan

Requested scope: implement cluster-control improvements 1 through 7 in order.

1. Rebalance plan execution state machine
   - Persist a current execution view for a rebalance plan.
   - Track per-step state and expose start/cancel/status commands.
   - Keep execution resumable after process restart.
   - Implemented with `RebalanceExecutionStore`, `START_REBALANCE`,
     `CANCEL_REBALANCE`, `REBALANCE_STATUS`, and per-step state.

2. Cluster metadata authority
   - Add an authority abstraction for membership, routing, and rebalance metadata mutations.
   - Require cluster-control mutations to pass through the local metadata authority guard.
   - Persist enough metadata to reject stale or unauthorized local changes.
   - Implemented with `ClusterMetadataStore`, `METADATA_AUTHORITY`, and
     `SET_METADATA_AUTHORITY`.

3. Automatic catch-up orchestration
   - Add a command/API that advances a rebalance execution by preparing replica additions,
     observing catch-up position, marking caught-up assignments, and applying ready steps.
   - Keep the implementation deterministic and locally testable.
   - Implemented with `ADVANCE_REBALANCE`, which prepares, observes match index,
     marks caught-up assignments, and applies ready steps.

4. Fencing and epoch
   - Track routing/config epoch.
   - Gate writes and primary transfer against stale epochs to prevent writes through old
     primaries after metadata changes.
   - Implemented persisted `config_epoch` and local write epoch validation.

5. Failure handling
   - Persist step failures with retryable/non-retryable classification.
   - Add retry and failure status reporting for interrupted rebalance execution.
   - Implemented per-step attempts, retryable flag, last error, and failed plan state.

6. Rebalance policy
   - Add policy inputs for replication factor and simple balancing limits.
   - Generate plans from the policy rather than hard-coded all-shard addition only.
   - Implemented `RebalancePolicy` with replication factor and max steps per plan.

7. Structured management API
   - Add stable structured status payloads for nodes, assignments, rebalance plans, and
     rebalance execution status.
   - Keep existing text line protocol responses backward-compatible where practical.
   - Implemented `CLUSTER_MANAGEMENT_STATUS` and structured cluster-management
     response payloads.

Verification target:

- `cargo fmt --all`
- `cargo test --workspace --no-run`
- `cargo test --workspace`

# Operations UX Work Plan

Requested scope: implement suggested features 1 through 7.

1. Web console query UX
   - Added query examples, local query history, JSON params input, Plan, Profile,
     Metrics, Slow, Cluster, and Rebalance controls.

2. Cypher workflow support
   - Kept the `CREATE ... WITH ... MATCH ... CREATE ... RETURN n, r` workflow
     available through web params and examples.

3. Admin/auth
   - Added `--web-auth-token TOKEN` for bearer-token or query-token web/API
     protection.

4. Backup/restore
   - Added `POST /api/backup` and `POST /api/restore` for controlled local
     directory snapshot copy workflows.

5. Observability
   - Added `/api/metrics` counters and `/api/slow-queries` in-memory slow query
     log controlled by `--slow-query-threshold-ms`.

6. Cluster operations
   - Exposed `/api/cluster`, `/api/cluster/plan-rebalance`, and
     `/api/cluster/advance-rebalance` to the web console.

7. SDKs
   - Added Rust/Python `query_plan`, `cluster_status`, and
     `cluster_management_status` helpers and updated examples.

Verification target:

- `cargo fmt --all`
- `cargo test --workspace`
- `PYTHONPATH=sdks/python python3 -m unittest discover -s sdks/python/tests -v`

# Data Correctness Test Work Plan

Requested scope: add a separately runnable data correctness test suite with
around 1000 checks covering input, query, update, and delete results.

Implemented:

1. Separate correctness test target
   - Added `crates/neo4r-db/tests/data_correctness.rs`.
   - Can be run independently with
     `cargo test -p neo4r-db --test data_correctness`.

2. Bulk node lifecycle correctness
   - Covers 360 node inserts, direct node reads, label/property validation,
     bucket queries, property updates, property removals, deletes, and final
     row-count checks.
   - Enforces at least 1000 explicit checks in the test.

3. Cypher mutation correctness
   - Covers Cypher `CREATE`, `MATCH ... SET`, `REMOVE`, conditional queries,
     and `DELETE` result correctness.

4. Relationship lifecycle correctness
   - Covers 180 relationship creates, traversal query results, relationship
     property updates/removals, relationship deletes, and `DETACH DELETE`
     cleanup behavior.
   - Enforces at least 1000 explicit checks in the test.

5. Reopen correctness
   - Verifies mutated node state survives database reopen after create, update,
     remove, and delete operations.

Verification:

- `cargo test -p neo4r-db --test data_correctness --quiet`

# SDK Work Plan

Requested scope: prepare both a Rust SDK and a Python SDK, using a separated
shared codec instead of duplicating server wire protocol logic.

Implemented:

1. Shared protocol crate
   - Added `crates/neo4r-protocol`.
   - Moved the native frame codec surface into a reusable crate.
   - Added shared query payload, query row, row response, cursor response,
     property/value, and escaping helpers.
   - Updated `neo4r-server` to use the shared frame types and delegate public
     query payload/row codec functions to `neo4r-protocol`.

2. Rust SDK
   - Added `crates/neo4r-client`.
   - Implemented blocking `Client::connect`, `ping`, `close`, `query`,
     `query_with_params`, `execute`, `execute_with_params`, `command`,
     `profile`, `statistics`, `storage_status`, and `metadata_log`.
   - Added cursor fetch/close handling so query results are collected across
     native result pages.
   - Added a server-backed SDK integration test.
   - Added `crates/neo4r-client/examples/basic_usage.rs` for a runnable Rust
     SDK example.

3. Python SDK
   - Added `sdks/python/neo4r_client`.
   - Implemented blocking native frame IO, query payload encoding, result row
     decoding, cursor response parsing, and `Client` helpers mirroring the Rust
     SDK's initial surface.
   - Added standard-library `unittest` protocol fixture tests.

Verification:

- `cargo fmt --all`
- `cargo test -p neo4r-protocol -p neo4r-client --quiet`
- `PYTHONPATH=sdks/python python3 -m unittest discover -s sdks/python/tests -v`

# Web Console Work Plan

Requested scope: expose a browser-accessible page when the server starts so the
graph can be inspected in 3D.

Implemented:

1. Web listener
   - Added `neo4r-server --web-bind ADDR`.
   - Added `TcpBackend::serve_web_addr`, `serve_web_listener`,
     `serve_web_listener_once`, and `handle_web_stream`.
   - Preserved `--web-bind` in daemon child args.

2. HTTP JSON API
   - `GET /api/graph?limit=N` returns nodes and relationships as JSON.
   - `POST /api/query` executes a Cypher query from a JSON body.
   - `GET /api/statistics`, `GET /api/storage`, and `GET /api/metadata-log`
     expose management responses.

3. 3D graph viewer
   - Served a static browser console at `/`.
   - Uses Three.js to render nodes and relationships in a 3D scene.
   - Supports refresh, query execution, storage/statistics panels, click
     selection, drag rotation, and wheel zoom.

4. Tests
   - Added server test coverage for HTML serving, graph API JSON, node data, and
     relationship type output.

Verification:

- `cargo fmt --all`
- `cargo test -p neo4r-server --quiet`

# Engine Hardening Work Plan

Requested scope: implement engine-hardening improvements 1 through 8 in order.

1. Query execution operator tree and PROFILE operator metrics
   - Add an explainable operator tree for the current supported query shapes.
   - Surface per-operator estimated rows, actual rows, and elapsed time in profile output.
   - Implemented `QueryOperatorProfile` and included operator summaries in
     public and native `PROFILE` output.

2. Persistent statistics catalog
   - Persist statistics under the data directory.
   - Refresh and save statistics after write application and expose the persisted view.
   - Implemented `StatisticsCatalogStore` under `cluster/statistics-catalog.txt`
     and refresh it after committed write application.

3. Real cost-based optimizer increment
   - Use persisted statistics, selectivity estimates, and remote shard penalties in
     query plan cost and row estimates.
   - Implemented statistics-backed row and cost estimates in distributed query
     planning and profile output.

4. Storage maintenance
   - Add checkpoint-aware WAL pruning metadata and a backend compaction hook result.
   - Keep maintenance safe for the existing storage primitives.
   - Implemented WAL prune-position reporting for checkpoint and compaction
     maintenance results.

5. Snapshot/index cache
   - Add a snapshot/version-aware index lookup cache surface with hit/miss counters.
   - Invalidate cache on writes.
   - Implemented a routing-version-aware index lookup cache surface and exposed
     read/index cache counters through profile and storage status.

6. Metadata consensus/log
   - Add a durable metadata operation log for membership/routing/rebalance authority
     operations.
   - Treat local authority changes as metadata-log committed operations.
   - Implemented `MetadataOperationLogStore` under `cluster/metadata-log.txt`,
     appended authority/membership/routing/rebalance operations, and exposed
     `METADATA_LOG`.

7. Recovery and chaos tests
   - Add restart/recovery tests for persisted statistics, metadata log, rebalance
     execution, checkpoint, and cache invalidation behavior.
   - Added restart coverage for persisted statistics and metadata operations,
     alongside the existing rebalance, checkpoint, and cache invalidation tests.

8. Native protocol structured management response
   - Add native command variants/responses for profile, storage status, statistics,
     and cluster management status where the current native protocol supports command
     dispatch.
   - Implemented native structured management responses for profile, storage
     status, statistics, metadata log, and cluster management status.

Verification target:

- `cargo fmt --all`
- `cargo test --workspace --no-run`
- `cargo test --workspace`

# Performance Work Plan

Requested scope: implement performance improvements 1 through 7 in order.

1. Query metrics and PROFILE
   - Add query execution metrics for planning/execution elapsed time, row count,
     access path, routing, and remote shard participation.
   - Add public and protocol-level PROFILE support.
   - Implemented `QueryProfile`, `QueryMetrics`, and `PROFILE`.

2. Storage status
   - Expose data directory, WAL/checkpoint/commit/routing/membership metadata file
     counts and sizes.
   - Add `STORAGE_STATUS`.
   - Implemented `StorageStatus` and `STORAGE_STATUS`.

3. Statistics catalog
   - Add an in-memory statistics view with node count, relationship count, label
     counts, relationship type counts, and index catalog size.
   - Add a protocol command to inspect statistics.
   - Implemented exact local `StatisticsCatalog` and `STATISTICS`.

4. Cost-based optimizer skeleton
   - Add simple deterministic cost estimates for existing access paths.
   - Surface estimated cost in plan/profile output.
   - Implemented deterministic access-path cost and row estimates in
     `DistributedQueryPlan`.

5. Read path cache
   - Add a small snapshot-safe point lookup cache for node/relationship reads.
   - Track cache hits/misses in storage/query status.
   - Implemented node/relationship point lookup cache with hit/miss counters.

6. Compaction and checkpoint controls
   - Add explicit checkpoint command.
   - Add a compaction control command that is safe for current storage primitives.
   - Implemented `CHECKPOINT_NOW` and safe `COMPACT_STORAGE` maintenance hook.

7. Distributed query optimizer
   - Extend distributed query planning with remote shard count and a cost-oriented
     route summary.
   - Surface distributed estimates through EXPLAIN/PROFILE.
   - Implemented remote shard count and remote cost penalty in plan/profile output.

Verification target:

- `cargo fmt --all`
- `cargo test --workspace --no-run`
- `cargo test --workspace`
