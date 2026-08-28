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


# Archived Work Logs

- [2026-08 part 1](docs/worklog/2026-08-part1.md)
- [2026-08 part 2](docs/worklog/2026-08-part2.md)
