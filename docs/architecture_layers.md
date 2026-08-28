# Architecture Layers

This project should prefer Rust module boundaries over text-level `include!`
splits. Large files may be split mechanically first, but the follow-up state
should make dependencies explicit through `mod`, `pub(super)`, and narrow facade
exports.

## Layer Order

The intended dependency direction is:

```text
server -> db -> query -> storage -> core
       -> protocol
```

`core` owns graph domain types and deterministic commands.

`storage` owns durable key-value, WAL, snapshot, index materialization, and
invariant repair primitives. It should not depend on `query`, `db`, or `server`.

`query` owns parsing, semantic validation, logical planning, physical planning,
and execution over the `GraphRead` trait. It should not depend on `db` or
`server`.

`db` owns shard routing, Raft/replication coordination, transaction application,
catalogs, backup/restore orchestration, and read consistency policy.

`server` owns wire protocols, HTTP/native request handling, tenant selection,
authentication UI/API, worker pools, and remote cluster calls.

`protocol` owns stable native frame and payload compatibility types.

## Module Policy

- Use `mod` files for feature boundaries.
- Use `pub(super)` for sibling-only contracts.
- Keep public exports in facade modules small and intentional.
- Keep parser/codec helpers private to their protocol or query submodule.
- Avoid new top-level `include!` splits except as a temporary migration step for
  very large files.
- Tests should be grouped by behavior, not only by numeric file shards, once the
  implementation boundary is stable.

## Current Formal Boundary

`neo4r-query::cypher` is the first formalized boundary:

- `cypher.rs`: public facade, query AST/plan types, statement classification,
  semantic validation, plan construction, and `CypherEngine`.
- `cypher/parse.rs`: parser implementation and parser-local helpers.
- `cypher/binding.rs`: execution binding model for nodes and relationships.
- `cypher/execute.rs`: physical execution, cursors, projection, aggregation,
  ordering, distinct, and predicate evaluation.
- `cypher/tests.rs`: behavior tests through the facade.

The facade imports only the sibling functions and types it needs. Sibling module
contracts are `pub(super)`, not crate-public.

## Next Formalization Targets

1. Split `db_index_validation.rs`, `db_open_write.rs`, `metadata_types.rs`, and
   `db_maintenance_plan.rs` into narrower formal submodules.
2. Move `server/src/lib.rs` backend `include!` files into a real backend module
   tree.
3. Move `server/src/protocol.rs` parser/executor/codec include splits into
   formal protocol submodules.
4. Replace behavior-named test `include!` wrappers with real `mod` tests once
   shared fixtures are factored out.
