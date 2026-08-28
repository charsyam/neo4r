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

1. Convert `neo4r-db/src/database.rs` include-backed files into a `database/`
   module tree with explicit facade exports.
2. Split `write_cypher_helpers.rs` into node mutation, relationship mutation,
   merge/upsert, and expression helper modules.
3. Split `db_write_schema.rs` into query read, write dispatch, schema/index
   catalog, and index status modules.
4. Split `db_cluster.rs` into membership, rebalance planning, rebalance
   execution, and metadata authority modules.
5. Split `server/backend/http_json_backup.rs` into HTTP parsing, JSON codec,
   query HTTP, admin HTTP, and backup HTTP modules.
6. Re-group numeric test shards by behavior after the module tree is stable.
