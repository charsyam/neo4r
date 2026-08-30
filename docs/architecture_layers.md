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

The main database, query, server backend, protocol, and replication seams now
use Rust module boundaries instead of text-level composition:

- `neo4r-query::cypher`: facade plus `parse`, `binding`, and `execute`
  submodules.
- `neo4r-db::database`: public DB facade plus focused config, DB, cluster,
  schema, Cypher write, and helper submodules.
- `neo4r-db::replication`: replication facade plus TCP request and response
  codec modules.
- `neo4r-server::protocol`: request/response model facade plus parse helper,
  format, row codec, and response writing modules. DB command execution lives
  in the backend layer.
- `neo4r-server::backend`: nested backend facade plus feature modules for HTTP
  admin, web query/backup, native execution, worker pools, transaction
  handling, distributed query, gossip discovery, remote transactions,
  replication admin, shared backend state, and web assets.
- DB/server/protocol tests: behavior-named real Rust modules with shared test
  fixtures imported through dedicated support modules.

The facade imports only the sibling functions and types it needs. Sibling module
contracts are kept at `pub(super)` or `pub(crate)` depending on whether the
consumer is a direct sibling or another backend submodule.

## Guardrails

- `scripts/check-architecture.sh` rejects `include!` in the DB and server source
  trees.
- `scripts/check-file-lines.sh` keeps Rust, Python, shell, and markdown files
  under the configured line limit.
- `scripts/ci-fast.sh` runs the architecture guard before fast crate tests.

## Next Formalization Targets

1. Continue splitting the remaining 800-1000 line DB files by store, planning,
   metadata persistence, and validation responsibilities before they grow.
2. Split the largest server HTTP modules into route, authorization, and
   response-building submodules.
3. Continue replacing broad parent test imports with explicit support imports.
