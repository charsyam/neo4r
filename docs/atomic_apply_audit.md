# Atomic Apply Audit

Logical graph mutations must materialize into RocksDB atomically. A command
that writes a record and any derived indexes must use one batch boundary.

Covered invariants:

- relationship body, outgoing adjacency, typed outgoing adjacency, incoming
  adjacency, and typed incoming adjacency are one batch.
- node property updates remove old secondary index keys, write the node body,
  and add new secondary index keys in one batch.
- node label updates and boundary node copies update body/index keys together.

Operator checks:

```text
VERIFY_INVARIANTS
REPAIR_INVARIANTS
```

CI checks:

```bash
scripts/ci-crash.sh
cargo test -p neo4r-storage graph_store::tests::relationship_create_uses_one_atomic_write_batch
cargo test -p neo4r-storage graph_store::tests::failed_relationship_write_batch_leaves_no_partial_indexes
```

Future command additions must include a failing batch test when the command
touches more than one RocksDB key.
