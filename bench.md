# Neo4r Benchmark Notes

Date: 2026-08-31

All numbers below were measured on the local development machine. They are
single-run observations, not a statistically stable benchmark suite.

## Neo4j Docker Baseline

Environment:

- Image: `neo4j:community-ubi10`
- Neo4j version: `2026.07.1`
- Auth disabled
- Heap: `1G`
- Pagecache: `512M`

| Workload | Ops | Elapsed ms | Ops/sec |
| --- | ---: | ---: | ---: |
| `create_nodes_batch1` | 5,000 | 1,585.893 | 3,152.8 |
| `create_nodes_batch_with_index` | 5,000 | 1,576.375 | 3,171.8 |
| `basic_perf_like_batch` | 10,499 | 1,751.560 | 5,994.1 |
| `single_statement_basic_like` | 2,099 | 12,847.096 | 163.4 |
| `single_statement_indexed_reads` | 2,000 | 7,682.570 | 260.3 |

## Neo4r Local Batch

Command:

```bash
cargo run -p neo4r-db --example basic_perf --release
```

| Workload | Ops | Elapsed ms | Ops/sec |
| --- | ---: | ---: | ---: |
| `create_nodes` | 5,000 | 20,507.434 | 243.8 |
| `create_relationships` | 4,999 | 30,693.009 | 162.9 |
| `set_node_property` | 500 | 3,557.996 | 140.5 |
| `total` | 10,499 | 56,757.465 | 185.0 |
| `batch_create_nodes` | 5,000 | 259.928 | 19,236.1 |
| `batch_create_relationships` | 4,999 | 265.767 | 18,809.7 |
| `batch_set_node_property` | 500 | 99.755 | 5,012.3 |
| `batch_read_queries` | 3 | 0.479 | 6,262.4 |
| `batch_total` | 10,499 | 666.056 | 15,762.9 |

## Neo4r Replicated Batch

The replicated benchmark uses an in-process primary/replica pair. It includes
replication publish, replica append/apply, ACK handling, primary commit/apply,
and a final read from the replica to verify visibility.

Default replicated benchmark count is capped by `NEO4R_REPLICATION_PERF_NODES`
and was 2,000 nodes for this run.

| Workload | Ops | Elapsed ms | Ops/sec |
| --- | ---: | ---: | ---: |
| `replicated_batch_create_nodes_e2e` | 2,000 | 7,986.622 | 250.4 |
| `replicated_batch_create_relationships_e2e` | 1,999 | 11,404.190 | 175.3 |
| `replicated_batch_set_node_property_e2e` | 200 | 1,255.951 | 159.2 |
| `replicated_batch_replica_visible_reads` | 3 | 0.408 | 7,357.5 |
| `replicated_batch_total_e2e` | 4,199 | 20,647.171 | 203.4 |

Committed indexes:

```text
primary=[263, 263, 263, 263, 263, 263, 263, 263, 262, 262, 262, 262, 262, 262, 262, 261]
replica=[263, 263, 263, 263, 263, 263, 263, 263, 262, 262, 262, 262, 262, 262, 262, 261]
```

## Interpretation

- Local neo4r batch write is faster than the local Neo4j Docker batch baseline
  for this specific workload: `15,762.9 ops/sec` vs `5,994.1 ops/sec`.
- Replicated neo4r batch e2e is much slower: `203.4 ops/sec`.
- The current bottleneck is not local storage batch apply; it is the
  synchronous replication receiver path and ACK round trip.
