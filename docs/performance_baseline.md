# Performance Baseline

Performance checks are intentionally split into smoke and release paths.

## Smoke

```bash
scripts/bench-smoke.sh
```

The smoke path verifies that query, write, batch write/read, update/delete
mutation mix, index, vector, cursor, Raft append batching, and reopen paths execute within the
existing correctness-oriented performance tests.

Smoke thresholds can be adjusted without changing source:

- `NEO4R_PERF_SMOKE_MAX_MS`: per-test end-to-end smoke budget, default 30000.
- `NEO4R_PERF_QUERY_P50_MS`: repeated indexed query p50 budget, default 25.
- `NEO4R_PERF_QUERY_P99_MS`: repeated indexed query p99 budget, default 250.
- `NEO4R_PERF_BATCH_WRITE_MS`: batch write smoke budget, default 30000.
- `NEO4R_PERF_BATCH_READ_MS`: batch read smoke budget, default 1000.
- `NEO4R_PERF_REPLICATED_BATCH_WRITE_MS`: replicated batch write end-to-end
  smoke budget, default 30000.
- `NEO4R_PERF_REPLICATED_BATCH_VISIBLE_READ_MS`: replica visibility read smoke
  budget, default 1000.
- `NEO4R_PERF_REPLICATION_APPEND_P99_MS`: Raft append batch budget, default 1000.

## Release Regression

```bash
NEO4R_RUN_BENCH_REGRESSION=1 scripts/bench-regression.sh
```

Release regression runs the same benchmark entrypoint under the release gate.
Store each release's observed values with the git SHA and compare:

- node write throughput
- batch node write throughput
- relationship write throughput
- indexed lookup latency
- batch read query latency
- replicated batch write end-to-end latency
- replica visibility read latency
- mutation/index maintenance latency
- vector KNN latency
- cursor page latency
- Raft append batch latency
- reopen/replay latency

Any threshold tightening should happen in code and in this document together.

`docs/performance_thresholds.txt` is the machine-readable threshold registry
checked by `scripts/bench-thresholds.sh` during the release gate. CI artifacts
include `target/neo4r-release/metadata.txt` so a release run can be tied back
to the git SHA, result contract, and benchmark threshold file used for that
run.
