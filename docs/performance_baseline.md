# Performance Baseline

Performance checks are intentionally split into smoke and release paths.

## Smoke

```bash
scripts/bench-smoke.sh
```

The smoke path verifies that query, write, index, vector, cursor, and reopen
paths execute within the existing correctness-oriented performance tests.

## Release Regression

```bash
NEO4R_RUN_BENCH_REGRESSION=1 scripts/bench-regression.sh
```

Release regression runs the same benchmark entrypoint under the release gate.
Store each release's observed values with the git SHA and compare:

- node write throughput
- relationship write throughput
- indexed lookup latency
- vector KNN latency
- cursor page latency
- reopen/replay latency

Any threshold tightening should happen in code and in this document together.

`docs/performance_thresholds.txt` is the machine-readable threshold registry
checked by `scripts/bench-thresholds.sh` during the release gate. CI artifacts
include `target/neo4r-release/metadata.txt` so a release run can be tied back
to the git SHA, result contract, and benchmark threshold file used for that
run.
