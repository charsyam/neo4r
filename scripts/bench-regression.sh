#!/usr/bin/env bash
set -euo pipefail

cargo test -p neo4r-db --test performance_smoke
cargo test -p neo4r-db --test data_correctness
cargo test -p neo4r-server native_read_write_transaction_commits_multi_shard

if [[ "${NEO4R_RUN_BENCH_REGRESSION:-0}" == "1" ]]; then
  cargo run -p neo4r-db --example basic_perf --release
else
  printf 'release benchmark skipped; set NEO4R_RUN_BENCH_REGRESSION=1 to run release perf path\n'
fi
