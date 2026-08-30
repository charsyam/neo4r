#!/usr/bin/env bash
set -euo pipefail

cargo run -p neo4r-db --example basic_perf
cargo test -p neo4r-db --test performance_smoke -- --nocapture
