#!/usr/bin/env bash
set -euo pipefail

cargo test -p neo4r-db --test data_correctness
cargo test -p neo4r-db --test performance_smoke
