#!/usr/bin/env bash
set -euo pipefail

cargo test -p neo4r-db snapshot_chunk --quiet
cargo test -p neo4r-db tcp_snapshot_fetch_resumes_chunked_snapshot --quiet
echo "snapshot chunk checks passed"
