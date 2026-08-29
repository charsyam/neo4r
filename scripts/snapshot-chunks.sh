#!/usr/bin/env bash
set -euo pipefail

cargo test -p neo4r-db snapshot_chunk --quiet
echo "snapshot chunk checks passed"
