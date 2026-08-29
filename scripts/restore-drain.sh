#!/usr/bin/env bash
set -euo pipefail

cargo test -p neo4r-server restore_maintenance_drains_native_backend_writes --quiet
echo "restore drain checks passed"
