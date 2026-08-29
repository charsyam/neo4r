#!/usr/bin/env bash
set -euo pipefail

scripts/storage-atomicity.sh
scripts/ci-crash.sh
scripts/failure-injection.sh

if [[ "${NEO4R_RUN_CRASH_EXTENDED:-0}" == "1" ]]; then
  cargo test -p neo4r-db --test real_crash_harness --quiet
  cargo test -p neo4r-server restore_maintenance_drains_native_backend_writes --quiet
fi

echo "neo4r crash consistency gate passed"
