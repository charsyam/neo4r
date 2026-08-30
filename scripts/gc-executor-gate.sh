#!/usr/bin/env bash
set -euo pipefail

grep -q "compute_safe_watermark" docs/gc_executor_contract.yml
grep -q "GC_DELETE" docs/gc_executor_contract.yml
grep -q "never_delete_pending_restore_inputs" docs/gc_executor_contract.yml

cargo test -p neo4r-server gc_executor_respects_dry_run_and_pending_restore_guard --quiet
scripts/safe-watermark-gc-gate.sh
echo "neo4r gc executor gate passed"
