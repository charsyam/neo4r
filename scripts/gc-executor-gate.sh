#!/usr/bin/env bash
set -euo pipefail

grep -q "compute_safe_watermark" docs/gc_executor_contract.yml
grep -q "GC_DELETE" docs/gc_executor_contract.yml
grep -q "never_delete_pending_restore_inputs" docs/gc_executor_contract.yml

scripts/safe-watermark-gc-gate.sh
echo "neo4r gc executor gate passed"
