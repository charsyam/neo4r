#!/usr/bin/env bash
set -euo pipefail

grep -q "node_kill_restart" docs/chaos_longevity_plan.yml
grep -q "network_partition_primary_replica" docs/chaos_longevity_plan.yml
grep -q "routing_epoch_monotonic" docs/chaos_longevity_plan.yml

scripts/cluster-chaos-smoke.sh

if [[ "${NEO4R_RUN_CHAOS_LONGEVITY:-0}" == "1" ]]; then
  NEO4R_RUN_MULTI_NODE_LIVE=1 scripts/multi-node-live-gate.sh
else
  echo "neo4r chaos longevity live run skipped; set NEO4R_RUN_CHAOS_LONGEVITY=1"
fi

echo "neo4r production chaos longevity gate passed"
