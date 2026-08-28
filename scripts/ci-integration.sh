#!/usr/bin/env bash
set -euo pipefail

cargo test -p neo4r-server --test multi_process_cluster_smoke
scripts/sdk-compat.sh

if [[ "${NEO4R_RUN_CLUSTER_SMOKE:-0}" == "1" ]]; then
  scripts/multi_process_cluster_smoke.sh
fi

if [[ "${NEO4R_RUN_CLUSTER_CHAOS:-0}" == "1" ]]; then
  scripts/cluster-chaos-smoke.sh
fi
