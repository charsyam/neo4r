#!/usr/bin/env bash
set -euo pipefail

NEO4R_RUN_CLUSTER_RESTART=1 scripts/multi_process_cluster_smoke.sh
