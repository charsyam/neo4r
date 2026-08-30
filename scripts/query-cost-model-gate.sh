#!/usr/bin/env bash
set -euo pipefail

grep -q "node_index_seek" docs/query_cost_model.yml
grep -q "statistics-calibration-smoke" docs/query_cost_model.yml
grep -q "cost_model_version: 3" crates/neo4r-db/src/database/tests/tcp_replication.rs
grep -q "query:" docs/query_regression_corpus.yml
scripts/query-plan-golden.sh
scripts/query-statistics-maintenance-gate.sh
scripts/bench-thresholds.sh
echo "neo4r query cost model gate passed"
