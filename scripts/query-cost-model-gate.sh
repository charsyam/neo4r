#!/usr/bin/env bash
set -euo pipefail

grep -q "node_index_seek" docs/query_cost_model.yml
grep -q "query:" docs/query_regression_corpus.yml
scripts/query-plan-golden.sh
scripts/bench-thresholds.sh
echo "neo4r query cost model gate passed"
