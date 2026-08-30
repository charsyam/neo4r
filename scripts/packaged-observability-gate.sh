#!/usr/bin/env bash
set -euo pipefail

grep -q "kind: ConfigMap" packaging/grafana-dashboard.yml
grep -q "kind: PrometheusRule" packaging/grafana-dashboard.yml
grep -q "Neo4r Production Overview" packaging/grafana-dashboard.yml
grep -q "Neo4rStorageRepairFailures" packaging/grafana-dashboard.yml

echo "neo4r packaged observability gate passed"
