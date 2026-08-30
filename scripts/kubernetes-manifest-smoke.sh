#!/usr/bin/env bash
set -euo pipefail

grep -q "kind: ConfigMap" packaging/kubernetes/neo4r-configmap.yml
grep -q "kind: Service" packaging/kubernetes/neo4r-service.yml
grep -q "kind: StatefulSet" packaging/kubernetes/neo4r-statefulset.yml
grep -q "/readyz" packaging/kubernetes/neo4r-statefulset.yml
grep -q "/healthz" packaging/kubernetes/neo4r-statefulset.yml
grep -q "wal_archive_dir:" packaging/kubernetes/neo4r-configmap.yml
echo "neo4r kubernetes manifest smoke passed"
