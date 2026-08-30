#!/usr/bin/env bash
set -euo pipefail

grep -q "health-probe" docs/packaging_readiness.yml
grep -q "kubernetes-manifest-smoke" docs/packaging_readiness.yml
grep -q "ExecStartPre" packaging/neo4r-server.service
grep -q "web_tls_cert:" packaging/server.production.yml
scripts/kubernetes-manifest-smoke.sh
scripts/upgrade-smoke.sh
scripts/production-preflight.sh packaging/server.production.yml
echo "neo4r packaging readiness gate passed"
