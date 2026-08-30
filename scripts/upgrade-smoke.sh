#!/usr/bin/env bash
set -euo pipefail

grep -q "upgrade-smoke-from-current-release" docs/rolling_upgrade_manifest.yml
grep -q "leader-transfer-before-drain" docs/rolling_upgrade_manifest.yml
scripts/protocol-compat.sh
scripts/protocol-matrix.sh
scripts/production-preflight.sh packaging/server.production.yml
echo "neo4r upgrade smoke passed"
