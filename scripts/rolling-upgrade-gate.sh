#!/usr/bin/env bash
set -euo pipefail

grep -q "mixed-version-protocol-compat" docs/rolling_upgrade_manifest.yml
scripts/protocol-compat.sh
scripts/protocol-matrix.sh
echo "neo4r rolling upgrade gate passed"
