#!/usr/bin/env bash
set -euo pipefail

grep -q "mixed-version-protocol-compat" docs/rolling_upgrade_manifest.yml
grep -q "mixed-version-snapshot-fetch-compat" docs/rolling_upgrade_manifest.yml
grep -q "learner-membership-metadata-compatible" docs/rolling_upgrade_manifest.yml
grep -q "upgrade-smoke-from-current-release" docs/rolling_upgrade_manifest.yml
scripts/protocol-compat.sh
scripts/protocol-matrix.sh
cargo test -p neo4r-db tcp_snapshot_fetch_serves_primary_snapshot_for_node_catch_up --quiet
cargo test -p neo4r-db learner_does_not_vote_until_promoted --quiet
scripts/upgrade-smoke.sh
echo "neo4r rolling upgrade gate passed"
