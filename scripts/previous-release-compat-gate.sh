#!/usr/bin/env bash
set -euo pipefail

grep -q "NEO4R_PREVIOUS_RELEASE_DIR" docs/previous_release_fixture.yml

if [[ -n "${NEO4R_PREVIOUS_RELEASE_DIR:-}" ]]; then
  test -d "$NEO4R_PREVIOUS_RELEASE_DIR"
  test -s "$NEO4R_PREVIOUS_RELEASE_DIR/metadata.txt"
else
  grep -q "current-fixture-self-check" docs/previous_release_fixture.yml
fi

scripts/compatibility-matrix-gate.sh
echo "neo4r previous release compatibility gate passed"
