#!/usr/bin/env bash
set -euo pipefail

cargo fmt --check
scripts/check-file-lines.sh
cargo test -p neo4r-core -p neo4r-query -p neo4r-protocol -p neo4r-client
git diff --check
