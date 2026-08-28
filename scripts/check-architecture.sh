#!/usr/bin/env bash
set -euo pipefail

failed=0

if rg -n 'include!' crates/neo4r-db/src crates/neo4r-server/src; then
  printf 'architecture violation: use real Rust modules instead of include! in db/server src\n' >&2
  failed=1
fi

if ! scripts/check-file-lines.sh; then
  failed=1
fi

if [ ! -f crates/neo4r-server/src/backend/MODULE_TREE.md ]; then
  printf 'architecture violation: document server backend module ownership in crates/neo4r-server/src/backend/MODULE_TREE.md\n' >&2
  failed=1
fi

exit "$failed"
