#!/usr/bin/env bash
set -euo pipefail

cargo test -p neo4r-server web_console_serves_index_and_graph_api --quiet
cargo test -p neo4r-server web_console_isolates_tenant_databases_and_scopes_tokens --quiet
printf 'security regression checks passed\n'
