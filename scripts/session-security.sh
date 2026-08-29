#!/usr/bin/env bash
set -euo pipefail

cargo test -p neo4r-server web_session_csrf_and_logout_are_enforced --quiet
cargo test -p neo4r-server web_console_isolates_tenant_databases_and_scopes_tokens --quiet
echo "session security checks passed"
