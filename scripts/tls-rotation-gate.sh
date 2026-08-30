#!/usr/bin/env bash
set -euo pipefail

grep -q "Transfer shard leadership" docs/tls_rotation_runbook.md
grep -q "certificate expires" docs/tls_rotation_runbook.md
grep -q "neo4r_tls_cert_not_after_seconds" docs/tls_cert_inventory.yml
grep -q "web_tls_require_client_auth: true" packaging/server.production.yml
grep -Eq "replication_tls_mode: (external|required)" packaging/server.production.yml

cargo test -p neo4r-server production_check_accepts_web_tls_required_when_cert_and_key_are_configured --quiet
cargo test -p neo4r-server production_check_accepts_replication_tls_required_when_configured --quiet
cargo test -p neo4r-client parses_native_tls_cli_args --quiet

echo "neo4r tls rotation gate passed"
