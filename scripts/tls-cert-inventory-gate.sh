#!/usr/bin/env bash
set -euo pipefail

grep -q "neo4r_tls_cert_not_after_seconds" docs/tls_cert_inventory.yml
grep -q "build_new_acceptor_before_swap" docs/tls_cert_inventory.yml

cargo test -p neo4r-server tls_cert_inventory_reports_existing_cert_files --quiet
scripts/tls-rotation-gate.sh
echo "neo4r tls cert inventory gate passed"
