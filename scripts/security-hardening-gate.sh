#!/usr/bin/env bash
set -euo pipefail

grep -q "tls-web-admin" docs/security_hardening_contract.yml
grep -q "web_tls_mode: required" packaging/server.production.yml
scripts/security-regression.sh
scripts/session-security.sh
echo "neo4r security hardening gate passed"
