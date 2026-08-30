#!/usr/bin/env bash
set -euo pipefail

grep -q "tls-web-admin" docs/security_hardening_contract.yml
grep -q "rbac-role-boundary" docs/security_hardening_contract.yml
grep -q "WebAction" docs/rbac_policy.md
grep -q "rbac.grant" docs/rbac_policy.md
grep -q "rbac-grant-revoke-audit" docs/security_hardening_contract.yml
grep -q "operational-confirmation-token" docs/security_hardening_contract.yml
grep -q "web_role_allows_action" crates/neo4r-server/src/backend/rbac.rs
grep -q "grant_database_role" crates/neo4r-server/src/web_auth.rs
grep -q "web_tls_mode: required" packaging/server.production.yml
cargo test -p neo4r-server web_roles_enforce_reader_writer_admin_boundaries --quiet
cargo test -p neo4r-server rbac_policy_maps_actions_to_minimum_roles --quiet
cargo test -p neo4r-server rbac_grant_and_revoke_role_record_audit_reason --quiet
cargo test -p neo4r-db bootstrap_safety_topology_backup_and_chaos_contracts_are_enforced --quiet
scripts/security-regression.sh
scripts/session-security.sh
echo "neo4r security hardening gate passed"
