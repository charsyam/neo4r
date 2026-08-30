#!/usr/bin/env bash
set -euo pipefail

grep -q "explicit deny" docs/rbac_policy.md
grep -q "deny precedence" docs/rbac_policy.md
grep -q "rbac.grant" docs/rbac_policy.md
grep -q "rbac.revoke" docs/rbac_policy.md

cargo test -p neo4r-server rbac_grant_and_revoke_role_record_audit_reason --quiet

echo "neo4r explicit rbac policy gate passed"
