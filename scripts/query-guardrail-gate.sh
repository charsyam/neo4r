#!/usr/bin/env bash
set -euo pipefail

grep -q "operator_memory_budget_bytes" docs/query_guardrails.yml
grep -q "spill_when_budget_exceeded" docs/query_guardrails.yml
grep -q "cancellation_releases_tenant_concurrency" docs/query_guardrails.yml

cargo test -p neo4r-server web_query_enforces_tenant_result_row_quota --quiet
cargo test -p neo4r-server native_query_holds_tenant_concurrency_quota_until_cursor_is_exhausted --quiet
cargo test -p neo4r-server native_cancel_reports_missed_request_when_not_pending --quiet

echo "neo4r query guardrail gate passed"
