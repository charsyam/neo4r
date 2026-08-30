#!/usr/bin/env bash
set -euo pipefail

grep -q "workflow_dispatch" .github/workflows/production-live.yml
grep -q "minio/minio" .github/workflows/production-live.yml
grep -q "NEO4R_RUN_OBJECT_STORAGE_E2E" .github/workflows/production-live.yml
grep -q "NEO4R_RUN_CHAOS_LONGEVITY" .github/workflows/production-live.yml

echo "neo4r production live workflow gate passed"
