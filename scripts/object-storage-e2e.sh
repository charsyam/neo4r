#!/usr/bin/env bash
set -euo pipefail

grep -q "restore_apply_with_confirmation" docs/object_storage_e2e.yml
grep -q "verify_checksum_and_query_results" docs/object_storage_e2e.yml

if [[ "${NEO4R_RUN_OBJECT_STORAGE_E2E:-0}" != "1" ]]; then
  echo "neo4r object storage E2E skipped; set NEO4R_RUN_OBJECT_STORAGE_E2E=1 with S3-compatible env"
  exit 0
fi

for name in NEO4R_S3_ENDPOINT NEO4R_S3_BUCKET NEO4R_S3_ACCESS_KEY NEO4R_S3_SECRET_KEY; do
  if [[ -z "${!name:-}" ]]; then
    echo "missing required object storage env: $name" >&2
    exit 1
  fi
done

scripts/object-storage-smoke.sh
echo "neo4r object storage E2E prerequisites passed"
