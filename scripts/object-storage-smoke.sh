#!/usr/bin/env bash
set -euo pipefail

grep -q "provider: s3-compatible" docs/object_storage_archive.yml
grep -q "object-storage-smoke-opt-in" docs/object_storage_archive.yml

if [[ "${NEO4R_RUN_OBJECT_STORAGE_SMOKE:-0}" != "1" ]]; then
  echo "neo4r object storage smoke skipped; set NEO4R_RUN_OBJECT_STORAGE_SMOKE=1 with S3-compatible env"
  exit 0
fi

: "${NEO4R_S3_ENDPOINT:?missing NEO4R_S3_ENDPOINT}"
: "${NEO4R_S3_BUCKET:?missing NEO4R_S3_BUCKET}"
: "${NEO4R_S3_ACCESS_KEY:?missing NEO4R_S3_ACCESS_KEY}"
: "${NEO4R_S3_SECRET_KEY:?missing NEO4R_S3_SECRET_KEY}"

echo "neo4r object storage smoke env validated for $NEO4R_S3_ENDPOINT/$NEO4R_S3_BUCKET"
