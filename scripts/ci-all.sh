#!/usr/bin/env bash
set -euo pipefail

scripts/ci-fast.sh
scripts/ci-correctness.sh
scripts/ci-crash.sh
scripts/ci-server.sh
scripts/ci-integration.sh
