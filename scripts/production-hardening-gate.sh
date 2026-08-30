#!/usr/bin/env bash
set -euo pipefail

disallowed=$(
  awk '
    /#\[cfg\(test\)\]/ { in_test = 1 }
    in_test == 0 && ($0 ~ /lock\(\)\.unwrap\(\)/ || $0 ~ /panic!\(/) {
      print FILENAME ":" FNR ":" $0
    }
  ' $(find crates -path '*/src/*.rs' -o -path '*/src/*/*.rs' | grep -v '/tests/' | grep -v '/examples/' | grep -v '/src/main/tests.rs') || true
)

if [[ -n "$disallowed" ]]; then
  echo "production hardening gate failed: panic or lock unwrap in non-test code" >&2
  echo "$disallowed" >&2
  exit 1
fi

echo "neo4r production hardening gate passed"
