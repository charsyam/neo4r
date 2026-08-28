#!/usr/bin/env bash
set -euo pipefail

LIMIT="${NEO4R_FILE_LINE_LIMIT:-1000}"
failed=0

while IFS= read -r -d '' file; do
  lines="$(wc -l < "$file")"
  if (( lines > LIMIT )); then
    printf 'file too large: %s lines=%s limit=%s\n' "$file" "$lines" "$LIMIT" >&2
    failed=1
  fi
done < <(find crates sdks scripts docs -type f \( -name '*.rs' -o -name '*.py' -o -name '*.sh' -o -name '*.md' \) -print0)

exit "$failed"
