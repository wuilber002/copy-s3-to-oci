#!/usr/bin/env bash
set -euo pipefail

request=/var/lib/s3-oci-migration/mode-control/request
result=/var/lib/s3-oci-migration/mode-control/result
[[ -s "$request" ]] || exit 0
target="$(tr -d '[:space:]' <"$request" | tr '[:lower:]' '[:upper:]')"
mv "$request" "$request.processing"
if output="$(/usr/local/sbin/raijin-mode "$target" 2>&1)"; then
  printf 'SUCCEEDED\t%s\t%s\n' "$(date --iso-8601=seconds)" "$output" >"$result.tmp"
else
  status=$?
  printf 'FAILED\t%s\t%s\n' "$(date --iso-8601=seconds)" "$output" >"$result.tmp"
  mv "$result.tmp" "$result"
  rm -f "$request.processing"
  exit "$status"
fi
mv "$result.tmp" "$result"
rm -f "$request.processing"
