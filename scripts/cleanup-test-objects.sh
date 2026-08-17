#!/usr/bin/env bash
# Remove a deliberately supplied list of non-production test objects.
# The script defaults to a read-only plan and refuses wildcard/prefix cleanup.
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: cleanup-test-objects.sh --keys-file FILE --s3-bucket BUCKET --oci-bucket BUCKET --oci-namespace NAMESPACE [--apply --confirm DELETE_TEST_OBJECTS] [--keep-source]

The keys file contains one exact object key per line (or a CSV whose first
column is the key).  Without --apply, this script prints the deletion plan.
--apply requires the literal --confirm DELETE_TEST_OBJECTS acknowledgement.
EOF
}

keys_file='' s3_bucket='' oci_bucket='' namespace='' apply=false keep_source=false confirm=''
while (($#)); do
  case "$1" in
    --keys-file) keys_file=${2:?}; shift 2;;
    --s3-bucket) s3_bucket=${2:?}; shift 2;;
    --oci-bucket) oci_bucket=${2:?}; shift 2;;
    --oci-namespace) namespace=${2:?}; shift 2;;
    --keep-source) keep_source=true; shift;;
    --apply) apply=true; shift;;
    --confirm) confirm=${2:?}; shift 2;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 2;;
  esac
done

[[ -r "$keys_file" && -n "$s3_bucket" && -n "$oci_bucket" && -n "$namespace" ]] || { usage >&2; exit 2; }
mapfile -t keys < <(awk -F, 'NF && $1 !~ /^([[:space:]]*#|[[:space:]]*key[[:space:]]*$)/ { gsub(/^"|"$/, "", $1); print $1 }' "$keys_file")
((${#keys[@]})) || { echo "No exact keys found in $keys_file" >&2; exit 2; }
for key in "${keys[@]}"; do
  [[ -n "$key" && "$key" != *'*'* && "$key" != */ ]] || { echo "Refusing non-exact key: '$key'" >&2; exit 2; }
done

echo "Cleanup plan (${#keys[@]} exact key(s))"
echo "  OCI destination: oci://${oci_bucket}/"
[[ "$keep_source" == true ]] || echo "  AWS source:      s3://${s3_bucket}/"
printf '  - %s\n' "${keys[@]}"
if [[ "$apply" != true ]]; then
  echo "Plan only. Re-run with --apply --confirm DELETE_TEST_OBJECTS after review."
  exit 0
fi
[[ "$confirm" == DELETE_TEST_OBJECTS ]] || { echo "Explicit confirmation is required." >&2; exit 2; }
command -v oci >/dev/null || { echo "OCI CLI is required." >&2; exit 2; }
[[ "$keep_source" == true ]] || command -v aws >/dev/null || { echo "AWS CLI is required to remove source objects." >&2; exit 2; }

for key in "${keys[@]}"; do
  echo "Deleting OCI destination: $key"
  oci os object delete --namespace-name "$namespace" --bucket-name "$oci_bucket" --name "$key" --force >/dev/null
  if [[ "$keep_source" != true ]]; then
    echo "Deleting AWS source: $key"
    aws s3api delete-object --bucket "$s3_bucket" --key "$key" >/dev/null
  fi
done
echo "Cleanup completed for ${#keys[@]} exact key(s)."
