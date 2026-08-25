#!/usr/bin/env bash
set -euo pipefail
for container in s3-oci-governance-worker s3-oci-transfer-worker s3-oci-app s3-oci-simulator s3-oci-postgres; do
  podman stop -t 30 --ignore "$container"
  podman rm -f --ignore "$container"
done
