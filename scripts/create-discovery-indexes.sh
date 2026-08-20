#!/usr/bin/env bash
set -euo pipefail

# Online, additive indexes for inventories with millions of objects. Keep this
# out of application startup: PostgreSQL's CONCURRENTLY form avoids blocking
# normal reads/writes, but can still take significant time and I/O.
container_name="${RAIJIN_POSTGRES_CONTAINER:-s3-oci-postgres}"

if ! podman container exists "$container_name"; then
  echo "PostgreSQL container '$container_name' was not found. Set RAIJIN_POSTGRES_CONTAINER." >&2
  exit 1
fi

run_index() {
  local index_sql="$1"
  podman exec "$container_name" psql -U migration -d migration -v ON_ERROR_STOP=1 -c "$index_sql"
}

run_index 'CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_objects_source_key_id ON objects (source_id, object_key, id)'
run_index 'CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_objects_source_state_key_id ON objects (source_id, state, object_key, id)'

echo 'Discovery indexes are ready.'
