#!/usr/bin/env bash
set -euo pipefail

backup_root=/var/lib/s3-oci-migration/backups
timestamp=$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$backup_root"

podman exec s3-oci-postgres pg_dump -U migration -d migration --format=custom \
  >"$backup_root/migration-$timestamp.dump"

# Keep fourteen daily logical backups locally. Boot-volume backup remains the
# recovery layer for the complete machine and its PostgreSQL WAL directory.
find "$backup_root" -type f -name 'migration-*.dump' -mtime +14 -delete
