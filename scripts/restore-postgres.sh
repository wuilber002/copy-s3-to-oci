#!/usr/bin/env bash
set -euo pipefail

# Deliberately explicit recovery tool.  It is never called by the platform and
# refuses ambiguous paths or an omitted confirmation because it replaces the
# durable control-plane state (inventory, tasks, waves and audit evidence).
backup_root=/var/lib/s3-oci-migration/backups
confirm=false
backup_file=''

usage() {
  cat <<'EOF'
Usage: s3-oci-restore-postgres --backup /var/lib/s3-oci-migration/backups/migration-<timestamp>.dump --confirm-restore

Restores one logical PostgreSQL backup into the Raijin control plane. The
current database is backed up immediately before replacement. This operation
does not alter S3 or OCI objects, but it can roll the Raijin task state back.
EOF
}

while (($#)); do
  case "$1" in
    --backup) backup_file=${2:-}; shift 2 ;;
    --confirm-restore) confirm=true; shift ;;
    --help|-h) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ "$confirm" == true && -n "$backup_file" ]] || { usage >&2; exit 2; }
[[ -f "$backup_file" ]] || { echo "Backup file not found: $backup_file" >&2; exit 2; }

backup_root_real=$(realpath "$backup_root")
backup_file_real=$(realpath "$backup_file")
[[ "$backup_file_real" == "$backup_root_real"/* ]] || {
  echo "Backup must be inside $backup_root_real" >&2
  exit 2
}

systemctl stop s3-oci-migration.service
podman start s3-oci-postgres >/dev/null

# Preserve the current state before a potentially irreversible logical restore.
/usr/local/sbin/s3-oci-backup-postgres
podman exec s3-oci-postgres pg_restore -U migration -d migration \
  --clean --if-exists --no-owner --exit-on-error "$backup_file_real"

systemctl start s3-oci-migration.service
for attempt in $(seq 1 30); do
  if curl --fail --silent http://127.0.0.1:8080/healthz >/dev/null; then
    echo "Raijin PostgreSQL restore completed and API is healthy."
    exit 0
  fi
  sleep 2
done

echo "PostgreSQL restore completed, but the Raijin API did not become healthy." >&2
exit 1
