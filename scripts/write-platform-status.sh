#!/usr/bin/env bash
set -euo pipefail

runtime_root=/run/s3-oci-migration
status_file="$runtime_root/status.json"
backup_root=/var/lib/s3-oci-migration/backups
mkdir -p "$runtime_root"

unit_state() {
  if systemctl is-active --quiet "$1"; then
    printf 'active'
  else
    printf 'inactive'
  fi
}

container_state() {
  podman inspect --format '{{.State.Status}}' "$1" 2>/dev/null || printf 'absent'
}

latest_backup=''
if latest_backup_file=$(find "$backup_root" -maxdepth 1 -type f -name 'migration-*.dump' -printf '%T@ %p\n' 2>/dev/null | sort -nr | head -n1); then
  latest_backup=${latest_backup_file#* }
fi
backup_json=null
if [[ -n "$latest_backup" ]]; then
  backup_json=$(printf '"%s"' "$latest_backup")
fi

temporary_file=$(mktemp "$runtime_root/status.XXXXXX")
printf '{"available":true,"generated_at":"%s","services":{"migration_systemd":"%s","postgres_container":"%s","app_container":"%s","postgres_backup_timer":"%s","platform_status_timer":"%s"},"last_postgres_backup":%s}\n' \
  "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  "$(unit_state s3-oci-migration.service)" \
  "$(container_state s3-oci-postgres)" \
  "$(container_state s3-oci-app)" \
  "$(unit_state s3-oci-backup-postgres.timer)" \
  "$(unit_state s3-oci-platform-status.timer)" \
  "$backup_json" >"$temporary_file"
chmod 644 "$temporary_file"
mv "$temporary_file" "$status_file"
