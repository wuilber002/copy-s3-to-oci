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
  backup_json=$(printf '{"path":"%s","modified_at":"%s","size_bytes":%s}' \
    "$latest_backup" \
    "$(date -u -r "$latest_backup" +%Y-%m-%dT%H:%M:%SZ)" \
    "$(stat -c %s "$latest_backup")")
fi

# CPU is calculated from consecutive host-wide /proc/stat samples. The status
# timer runs every minute, so this represents VM utilization during the last
# interval rather than container-only consumption.
read -r cpu_total cpu_idle < <(awk '/^cpu / {total=0; for(i=2;i<=NF;i++) total+=$i; print total, $5+$6}' /proc/stat)
cpu_state_file="$runtime_root/cpu-ticks"
cpu_percent=0
if [[ -f "$cpu_state_file" ]]; then
  read -r previous_total previous_idle <"$cpu_state_file" || true
  total_delta=$((cpu_total - ${previous_total:-cpu_total}))
  idle_delta=$((cpu_idle - ${previous_idle:-cpu_idle}))
  if (( total_delta > 0 )); then
    cpu_percent=$(awk -v total="$total_delta" -v idle="$idle_delta" 'BEGIN { printf "%.2f", (total-idle)*100/total }')
  fi
fi
printf '%s %s\n' "$cpu_total" "$cpu_idle" >"$cpu_state_file"

memory_total_kb=$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)
memory_available_kb=$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)
memory_used_kb=$((memory_total_kb - memory_available_kb))
memory_percent=$(awk -v total="$memory_total_kb" -v used="$memory_used_kb" 'BEGIN { printf "%.2f", used*100/total }')

temporary_file=$(mktemp "$runtime_root/status.XXXXXX")
printf '{"available":true,"generated_at":"%s","resources":{"cpu_percent":%s,"memory_total_bytes":%s,"memory_used_bytes":%s,"memory_percent":%s},"services":{"migration_systemd":"%s","postgres_container":"%s","app_container":"%s","governance_worker":"%s","transfer_worker":"%s","postgres_backup_timer":"%s","platform_status_timer":"%s"},"last_postgres_backup":%s}\n' \
  "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  "$cpu_percent" \
  "$((memory_total_kb * 1024))" \
  "$((memory_used_kb * 1024))" \
  "$memory_percent" \
  "$(unit_state s3-oci-migration.service)" \
  "$(container_state s3-oci-postgres)" \
  "$(container_state s3-oci-app)" \
  "$(container_state s3-oci-governance-worker)" \
  "$(container_state s3-oci-transfer-worker)" \
  "$(unit_state s3-oci-backup-postgres.timer)" \
  "$(unit_state s3-oci-platform-status.timer)" \
  "$backup_json" >"$temporary_file"
chmod 644 "$temporary_file"
mv "$temporary_file" "$status_file"
