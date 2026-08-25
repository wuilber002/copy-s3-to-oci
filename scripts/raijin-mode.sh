#!/usr/bin/env bash
set -euo pipefail

mode_file=/etc/s3-oci-migration/operation-mode
requested="${1:-status}"
current="$(tr '[:lower:]' '[:upper:]' <"$mode_file" 2>/dev/null || echo REAL)"

if [[ "$requested" == status ]]; then
  printf '%s\n' "$current"
  exit 0
fi
target="$(printf '%s' "$requested" | tr '[:lower:]' '[:upper:]')"
[[ "$target" == REAL || "$target" == SIMULATION ]] || { echo "Usage: raijin-mode real|simulation|status" >&2; exit 2; }
[[ "$target" != "$current" ]] || { echo "RAIJIN is already in $current mode"; exit 0; }

database=migration
database_user=migration
if [[ "$current" == SIMULATION ]]; then
  database=migration_simulation
  database_user=migration_simulation
fi
active="$(podman exec s3-oci-postgres psql -U "$database_user" -d "$database" -tAc \
  "SELECT COALESCE((SELECT count(*) FROM tasks JOIN waves ON waves.id = tasks.wave_id WHERE tasks.state IN ('READY','RUNNING') AND waves.status <> 'PAUSED'),0) + COALESCE((SELECT count(*) FROM discovery_jobs WHERE state IN ('READY','RUNNING')),0)" 2>/dev/null | tr -d '[:space:]')"
if [[ ! "$active" =~ ^[0-9]+$ ]]; then
  echo "Could not prove that the $current durable queues are idle; mode switch refused" >&2
  exit 1
fi
if (( active > 0 )); then
  echo "Mode switch refused: $active READY/RUNNING durable operation(s) exist in $current" >&2
  exit 1
fi

install -d -m 0755 "$(dirname "$mode_file")"
printf '%s\n' "$target" >"$mode_file.tmp"
chmod 0644 "$mode_file.tmp"
mv "$mode_file.tmp" "$mode_file"
systemctl restart s3-oci-migration.service
sleep 2
reported="$(curl --fail --silent http://127.0.0.1:8080/api/runtime | sed -n 's/.*"operation_mode":"\([A-Z]*\)".*/\1/p')"
if [[ "$reported" != "$target" ]]; then
  echo "Runtime validation failed after switch to $target; rolling back to $current" >&2
  printf '%s\n' "$current" >"$mode_file.tmp"
  chmod 0644 "$mode_file.tmp"
  mv "$mode_file.tmp" "$mode_file"
  systemctl restart s3-oci-migration.service
  exit 1
fi
echo "RAIJIN switched safely to $target"
