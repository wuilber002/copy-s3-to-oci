#!/usr/bin/env bash
set -euo pipefail

# Rotates the already-running local PostgreSQL only after Vault contains the
# new value. The secret is never printed or written to a log.
install_root=/opt/s3-oci-migration/release
secret_root=/etc/s3-oci-migration/secrets
runtime_config=/etc/s3-oci-migration/oci-runtime.json
current_password_file="$secret_root/postgres_password"
candidate_password_file="$secret_root/.postgres_password.vault"

[[ -s "$current_password_file" ]] || {
  echo "Local PostgreSQL password file is missing; use bootstrap for first initialization." >&2
  exit 1
}

python3 "$install_root/scripts/fetch-oci-secret.py" \
  --runtime-config "$runtime_config" \
  --secret-name postgres_password \
  --output "$candidate_password_file"

if cmp -s "$current_password_file" "$candidate_password_file"; then
  rm -f "$candidate_password_file"
  exit 0
fi

old_password="$(<"$current_password_file")"
new_password="$(<"$candidate_password_file")"
[[ "$new_password" =~ ^[A-Za-z0-9]+$ ]] || {
  rm -f "$candidate_password_file"
  echo "Vault PostgreSQL password has unsupported characters for this automated rotation." >&2
  exit 1
}

podman exec -e "PGPASSWORD=$old_password" s3-oci-postgres \
  psql -v ON_ERROR_STOP=1 -U migration -d migration \
  -c "ALTER ROLE migration PASSWORD '$new_password';" >/dev/null

install -o root -g root -m 0600 "$candidate_password_file" "$current_password_file"
rm -f "$candidate_password_file"
