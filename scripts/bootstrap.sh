#!/usr/bin/env bash
set -euo pipefail

install_root=/opt/s3-oci-migration/release
data_root=/var/lib/s3-oci-migration
secret_root=/etc/s3-oci-migration/secrets

mkdir -p "$data_root/postgres" "$secret_root"
chmod 700 "$secret_root"

# The initial password is deliberately local-only. The production installer
# replaces it after reading the OCI Vault Secret through the instance principal.
if [[ ! -s "$secret_root/postgres_password" ]]; then
  openssl rand -base64 48 > "$secret_root/postgres_password"
  chmod 600 "$secret_root/postgres_password"
fi

cd "$install_root"
docker compose up -d --build
