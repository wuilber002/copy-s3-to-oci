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

podman network exists s3-oci-migration 2>/dev/null || podman network create s3-oci-migration
podman rm -f s3-oci-app s3-oci-postgres 2>/dev/null || true

podman run -d --name s3-oci-postgres --replace --restart unless-stopped \
  --network s3-oci-migration --network-alias postgres \
  -e POSTGRES_DB=migration \
  -e POSTGRES_USER=migration \
  -e POSTGRES_PASSWORD_FILE=/run/secrets/postgres_password \
  -v "$data_root/postgres:/var/lib/postgresql/data:Z" \
  -v "$secret_root/postgres_password:/run/secrets/postgres_password:ro,Z" \
  docker.io/library/postgres:16-alpine

for attempt in $(seq 1 30); do
  if podman exec s3-oci-postgres pg_isready -U migration -d migration >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

podman build -t localhost/s3-oci-migration:latest "$install_root"
podman run -d --name s3-oci-app --replace --restart unless-stopped \
  --network s3-oci-migration \
  -p 127.0.0.1:8080:8080 \
  -e DATABASE_URL=postgresql+psycopg://migration@postgres:5432/migration \
  -e POSTGRES_PASSWORD_FILE=/run/secrets/postgres_password \
  -v "$secret_root/postgres_password:/run/secrets/postgres_password:ro,Z" \
  localhost/s3-oci-migration:latest

cat >/etc/systemd/system/s3-oci-migration.service <<'EOF'
[Unit]
Description=S3 to OCI migration platform
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/opt/s3-oci-migration/release/scripts/bootstrap.sh
ExecStop=/usr/bin/podman stop -t 30 s3-oci-app s3-oci-postgres

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable s3-oci-migration.service
