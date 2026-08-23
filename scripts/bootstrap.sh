#!/usr/bin/env bash
set -euo pipefail

install_root=/opt/s3-oci-migration/release
data_root=/var/lib/s3-oci-migration
secret_root=/etc/s3-oci-migration/secrets
runtime_root=/run/s3-oci-migration
oci_runtime_config=/etc/s3-oci-migration/oci-runtime.json

mkdir -p "$data_root/postgres" "$secret_root" "$runtime_root"
chmod 700 "$secret_root"
# The web service is loopback-only. SSH is the sole administrative entrypoint,
# and it accepts only the public key provisioned by Terraform/cloud-init.
install -d -m 0755 /etc/ssh/sshd_config.d
cat >/etc/ssh/sshd_config.d/99-raijin-key-only.conf <<'EOF'
PasswordAuthentication no
KbdInteractiveAuthentication no
ChallengeResponseAuthentication no
PubkeyAuthentication yes
PermitRootLogin no
EOF
sshd -t && systemctl reload sshd
if [[ ! -f "$oci_runtime_config" ]]; then
  printf '{"secret_ocids":{}}\n' >"$oci_runtime_config"
fi
# The OCI runtime config contains OCIDs, not secret values. The application
# container must read it; Vault still protects every credential value.
chmod 644 "$oci_runtime_config"

# Vault is the source of truth. A first boot materializes the Terraform-created
# password locally without writing its value to a command line or service log.
if [[ ! -s "$secret_root/postgres_password" ]]; then
  python3 "$install_root/scripts/fetch-oci-secret.py" \
    --runtime-config "$oci_runtime_config" \
    --secret-name postgres_password \
    --output "$secret_root/postgres_password"
fi

podman network exists s3-oci-migration 2>/dev/null || podman network create s3-oci-migration
podman rm -f s3-oci-app s3-oci-postgres s3-oci-governance-worker s3-oci-transfer-worker s3-oci-real-worker 2>/dev/null || true

podman run -d --name s3-oci-postgres --replace --restart unless-stopped \
  --network s3-oci-migration --network-alias postgres \
  -e POSTGRES_DB=migration \
  -e POSTGRES_USER=migration \
  -e POSTGRES_PASSWORD_FILE=/run/secrets/postgres_password \
  -v "$data_root/postgres:/var/lib/postgresql/data:Z" \
  -v "$secret_root/postgres_password:/run/secrets/postgres_password:ro,z" \
  docker.io/library/postgres:16-alpine

postgres_ready=false
for attempt in $(seq 1 30); do
  if podman exec s3-oci-postgres pg_isready -U migration -d migration >/dev/null 2>&1; then
    postgres_ready=true
    break
  fi
  sleep 2
done
if [[ "$postgres_ready" != true ]]; then
  echo "PostgreSQL did not become ready" >&2
  exit 1
fi

podman build -t localhost/s3-oci-migration:latest "$install_root"
podman run -d --name s3-oci-app --replace --restart unless-stopped \
  --network s3-oci-migration \
  -p 127.0.0.1:8080:8080 \
  -e DATABASE_URL=postgresql+psycopg://migration@postgres:5432/migration \
  -e POSTGRES_PASSWORD_FILE=/run/secrets/postgres_password \
  -e OCI_RUNTIME_CONFIG_FILE=/run/oci-runtime/oci-runtime.json \
  -v "$secret_root/postgres_password:/run/secrets/postgres_password:ro,z" \
  -v "$runtime_root:/run/platform-status:ro,z" \
  -v "$oci_runtime_config:/run/oci-runtime/oci-runtime.json:ro,z" \
  localhost/s3-oci-migration:latest

# The API startup performs safe additive schema migrations. Wait for it before
# starting the worker, otherwise a freshly deployed worker could query a new
# column a few seconds before that migration has completed.
app_ready=false
for attempt in $(seq 1 30); do
  if curl --fail --silent http://127.0.0.1:8080/healthz >/dev/null; then
    app_ready=true
    break
  fi
  sleep 2
done
if [[ "$app_ready" != true ]]; then
  echo "API did not become healthy; real worker will not start" >&2
  exit 1
fi

# Kept separate from the API: governance owns discovery, Batch Operations,
# restore polling and deep audits. It remains idle until explicitly enabled.
podman run -d --name s3-oci-governance-worker --replace --restart unless-stopped \
  --network s3-oci-migration \
  -e RAIJIN_WORKER_ID=raijin-governance-worker-vm \
  -e RAIJIN_WORKER_ROLE=governance \
  -e DATABASE_URL=postgresql+psycopg://migration@postgres:5432/migration \
  -e POSTGRES_PASSWORD_FILE=/run/secrets/postgres_password \
  -e OCI_RUNTIME_CONFIG_FILE=/run/oci-runtime/oci-runtime.json \
  -v "$secret_root/postgres_password:/run/secrets/postgres_password:ro,z" \
  -v "$oci_runtime_config:/run/oci-runtime/oci-runtime.json:ro,z" \
  localhost/s3-oci-migration:latest python3 -m app.real_worker

# Transfer owns only file copy and multipart resume. A long object cannot
# delay a restore poll, discovery checkpoint or scheduler decision.
podman run -d --name s3-oci-transfer-worker --replace --restart unless-stopped \
  --network s3-oci-migration \
  -e RAIJIN_WORKER_ID=raijin-transfer-worker-vm \
  -e RAIJIN_WORKER_ROLE=transfer \
  -e DATABASE_URL=postgresql+psycopg://migration@postgres:5432/migration \
  -e POSTGRES_PASSWORD_FILE=/run/secrets/postgres_password \
  -e OCI_RUNTIME_CONFIG_FILE=/run/oci-runtime/oci-runtime.json \
  -v "$secret_root/postgres_password:/run/secrets/postgres_password:ro,z" \
  -v "$oci_runtime_config:/run/oci-runtime/oci-runtime.json:ro,z" \
  localhost/s3-oci-migration:latest python3 -m app.real_worker

cat >/etc/systemd/system/s3-oci-migration.service <<'EOF'
[Unit]
Description=S3 to OCI migration platform
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/opt/s3-oci-migration/release/scripts/bootstrap.sh
ExecStop=/usr/bin/podman stop -t 30 s3-oci-app s3-oci-governance-worker s3-oci-transfer-worker s3-oci-postgres

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable s3-oci-migration.service

install -m 0750 "$install_root/scripts/backup-postgres.sh" /usr/local/sbin/s3-oci-backup-postgres
install -m 0750 "$install_root/scripts/restore-postgres.sh" /usr/local/sbin/s3-oci-restore-postgres
cat >/etc/systemd/system/s3-oci-backup-postgres.service <<'EOF'
[Unit]
Description=Local PostgreSQL backup for S3 to OCI migration platform
After=s3-oci-migration.service

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/s3-oci-backup-postgres
EOF
cat >/etc/systemd/system/s3-oci-backup-postgres.timer <<'EOF'
[Unit]
Description=Daily local PostgreSQL backup for S3 to OCI migration platform

[Timer]
OnCalendar=*-*-* 02:15:00 UTC
Persistent=true

[Install]
WantedBy=timers.target
EOF
systemctl daemon-reload
systemctl enable --now s3-oci-backup-postgres.timer

install -m 0750 "$install_root/scripts/write-platform-status.sh" /usr/local/sbin/s3-oci-write-platform-status
cat >/etc/systemd/system/s3-oci-platform-status.service <<'EOF'
[Unit]
Description=Write S3 to OCI migration platform status
After=s3-oci-migration.service

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/s3-oci-write-platform-status
EOF
cat >/etc/systemd/system/s3-oci-platform-status.timer <<'EOF'
[Unit]
Description=Refresh S3 to OCI migration platform status

[Timer]
OnBootSec=30
OnUnitActiveSec=60
Persistent=true

[Install]
WantedBy=timers.target
EOF
systemctl daemon-reload
systemctl enable --now s3-oci-platform-status.timer
/usr/local/sbin/s3-oci-write-platform-status

install -m 0750 "$install_root/scripts/simulated-worker.py" /usr/local/sbin/s3-oci-simulated-worker
cat >/etc/systemd/system/s3-oci-simulated-worker.service <<'EOF'
[Unit]
Description=S3 to OCI migration simulated worker
After=s3-oci-migration.service
Requires=s3-oci-migration.service

[Service]
Type=simple
ExecStart=/usr/bin/python3 /usr/local/sbin/s3-oci-simulated-worker
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
# The simulated worker requires this service. Do not synchronously wait for it
# here or systemd would wait for this oneshot unit to finish before it can mark
# the dependency active. The non-blocking start runs after this unit is active.
systemctl enable s3-oci-simulated-worker.service
systemctl start --no-block s3-oci-simulated-worker.service
