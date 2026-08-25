#!/usr/bin/env bash
set -euo pipefail

install_root=/opt/s3-oci-migration/release
data_root=/var/lib/s3-oci-migration
secret_root=/etc/s3-oci-migration/secrets
runtime_root=/run/s3-oci-migration
mode_control_root=/var/lib/s3-oci-migration/mode-control
oci_runtime_config=/etc/s3-oci-migration/oci-runtime.json

mkdir -p "$data_root/postgres" "$secret_root" "$runtime_root" "$mode_control_root"
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
if [[ ! -s "$secret_root/simulation_postgres_password" ]]; then
  python3 "$install_root/scripts/fetch-oci-secret.py" \
    --runtime-config "$oci_runtime_config" \
    --secret-name simulation_postgres_password \
    --output "$secret_root/simulation_postgres_password"
fi
# postgres:16-alpine runs the database server as UID/GID 70. The server reads
# this file through pg_read_file() while creating/updating the isolated role;
# keep it unreadable to every other non-root host user.
chown 70:70 "$secret_root/simulation_postgres_password"
chmod 0400 "$secret_root/simulation_postgres_password"

podman network exists s3-oci-migration 2>/dev/null || podman network create s3-oci-migration
podman rm -f s3-oci-app s3-oci-postgres s3-oci-governance-worker s3-oci-transfer-worker s3-oci-real-worker s3-oci-simulator 2>/dev/null || true

podman run -d --name s3-oci-postgres --replace --restart unless-stopped \
  --network s3-oci-migration --network-alias postgres \
  -e POSTGRES_DB=migration \
  -e POSTGRES_USER=migration \
  -e POSTGRES_PASSWORD_FILE=/run/secrets/postgres_password \
  -v "$data_root/postgres:/var/lib/postgresql/data:Z" \
  -v "$secret_root/postgres_password:/run/secrets/postgres_password:ro,z" \
  -v "$secret_root/simulation_postgres_password:/run/secrets/simulation_postgres_password:ro,z" \
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

# A distinct role and database keep real and simulated control-plane state
# isolated while sharing the same durable PostgreSQL container. The password
# is read inside PostgreSQL from the mounted Secret and never enters argv/logs.
podman exec -i s3-oci-postgres psql -U migration -d migration -v ON_ERROR_STOP=1 <<'SQL'
DO $block$
DECLARE
  simulation_password text := trim(pg_read_file('/run/secrets/simulation_postgres_password'));
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'migration_simulation') THEN
    EXECUTE format('CREATE ROLE migration_simulation LOGIN PASSWORD %L', simulation_password);
  ELSE
    EXECUTE format('ALTER ROLE migration_simulation PASSWORD %L', simulation_password);
  END IF;
END
$block$;
SQL
if ! podman exec s3-oci-postgres psql -U migration -d migration -tAc \
  "SELECT 1 FROM pg_database WHERE datname = 'migration_simulation'" | grep -q 1; then
  podman exec s3-oci-postgres createdb -U migration -O migration_simulation migration_simulation
fi

podman build -t localhost/s3-oci-migration:latest "$install_root"
mode_file=/etc/s3-oci-migration/operation-mode
if [[ ! -s "$mode_file" ]]; then
  printf 'REAL\n' >"$mode_file"
fi
operation_mode="$(tr '[:lower:]' '[:upper:]' <"$mode_file")"
[[ "$operation_mode" == REAL || "$operation_mode" == SIMULATION ]] || {
  echo "Invalid operation mode in $mode_file" >&2
  exit 1
}
install -m 0750 "$install_root/scripts/start-runtime.sh" /usr/local/sbin/s3-oci-start-runtime
install -m 0750 "$install_root/scripts/stop-runtime.sh" /usr/local/sbin/s3-oci-stop-runtime
install -m 0750 "$install_root/scripts/raijin-mode.sh" /usr/local/sbin/raijin-mode
install -m 0750 "$install_root/scripts/process-mode-request.sh" /usr/local/sbin/s3-oci-process-mode-request
/usr/local/sbin/s3-oci-start-runtime "$operation_mode"

cat >/etc/systemd/system/s3-oci-migration.service <<'EOF'
[Unit]
Description=S3 to OCI migration platform
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/opt/s3-oci-migration/release/scripts/bootstrap.sh
ExecStop=/usr/local/sbin/s3-oci-stop-runtime

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable s3-oci-migration.service

cat >/etc/systemd/system/s3-oci-mode-request.service <<'EOF'
[Unit]
Description=Process a validated RAIJIN operation-mode request
After=s3-oci-migration.service

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/s3-oci-process-mode-request
EOF
cat >/etc/systemd/system/s3-oci-mode-request.timer <<'EOF'
[Unit]
Description=Watch for RAIJIN operation-mode requests

[Timer]
OnBootSec=10
OnUnitActiveSec=5
AccuracySec=1
Persistent=true

[Install]
WantedBy=timers.target
EOF
systemctl daemon-reload
systemctl enable --now s3-oci-mode-request.timer

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

# Remove the legacy task-advancing simulation worker. The replacement backend
# simulator is intentionally not installed until the exclusive Simulation mode
# and its isolated database are available.
systemctl disable --now s3-oci-simulated-worker.service 2>/dev/null || true
rm -f /etc/systemd/system/s3-oci-simulated-worker.service /usr/local/sbin/s3-oci-simulated-worker
systemctl daemon-reload
