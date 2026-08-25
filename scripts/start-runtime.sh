#!/usr/bin/env bash
set -euo pipefail

mode="${1:-REAL}"
install_root=/opt/s3-oci-migration/release
secret_root=/etc/s3-oci-migration/secrets
runtime_root=/run/s3-oci-migration
mode_control_root=/var/lib/s3-oci-migration/mode-control
oci_runtime_config=/etc/s3-oci-migration/oci-runtime.json
image=localhost/s3-oci-migration:latest
network=s3-oci-migration

case "$mode" in
  REAL)
    podman run -d --name s3-oci-app --replace --restart unless-stopped \
      --network "$network" -p 127.0.0.1:8080:8080 \
      -e RAIJIN_OPERATION_MODE=REAL \
      -e DATABASE_URL=postgresql+psycopg://migration@postgres:5432/migration \
      -e POSTGRES_PASSWORD_FILE=/run/secrets/postgres_password \
      -e OCI_RUNTIME_CONFIG_FILE=/run/oci-runtime/oci-runtime.json \
      -e RAIJIN_MODE_REQUEST_FILE=/run/mode-control/request \
      -v "$secret_root/postgres_password:/run/secrets/postgres_password:ro,z" \
      -v "$runtime_root:/run/platform-status:ro,z" \
      -v "$mode_control_root:/run/mode-control:rw,z" \
      -v "$oci_runtime_config:/run/oci-runtime/oci-runtime.json:ro,z" "$image"
    for attempt in $(seq 1 30); do
      curl --fail --silent http://127.0.0.1:8080/healthz >/dev/null && break
      [[ "$attempt" -lt 30 ]] || { echo "REAL API did not become healthy" >&2; exit 1; }
      sleep 2
    done
    for role in governance transfer; do
      podman run -d --name "s3-oci-${role}-worker" --replace --restart unless-stopped \
        --network "$network" \
        -e "RAIJIN_WORKER_ID=raijin-${role}-worker-vm" -e "RAIJIN_WORKER_ROLE=$role" \
        -e RAIJIN_OPERATION_MODE=REAL \
        -e RAIJIN_MODE_REQUEST_FILE=/run/mode-control/request \
        -e DATABASE_URL=postgresql+psycopg://migration@postgres:5432/migration \
        -e POSTGRES_PASSWORD_FILE=/run/secrets/postgres_password \
        -e OCI_RUNTIME_CONFIG_FILE=/run/oci-runtime/oci-runtime.json \
        -v "$secret_root/postgres_password:/run/secrets/postgres_password:ro,z" \
        -v "$mode_control_root:/run/mode-control:ro,z" \
        -v "$oci_runtime_config:/run/oci-runtime/oci-runtime.json:ro,z" \
        "$image" python3 -m app.real_worker
    done
    ;;
  SIMULATION)
    podman run --rm --network "$network" \
      -e RAIJIN_OPERATION_MODE=SIMULATION \
      -e RAIJIN_SIMULATOR_BASE_URL=http://simulator:8090 \
      -e DATABASE_URL=postgresql+psycopg://migration_simulation@postgres:5432/migration_simulation \
      -e RAIJIN_SIMULATOR_DATABASE_URL=postgresql+psycopg://migration_simulation@postgres:5432/migration_simulation \
      -e POSTGRES_PASSWORD_FILE=/run/secrets/simulation_postgres_password \
      -e RAIJIN_SIMULATOR_POSTGRES_PASSWORD_FILE=/run/secrets/simulation_postgres_password \
      -v "$secret_root/simulation_postgres_password:/run/secrets/simulation_postgres_password:ro,z" \
      "$image" python3 scripts/migrate-simulation.py
    podman run -d --name s3-oci-simulator --replace --restart unless-stopped \
      --network "$network" --network-alias simulator \
      -e RAIJIN_OPERATION_MODE=SIMULATION \
      -e RAIJIN_SIMULATOR_DATABASE_URL=postgresql+psycopg://migration_simulation@postgres:5432/migration_simulation \
      -e RAIJIN_SIMULATOR_POSTGRES_PASSWORD_FILE=/run/secrets/simulation_postgres_password \
      -v "$secret_root/simulation_postgres_password:/run/secrets/simulation_postgres_password:ro,z" \
      "$image" uvicorn app.simulator:app --host 0.0.0.0 --port 8090
    for attempt in $(seq 1 30); do
      podman exec s3-oci-simulator python3 -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8090/healthz', timeout=2)" >/dev/null 2>&1 && break
      [[ "$attempt" -lt 30 ]] || { echo "Simulator did not become healthy" >&2; exit 1; }
      sleep 2
    done
    podman run -d --name s3-oci-app --replace --restart unless-stopped \
      --network "$network" -p 127.0.0.1:8080:8080 \
      -e RAIJIN_OPERATION_MODE=SIMULATION \
      -e RAIJIN_SIMULATOR_BASE_URL=http://simulator:8090 \
      -e DATABASE_URL=postgresql+psycopg://migration_simulation@postgres:5432/migration_simulation \
      -e POSTGRES_PASSWORD_FILE=/run/secrets/simulation_postgres_password \
      -e RAIJIN_MODE_REQUEST_FILE=/run/mode-control/request \
      -v "$mode_control_root:/run/mode-control:rw,z" \
      -v "$secret_root/simulation_postgres_password:/run/secrets/simulation_postgres_password:ro,z" "$image"
    for attempt in $(seq 1 30); do
      curl --fail --silent http://127.0.0.1:8080/healthz >/dev/null && break
      [[ "$attempt" -lt 30 ]] || { echo "SIMULATION API did not become healthy" >&2; exit 1; }
      sleep 2
    done
    for role in governance transfer; do
      podman run -d --name "s3-oci-${role}-worker" --replace --restart unless-stopped \
        --network "$network" \
        -e "RAIJIN_WORKER_ID=simulation-${role}-worker" -e "RAIJIN_WORKER_ROLE=$role" \
        -e RAIJIN_OPERATION_MODE=SIMULATION \
        -e RAIJIN_MODE_REQUEST_FILE=/run/mode-control/request \
        -e RAIJIN_SIMULATOR_BASE_URL=http://simulator:8090 \
        -e DATABASE_URL=postgresql+psycopg://migration_simulation@postgres:5432/migration_simulation \
        -e POSTGRES_PASSWORD_FILE=/run/secrets/simulation_postgres_password \
        -v "$mode_control_root:/run/mode-control:ro,z" \
        -v "$secret_root/simulation_postgres_password:/run/secrets/simulation_postgres_password:ro,z" \
        "$image" python3 -m app.real_worker
    done
    ;;
  *) echo "Mode must be REAL or SIMULATION" >&2; exit 2 ;;
esac
