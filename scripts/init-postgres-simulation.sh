#!/usr/bin/env bash
set -euo pipefail

# PostgreSQL's official image executes this only for a new data directory.
# Keep the simulator on its own role and logical database even in local Compose.
simulation_password="$(tr -d '\r\n' </run/secrets/simulation_postgres_password)"
[[ -n "$simulation_password" ]] || { echo "Simulation PostgreSQL password is empty" >&2; exit 1; }

psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set=simulation_password="$simulation_password" <<'SQL'
SELECT format('CREATE ROLE migration_simulation LOGIN PASSWORD %L', :'simulation_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'migration_simulation') \gexec
SELECT 'CREATE DATABASE migration_simulation OWNER migration_simulation'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'migration_simulation') \gexec
SQL
