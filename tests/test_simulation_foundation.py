import json
from pathlib import Path

import pytest

from app.cloud_backends import RealCloudBackend, SimulatedCloudBackend
from app.runtime_context import (
    OperationMode,
    RuntimeContext,
    RuntimeIsolationError,
    SIMULATOR_CONTRACT_VERSION,
    database_name,
    load_runtime_context,
)
from app.simulator_contract import SimulatorContractError, SimulatorHandshake, validate_handshake


def simulation_environment(**overrides):
    values = {
        "RAIJIN_OPERATION_MODE": "SIMULATION",
        "DATABASE_URL": "postgresql+psycopg://migration@postgres:5432/migration_simulation",
        "RAIJIN_SIMULATOR_BASE_URL": "http://raijin-simulator:8090",
        "RAIJIN_SIMULATOR_CONTRACT_VERSION": SIMULATOR_CONTRACT_VERSION,
    }
    values.update(overrides)
    return values


def test_real_mode_is_the_safe_backwards_compatible_default():
    context = load_runtime_context({"DATABASE_URL": "sqlite+pysqlite:///:memory:"})
    assert context.mode is OperationMode.REAL
    assert RealCloudBackend().readiness().operations_enabled


def test_simulation_requires_its_isolated_postgres_database():
    with pytest.raises(RuntimeIsolationError, match="migration_simulation"):
        load_runtime_context(simulation_environment(
            DATABASE_URL="postgresql+psycopg://migration@postgres:5432/migration"
        ))
    context = load_runtime_context(simulation_environment())
    assert context.mode is OperationMode.SIMULATION
    assert database_name(context.database_url) == "migration_simulation"


@pytest.mark.parametrize("credential", [
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_SHARED_CREDENTIALS_FILE",
    "OCI_CLI_CONFIG_FILE",
])
def test_simulation_refuses_cloud_credentials(credential):
    with pytest.raises(RuntimeIsolationError, match="refuses cloud credentials"):
        load_runtime_context(simulation_environment(**{credential: "must-not-be-mounted"}))


def test_simulation_refuses_real_cloud_operations():
    context = load_runtime_context(simulation_environment())
    with pytest.raises(RuntimeIsolationError, match="forbidden"):
        context.require_real_cloud("AssumeRole")


def test_contract_handshake_is_strict_and_fail_closed():
    valid = SimulatorHandshake()
    assert validate_handshake(valid).contract_version == SIMULATOR_CONTRACT_VERSION
    with pytest.raises(SimulatorContractError, match="not enabled"):
        validate_handshake(valid, require_operations=True)
    with pytest.raises(SimulatorContractError, match="mismatch"):
        validate_handshake(valid.model_copy(update={"contract_version": "999"}))


def test_simulated_backend_validates_remote_identity(monkeypatch):
    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return json.dumps(SimulatorHandshake().model_dump()).encode("utf-8")

    monkeypatch.setattr("app.cloud_backends.urlopen", lambda *_args, **_kwargs: Response())
    backend = SimulatedCloudBackend(load_runtime_context(simulation_environment()))
    readiness = backend.readiness()
    assert readiness.mode is OperationMode.SIMULATION
    assert readiness.contract_version == SIMULATOR_CONTRACT_VERSION
    assert not readiness.operations_enabled


def test_bootstrap_prepares_an_isolated_simulation_database_and_defaults_to_real():
    bootstrap = Path("scripts/bootstrap.sh").read_text(encoding="utf-8")
    runtime = Path("scripts/start-runtime.sh").read_text(encoding="utf-8")
    switch = Path("scripts/raijin-mode.sh").read_text(encoding="utf-8")
    assert "simulation_postgres_password" in bootstrap
    assert 'chown 70:70 "$secret_root/simulation_postgres_password"' in bootstrap
    assert 'chmod 0400 "$secret_root/simulation_postgres_password"' in bootstrap
    assert "trap cleanup_failed_bootstrap EXIT" in bootstrap
    assert 'podman stop -t 10 --ignore "${containers[@]}"' in bootstrap
    assert "s3-oci-postgres" in bootstrap[bootstrap.index("cleanup_failed_bootstrap"):bootstrap.index("trap cleanup_failed_bootstrap EXIT")]
    assert "bootstrap_complete=true" in bootstrap
    assert "CREATE ROLE migration_simulation" in bootstrap
    assert "createdb -U migration -O migration_simulation migration_simulation" in bootstrap
    assert "printf 'REAL\\n'" in bootstrap
    assert runtime.count("RAIJIN_OPERATION_MODE=REAL") == 2
    assert runtime.count("RAIJIN_OPERATION_MODE=SIMULATION") == 4
    assert "OCI_RUNTIME_CONFIG_FILE" in runtime
    simulation_branch = runtime.split("SIMULATION)", 1)[1]
    assert "OCI_RUNTIME_CONFIG_FILE" not in simulation_branch
    assert "AWS_ACCESS_KEY_ID" not in simulation_branch
    assert "state IN ('READY','RUNNING')" in switch


def test_mode_switch_audits_each_database_with_its_own_role():
    mode_switch = Path("scripts/raijin-mode.sh").read_text(encoding="utf-8")
    assert "database=migration_simulation" in mode_switch
    assert "database_user=migration_simulation" in mode_switch
    assert "JOIN waves ON waves.id = tasks.wave_id" in mode_switch
    assert "waves.status <> 'PAUSED'" in mode_switch


def test_terraform_creates_distinct_real_and_simulation_database_secrets():
    terraform = Path("terraform/orm/main.tf").read_text(encoding="utf-8")
    outputs = Path("terraform/orm/outputs.tf").read_text(encoding="utf-8")
    assert 'resource "random_password" "simulation_postgres"' in terraform
    assert 'resource "oci_vault_secret" "simulation_postgres_password"' in terraform
    assert "simulation_postgres_password =" in terraform
    assert "simulation_postgres_password =" in outputs


def test_backups_cover_real_and_simulated_databases_through_quarantine():
    backup = Path("scripts/backup-postgres.sh").read_text(encoding="utf-8")
    terraform = Path("terraform/orm/main.tf").read_text(encoding="utf-8")
    assert "-d migration_simulation" in backup
    assert "RAIJIN_LOGICAL_BACKUP_RETENTION_DAYS:-35" in backup
    assert "retention_days >= 30" in backup
    assert "retention_seconds = 3024000" in terraform


def test_simulator_foundation_cannot_claim_or_complete_raijin_tasks():
    simulator = Path("app/simulator.py").read_text(encoding="utf-8")
    assert "SessionLocal" not in simulator
    assert "Task" not in simulator
    assert "app.real_worker" not in simulator
    assert "claim_task" not in simulator
    assert "complete_task" not in simulator
