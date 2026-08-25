"""End-to-end proof that simulation uses the production worker state machine."""

import json
import os
import socket
import threading
import time
import uuid
from pathlib import Path

from sqlalchemy import BigInteger, create_engine, select
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker
import uvicorn

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
_password_file = Path("/tmp/raijin-test-password-simulation-e2e")
_password_file.write_text("test-password", encoding="utf-8")
os.environ.setdefault("POSTGRES_PASSWORD_FILE", str(_password_file))
os.environ.setdefault("OCI_RUNTIME_CONFIG_FILE", "/tmp/raijin-test-oci-runtime-simulation-e2e.json")

from app import main, real_worker, simulator
from app.cloud_backends import backend_for
from app.runtime_context import OperationMode, RuntimeContext
from app.simulation_migrations import migrate
from app.simulator_admin import SimulatorAdminClient
from app.simulator_store import ScenarioCreate, SimulatorStore


@compiles(BigInteger, "sqlite")
def compile_big_integer_for_sqlite(_type, _compiler, **_kwargs):
    # SQLite autoincrement requires the exact INTEGER type name. Production
    # continues to use PostgreSQL BIGINT; this only makes the isolated E2E
    # control-plane database exercise its normal generated identifiers.
    return "INTEGER"


def test_real_workers_complete_simulated_discovery_restore_and_transfer(tmp_path, monkeypatch):
    simulator_database_url = f"sqlite+pysqlite:///{tmp_path / 'simulator.db'}"
    migrate(create_engine(simulator_database_url))
    monkeypatch.setenv("RAIJIN_SIMULATOR_DATABASE_URL", simulator_database_url)
    simulator.store.cache_clear()

    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    port = listener.getsockname()[1]
    listener.close()
    server = uvicorn.Server(
        uvicorn.Config(simulator.app, host="127.0.0.1", port=port, log_level="error")
    )
    server_thread = threading.Thread(target=server.run, daemon=True)
    server_thread.start()
    for _ in range(200):
        if server.started:
            break
        time.sleep(0.01)
    assert server.started

    base_url = f"http://127.0.0.1:{port}"
    simulator_store = SimulatorStore.from_environ()
    scenario = simulator_store.create_scenario(
        ScenarioCreate(
            name="same-worker-e2e",
            fidelity="DATA",
            seed="same-worker-e2e-seed",
            logical_size_bytes=12_288,
            physical_budget_bytes=12_288,
            configuration={
                "bulk_restore_min_hours": 0,
                "bulk_restore_max_hours": 0,
            },
        )
    )
    simulator_engine = simulator.engine()
    simulator_engine.materialize(
        scenario.id,
        source_bucket="virtual-source",
        destination_bucket="virtual-destination",
        region="us-east-1",
        object_count=3,
        logical_size_bytes=12_288,
        prefixes=["smoke/"],
        storage_class="DEEP_ARCHIVE",
    )
    execution = simulator_store.create_execution(scenario.id)

    control_database_url = f"sqlite+pysqlite:///{tmp_path / 'control.db'}"
    control_engine = create_engine(
        control_database_url, connect_args={"check_same_thread": False}
    )
    main.Base.metadata.create_all(control_engine)
    sessions = sessionmaker(bind=control_engine, expire_on_commit=False)
    context = RuntimeContext(
        mode=OperationMode.SIMULATION,
        database_url=control_database_url,
        simulator_base_url=base_url,
    )
    monkeypatch.setattr(main, "runtime_context", context)
    monkeypatch.setattr(real_worker, "runtime_context", context)
    monkeypatch.setattr(main, "cloud_backend", backend_for(context))
    monkeypatch.setattr(real_worker, "cloud_backend", backend_for(context))
    monkeypatch.setattr(main, "SessionLocal", sessions)
    monkeypatch.setattr(real_worker, "SessionLocal", sessions)
    monkeypatch.setenv("RAIJIN_MODE_REQUEST_FILE", str(tmp_path / "no-mode-request"))

    with sessions() as session:
        session.add(
            main.RuntimeSettings(
                id=1,
                transfer_workers=2,
                max_throughput_mbps=10_000,
                multipart_part_size_mib=5,
                task_lease_seconds=300,
            )
        )
        source = main.Source(
            name="simulated-e2e-source",
            s3_bucket="virtual-source",
            s3_prefix="smoke/",
            aws_region="us-east-1",
            destination_bucket="virtual-destination",
            backend_kind="SIMULATED",
            simulation_scenario_id=scenario.id,
            simulation_execution_id=execution.id,
            simulation_correlation_id=execution.correlation_id,
            simulation_tenant_id=str(uuid.uuid4()),
            simulation_project_id=str(uuid.uuid4()),
            simulation_fidelity="DATA",
            status="DISCOVERY_QUEUED",
        )
        session.add(source)
        session.flush()
        session.add(main.SourcePrefix(source_id=source.id, prefix="smoke/"))
        session.add(main.DiscoveryJob(source_id=source.id, mode="REMOTE_LIST"))
        session.commit()
        source_id = source.id

    real_worker.run_once("governance")

    with sessions() as session:
        source = session.get(main.Source, source_id)
        objects = list(
            session.scalars(
                select(main.ObjectRecord)
                .where(main.ObjectRecord.source_id == source_id)
                .order_by(main.ObjectRecord.id)
            )
        )
        assert source.status == "DISCOVERED"
        assert len(objects) == 3
        wave = main.Wave(
            source_id=source_id,
            name="same-worker-wave-001",
            max_bytes=sum(item.size_bytes for item in objects),
            restore_days=1,
            restore_tier="BULK",
            status="READY_FOR_RESTORE",
        )
        session.add(wave)
        session.flush()
        for item in objects:
            item.wave_id = wave.id
            item.state = main.ObjectState.WAVE_ASSIGNED
        session.add(main.Task(wave_id=wave.id, kind="SUBMIT_BATCH_RESTORE"))
        session.commit()
        wave_id = wave.id

    real_worker.run_once("governance")
    real_worker.run_once("governance")
    real_worker.run_once("transfer")

    with sessions() as session:
        wave = session.get(main.Wave, wave_id)
        objects = list(
            session.scalars(select(main.ObjectRecord).where(main.ObjectRecord.wave_id == wave_id))
        )
        assert wave.status == "COMPLETED"
        assert {item.state for item in objects} == {main.ObjectState.TRANSFERRED}
        assert all(item.delivery_integrity_status == "OCI_ACCEPTED" for item in objects)
        assert all(item.delivery_integrity_checksum for item in objects)
        assert all(item.transfer_progress_bytes == item.size_bytes for item in objects)

    execution_status = SimulatorAdminClient(base_url).get_execution(execution.id)
    assert execution_status["state"] == "SUCCEEDED"
    assert execution_status["physical_bytes_processed"] == 12_288
    with simulator_store.sessions() as session:
        destination = simulator_engine.list_objects(
            execution.id, "virtual-destination", "", None, 1000
        )
        assert len(destination.objects) == 3

    server.should_exit = True
    server_thread.join(timeout=5)
    simulator.store.cache_clear()
