import json
import hashlib

import pytest
from sqlalchemy import create_engine, select

from app.simulation_engine import SimulationEngine
from app.simulation_migrations import migrate
from app.simulation_schema import InjectedFault, VirtualBucket, VirtualObject
from app.simulated_data import consume_and_discard
from app.simulated_data import SimulatedDataIntegrityError
from app.simulator_store import ScenarioCreate, SimulatorStore


@pytest.fixture
def virtual_cloud():
    database = create_engine("sqlite+pysqlite:///:memory:")
    migrate(database)
    store = SimulatorStore(database)
    scenario = store.create_scenario(
        ScenarioCreate(
            name="virtual-cloud",
            fidelity="DATA",
            seed="seed-v1",
            logical_size_bytes=10_001,
            physical_budget_bytes=20_000,
            configuration={
                "bulk_restore_min_hours": 0,
                "bulk_restore_max_hours": 0,
            },
        )
    )
    engine = SimulationEngine(store)
    materialized = engine.materialize(
        scenario.id,
        source_bucket="source",
        destination_bucket="destination",
        region="us-east-1",
        object_count=7,
        logical_size_bytes=10_001,
        prefixes=["app", "archive"],
        storage_class="DEEP_ARCHIVE",
    )
    execution = store.create_execution(scenario.id)
    return engine, execution, materialized


def test_materialization_preserves_exact_logical_totals(virtual_cloud):
    engine, execution, materialized = virtual_cloud
    assert materialized["objects"] == 7
    assert materialized["logical_size_bytes"] == 10_001

    token = None
    objects = []
    while True:
        page = engine.list_objects(execution.id, "source", "", token, 3)
        objects.extend(page.objects)
        token = page.next_continuation_token
        if not token:
            break

    assert len(objects) == 7
    assert sum(item.size_bytes for item in objects) == 10_001
    assert {item.key.split("/", 1)[0] for item in objects} == {"app", "archive"}


def test_control_network_profile_uses_virtual_time_for_degradation_and_outage():
    database = create_engine("sqlite+pysqlite:///:memory:")
    migrate(database)
    store = SimulatorStore(database)
    scenario = store.create_scenario(
        ScenarioCreate(
            name="network-profile",
            fidelity="CONTROL",
            seed="network-seed",
            logical_size_bytes=1_000_000,
            configuration={
                "bulk_restore_min_hours": 0,
                "bulk_restore_max_hours": 0,
                "network_profile": [
                    {"start_hour": 0, "end_hour": 1, "throughput_mbps": 100},
                    {"start_hour": 1, "end_hour": 2, "throughput_mbps": 10},
                    {"start_hour": 2, "end_hour": 3, "unavailable": True},
                ],
            },
        )
    )
    engine = SimulationEngine(store)
    engine.materialize(
        scenario.id,
        source_bucket="source",
        destination_bucket="destination",
        region="us-east-1",
        object_count=1,
        logical_size_bytes=1_000_000,
        prefixes=["x"],
        storage_class="STANDARD",
    )
    execution = store.create_execution(scenario.id)
    store.set_execution_state(execution.id, "RUNNING")
    store.control_clock(execution.id, "PAUSE")
    item = engine.list_objects(execution.id, "source", "", None, 1).objects[0]

    fast = engine.transfer_logically(
        execution.id, "source", "destination", item.key, item.size_bytes, "fast"
    )
    store.control_clock(execution.id, "ADVANCE", 3600)
    slow = engine.transfer_logically(
        execution.id, "source", "destination", item.key, item.size_bytes, "slow"
    )
    assert slow.simulated_elapsed_seconds > fast.simulated_elapsed_seconds * 5

    store.control_clock(execution.id, "ADVANCE", 3600)
    with pytest.raises(ConnectionError, match="network profile"):
        engine.transfer_logically(
            execution.id, "source", "destination", item.key, item.size_bytes, "down"
        )


def test_probabilistic_fault_selection_is_identical_in_a_cloned_catalog():
    database = create_engine("sqlite+pysqlite:///:memory:")
    migrate(database)
    store = SimulatorStore(database)
    outcomes = []
    for index in range(2):
        scenario = store.create_scenario(
            ScenarioCreate(
                name=f"replay-{index}",
                fidelity="CONTROL",
                seed="registered-replay-seed-v1",
                logical_size_bytes=100,
                configuration={"bulk_restore_min_hours": 1, "bulk_restore_max_hours": 1},
                fault_rules=[{
                    "operation": "RESTORE",
                    "action": "REJECT",
                    "probability": 0.5,
                    "error_code": "StableReplayFailure",
                }],
            )
        )
        engine = SimulationEngine(store)
        engine.materialize(
            scenario.id,
            source_bucket=f"source-{index}",
            destination_bucket=f"destination-{index}",
            region="us-east-1",
            object_count=20,
            logical_size_bytes=100,
            prefixes=["replay"],
            storage_class="DEEP_ARCHIVE",
        )
        execution = store.create_execution(scenario.id)
        objects = engine.list_objects(execution.id, f"source-{index}", "", None, 100).objects
        accepted = [
            engine.restore_object(
                execution.id,
                f"source-{index}",
                item.key,
                "BULK",
                1,
                f"restore-{position}",
            ).accepted
            for position, item in enumerate(objects)
        ]
        with store.sessions() as session:
            bucket_id = session.scalar(select(VirtualBucket.id).where(
                VirtualBucket.scenario_id == scenario.id,
                VirtualBucket.name == f"source-{index}",
            ))
            rows = list(session.scalars(select(VirtualObject).where(
                VirtualObject.bucket_id == bucket_id
            ).order_by(VirtualObject.object_key)))
            delays = [
                None if row.restore_available_at is None else round(
                    (row.restore_available_at - row.restore_requested_at).total_seconds(), 6
                )
                for row in rows
            ]
        outcomes.append((accepted, delays))

    assert outcomes[0] == outcomes[1]
    assert any(outcomes[0][0])
    assert not all(outcomes[0][0])


def test_restore_head_and_data_range_use_virtual_catalog(virtual_cloud):
    engine, execution, _ = virtual_cloud
    item = engine.list_objects(execution.id, "source", "", None, 1).objects[0]

    before = engine.head_object(execution.id, "source", item.key)
    assert before.exists is True
    assert before.restore_expires_at is None

    accepted = engine.restore_object(
        execution.id, "source", item.key, "BULK", 2, "idempotent-1"
    )
    assert accepted.accepted is True
    assert accepted.request_id == "idempotent-1"

    after = engine.head_object(execution.id, "source", item.key)
    assert after.restore_in_progress is False
    assert after.restore_expires_at is not None

    evidence = consume_and_discard(
        engine.read_range(execution.id, "source", item.key, 0, item.size_bytes),
        expected_size=item.size_bytes,
    )
    assert evidence.size_bytes == item.size_bytes


def test_expired_restore_is_unavailable_and_can_be_requested_again(virtual_cloud):
    engine, execution, _ = virtual_cloud
    item = engine.list_objects(execution.id, "source", "", None, 1).objects[0]
    first = engine.restore_object(
        execution.id, "source", item.key, "BULK", 1, "restore-first"
    )
    assert first.accepted is True
    store = engine.store
    store.control_clock(execution.id, "PAUSE")
    store.control_clock(execution.id, "ADVANCE", 2 * 24 * 3600)

    expired = engine.head_object(execution.id, "source", item.key)
    assert expired.exists is True
    assert expired.restore_in_progress is False
    assert expired.restore_expires_at is None
    with pytest.raises(PermissionError, match="not restored"):
        list(engine.read_range(execution.id, "source", item.key, 0, item.size_bytes))

    second = engine.restore_object(
        execution.id, "source", item.key, "BULK", 1, "restore-second"
    )
    assert second.accepted is True
    assert second.already_in_progress is False


def test_catalog_cannot_be_materialized_twice(virtual_cloud):
    engine, execution, _ = virtual_cloud
    with pytest.raises(ValueError, match="immutable"):
        engine.materialize(
            execution.scenario_id,
            source_bucket="source-2",
            destination_bucket="destination-2",
            region="us-east-1",
            object_count=1,
            logical_size_bytes=1,
            prefixes=["again"],
            storage_class="STANDARD",
        )


def test_destination_discards_payload_but_persists_independent_evidence(virtual_cloud):
    engine, execution, _ = virtual_cloud
    item = engine.list_objects(execution.id, "source", "", None, 1).objects[0]
    engine.restore_object(execution.id, "source", item.key, "BULK", 1, "restore")
    content = list(engine.read_range(execution.id, "source", item.key, 0, item.size_bytes))
    checksum = consume_and_discard(content).checksum_sha256

    evidence = engine.put_object(
        execution.id,
        "destination",
        item.key,
        iter(content),
        item.size_bytes,
        checksum,
        "put-1",
    )

    assert evidence.accepted is True
    assert evidence.checksum_sha256 == checksum
    assert engine.head_object(execution.id, "destination", item.key).exists is True


def test_destination_detects_corrupted_stream(virtual_cloud):
    engine, execution, _ = virtual_cloud
    item = engine.list_objects(execution.id, "source", "", None, 1).objects[0]
    with pytest.raises(SimulatedDataIntegrityError, match="deterministic source"):
        engine.put_object(
            execution.id,
            "destination",
            item.key,
            [b"x" * item.size_bytes],
            item.size_bytes,
            "invalid",
            "put-corrupt",
        )


def test_multipart_replay_validates_each_part_and_full_object(virtual_cloud):
    engine, execution, _ = virtual_cloud
    item = engine.list_objects(execution.id, "source", "", None, 1).objects[0]
    engine.restore_object(execution.id, "source", item.key, "BULK", 1, "restore")
    part_size = max(1, item.size_bytes // 2)
    upload_id = engine.create_multipart(
        execution.id,
        "destination",
        item.key,
        item.size_bytes,
        part_size,
        None,
        "create-1",
    )
    parts = []
    offset = 0
    part_number = 1
    full = []
    while offset < item.size_bytes:
        length = min(part_size, item.size_bytes - offset)
        chunks = list(
            engine.read_range(execution.id, "source", item.key, offset, length)
        )
        full.extend(chunks)
        checksum = consume_and_discard(chunks).checksum_sha256
        parts.append(
            engine.upload_part(
                upload_id, part_number, iter(chunks), length, checksum, attempt=1
            )
        )
        offset += length
        part_number += 1

    full_checksum = consume_and_discard(full).checksum_sha256
    committed = engine.commit_multipart(upload_id, parts, full_checksum, "commit-1")

    assert committed.accepted is True
    assert committed.size_bytes == item.size_bytes
    assert engine.head_object(execution.id, "destination", item.key).exists is True
    replay = engine.commit_multipart(upload_id, parts, full_checksum, "commit-1")
    assert replay.checksum_sha256 == committed.checksum_sha256


def test_destination_range_can_be_replayed_without_persisted_payload(virtual_cloud):
    engine, execution, _ = virtual_cloud
    item = engine.list_objects(execution.id, "source", "", None, 1).objects[0]
    engine.restore_object(execution.id, "source", item.key, "BULK", 1, "restore")
    source_chunks = list(
        engine.read_range(execution.id, "source", item.key, 0, item.size_bytes)
    )
    checksum = consume_and_discard(source_chunks).checksum_sha256
    engine.put_object(
        execution.id,
        "destination",
        item.key,
        iter(source_chunks),
        item.size_bytes,
        checksum,
        "put-audit",
    )
    destination = consume_and_discard(
        engine.read_range(execution.id, "destination", item.key, 0, item.size_bytes)
    )
    assert destination.checksum_sha256 == checksum


def test_data_budget_fails_closed_before_exceeding_limit():
    database = create_engine("sqlite+pysqlite:///:memory:")
    migrate(database)
    store = SimulatorStore(database)
    scenario = store.create_scenario(
        ScenarioCreate(
            name="budget",
            fidelity="DATA",
            seed="budget-seed",
            logical_size_bytes=100,
            physical_budget_bytes=50,
            configuration={"bulk_restore_min_hours": 0, "bulk_restore_max_hours": 0},
        )
    )
    engine = SimulationEngine(store)
    engine.materialize(
        scenario.id,
        source_bucket="source",
        destination_bucket="destination",
        region="us-east-1",
        object_count=1,
        logical_size_bytes=100,
        prefixes=["budget"],
        storage_class="DEEP_ARCHIVE",
    )
    execution = store.create_execution(scenario.id)
    item = engine.list_objects(execution.id, "source", "", None, 1).objects[0]
    engine.restore_object(execution.id, "source", item.key, "BULK", 1, "restore")

    with pytest.raises(RuntimeError, match="budget exhausted"):
        list(engine.read_range(execution.id, "source", item.key, 0, 51))


def test_fault_injection_records_exact_second_part_attempt():
    database = create_engine("sqlite+pysqlite:///:memory:")
    migrate(database)
    store = SimulatorStore(database)
    scenario = store.create_scenario(
        ScenarioCreate(
            name="retry-replay",
            fidelity="DATA",
            seed="fault-seed",
            logical_size_bytes=100,
            physical_budget_bytes=1000,
            configuration={"bulk_restore_min_hours": 0, "bulk_restore_max_hours": 0},
            fault_rules=[
                {
                    "operation": "UPLOAD_PART",
                    "action": "FAIL",
                    "part_number": 1,
                    "attempt": 2,
                }
            ],
        )
    )
    engine = SimulationEngine(store)
    engine.materialize(
        scenario.id,
        source_bucket="source",
        destination_bucket="destination",
        region="us-east-1",
        object_count=1,
        logical_size_bytes=100,
        prefixes=["fault"],
        storage_class="DEEP_ARCHIVE",
    )
    execution = store.create_execution(scenario.id)
    item = engine.list_objects(execution.id, "source", "", None, 1).objects[0]
    engine.restore_object(execution.id, "source", item.key, "BULK", 1, "restore")
    chunks = list(engine.read_range(execution.id, "source", item.key, 0, 100))
    checksum = consume_and_discard(chunks).checksum_sha256
    upload = engine.create_multipart(
        execution.id, "destination", item.key, 100, 100, None, "create"
    )

    engine.upload_part(upload, 1, iter(chunks), 100, checksum)
    with pytest.raises(ConnectionError, match="deterministic"):
        engine.upload_part(upload, 1, iter(chunks), 100, checksum)

    with store.sessions() as session:
        fault = session.scalar(select(InjectedFault))
        assert fault.attempt == 2
        assert fault.part_number == 1
        assert fault.seed == "fault-seed"


def test_batch_restore_preserves_manifest_and_paginated_per_object_evidence(virtual_cloud):
    engine, execution, _ = virtual_cloud
    page = engine.list_objects(execution.id, "source", "", None, 100)
    lines = [
        json.dumps(
            {"key": item.key, "version_id": item.version_id},
            sort_keys=True,
            separators=(",", ":"),
        ).encode() + b"\n"
        for item in page.objects
    ]
    manifest_sha = hashlib.sha256(b"".join(lines)).hexdigest()
    job = engine.create_restore_job(
        execution.id,
        "source",
        "BULK",
        2,
        len(lines),
        manifest_sha,
        "batch-idempotency",
    )
    engine.append_restore_job_objects(
        job.id, [(item.key, item.version_id) for item in page.objects[:3]]
    )
    engine.append_restore_job_objects(
        job.id, [(item.key, item.version_id) for item in page.objects[3:]]
    )
    completed = engine.finalize_restore_job(job.id, manifest_sha)

    assert completed.state == "COMPLETE"
    assert completed.accepted_count == len(lines)
    first = engine.describe_restore_job(job.id, None, 3)
    second = engine.describe_restore_job(job.id, first.next_continuation_token, 100)
    assert first.status == "COMPLETE"
    assert len(first.results) == 3
    assert len(second.results) == len(lines) - 3
    assert all(result.accepted for result in first.results + second.results)
