import json
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine

from app.simulation_migrations import migrate
from app.simulation_engine import SimulationEngine
from app.simulation_schema import GeneratorRelease, SimulationTombstone, VirtualBucket, VirtualObject
from app.simulator_store import ScenarioCreate, SimulatorStore, TemplateWrite
from sqlalchemy import func, select


@pytest.fixture
def store():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    migrate(engine)
    return SimulatorStore(engine)


def test_control_scenario_has_no_physical_data_budget(store):
    item = store.create_scenario(
        ScenarioCreate(
            name="100 TB scheduler",
            fidelity="CONTROL",
            seed="control-1",
            logical_size_bytes=100_000_000_000_000,
        )
    )

    assert item.logical_size_bytes == 100_000_000_000_000
    assert item.physical_budget_bytes == 0


def test_data_scenario_keeps_configured_unbounded_budget(store):
    item = store.create_scenario(
        ScenarioCreate(
            name="multipart path",
            fidelity="DATA",
            seed="data-1",
            logical_size_bytes=3_000_000_000_000,
            physical_budget_bytes=2_000_000_000_000,
        )
    )

    assert item.physical_budget_bytes == 2_000_000_000_000


def test_execution_contains_immutable_scenario_snapshot(store):
    scenario = store.create_scenario(
        ScenarioCreate(
            name="slow restore",
            fidelity="CONTROL",
            seed="restore-seed",
            logical_size_bytes=10_000,
            configuration={"restore_hours": 48},
            fault_rules=[{"type": "RESTORE_DELAY", "hours": 6}],
        )
    )

    execution = store.create_execution(scenario.id)
    snapshot = json.loads(execution.immutable_snapshot_json)

    assert snapshot["scenario_id"] == scenario.id
    assert snapshot["configuration"] == {"restore_hours": 48}
    assert snapshot["fault_rules"] == [{"hours": 6, "type": "RESTORE_DELAY"}]
    assert execution.correlation_id


def test_editable_template_never_changes_existing_execution_snapshot(store):
    template = store.create_template(TemplateWrite(
        name="Editable network",
        description="Initial profile",
        fidelity="CONTROL",
        configuration={"network_throughput_mbps": 1100},
        fault_rules=[],
    ))
    scenario = store.create_scenario(ScenarioCreate(
        name="template-snapshot",
        fidelity="DATA",  # Template fidelity is authoritative.
        seed="template-seed",
        logical_size_bytes=10,
        template_id=template.id,
    ))
    execution = store.create_execution(scenario.id)
    before = json.loads(execution.immutable_snapshot_json)

    store.update_template(template.id, TemplateWrite(
        name="Editable network",
        description="Degraded profile",
        fidelity="CONTROL",
        configuration={"network_throughput_mbps": 250},
        fault_rules=[{"operation": "LOGICAL_TRANSFER", "action": "TIMEOUT"}],
    ))
    after = json.loads(store.get_execution(execution.id).immutable_snapshot_json)

    assert before == after
    assert before["configuration"]["network_throughput_mbps"] == 1100
    assert before["template_snapshot"]["description"] == "Initial profile"


def test_scenario_contract_rejects_invalid_values(store):
    with pytest.raises(ValueError, match="CONTROL or DATA"):
        store.create_scenario(
            ScenarioCreate(
                name="invalid", fidelity="REAL", seed="x", logical_size_bytes=1
            )
        )


def test_housekeeping_uses_deprecation_and_quarantine_before_purge_eligibility(store):
    scenario = store.create_scenario(
        ScenarioCreate(name="lifecycle", fidelity="CONTROL", seed="life", logical_size_bytes=1)
    )
    execution = store.create_execution(scenario.id)
    store.set_execution_state(execution.id, "RUNNING")
    finished = store.set_execution_state(execution.id, "SUCCEEDED")

    first = store.apply_housekeeping(finished.real_finished_at + timedelta(days=61))
    assert first == {"deprecated": 1, "purge_eligible": 0, "purged": 0}
    deprecated = store.get_scenario(scenario.id)
    assert deprecated.state == "DEPRECATED"

    second = store.apply_housekeeping(deprecated.deprecated_at + timedelta(days=31))
    assert second == {"deprecated": 0, "purge_eligible": 1, "purged": 0}
    assert store.get_scenario(scenario.id).state == "PURGE_ELIGIBLE"

    restored = store.restore_scenario(scenario.id)
    assert restored.state == "ACTIVE"
    assert restored.deprecated_at is None


def test_housekeeping_never_deprecates_active_execution(store):
    scenario = store.create_scenario(
        ScenarioCreate(name="still-running", fidelity="CONTROL", seed="active", logical_size_bytes=1)
    )
    store.create_execution(scenario.id)
    result = store.apply_housekeeping(datetime.now(timezone.utc) + timedelta(days=365))
    assert result["deprecated"] == 0
    assert store.get_scenario(scenario.id).state == "ACTIVE"


def test_generator_release_is_not_removable_while_a_scenario_references_it(store):
    scenario = store.create_scenario(
        ScenarioCreate(name="legacy-generator", fidelity="CONTROL", seed="legacy", logical_size_bytes=1)
    )
    with store.sessions() as session:
        scenario_row = session.get(type(scenario), scenario.id)
        scenario_row.generator_version = "legacy-generator-v0"
        session.add(GeneratorRelease(version="legacy-generator-v0", state="ACTIVE"))
        session.commit()

    now = datetime.now(timezone.utc)
    assert store.apply_generator_housekeeping(now) == {"deprecated": 0, "purge_eligible": 0}
    legacy = next(item for item in store.list_generator_releases() if item["version"] == "legacy-generator-v0")
    assert legacy["state"] == "ACTIVE"
    assert legacy["referenced_scenarios"] == 1

    with store.sessions() as session:
        session.get(type(scenario), scenario.id).state = "PURGED"
        session.commit()
    assert store.apply_generator_housekeeping(now) == {"deprecated": 1, "purge_eligible": 0}
    assert store.apply_generator_housekeeping(now + timedelta(days=31)) == {"deprecated": 0, "purge_eligible": 1}
    legacy = next(item for item in store.list_generator_releases() if item["version"] == "legacy-generator-v0")
    assert legacy["state"] == "PURGE_ELIGIBLE"
    assert legacy["referenced_scenarios"] == 0


def test_physical_purge_requires_quarantine_and_retains_tombstone(store):
    scenario = store.create_scenario(
        ScenarioCreate(
            name="purge-after-quarantine",
            fidelity="CONTROL",
            seed="purge-seed",
            logical_size_bytes=1024,
            retention_days=0,
            quarantine_days=0,
        )
    )
    SimulationEngine(store).materialize(
        scenario.id,
        source_bucket="source",
        destination_bucket="destination",
        region="us-east-1",
        object_count=2,
        logical_size_bytes=1024,
        prefixes=["test/"],
        storage_class="DEEP_ARCHIVE",
    )
    execution = store.create_execution(scenario.id)
    store.set_execution_state(execution.id, "RUNNING")
    store.set_execution_state(execution.id, "SUCCEEDED")
    store.apply_housekeeping(datetime.now(timezone.utc) + timedelta(seconds=1))
    store.apply_housekeeping(datetime.now(timezone.utc) + timedelta(seconds=2))

    result = store.purge_scenario(scenario.id, "Controlled cleanup after completed test")

    assert result["state"] == "PURGED"
    assert result["evidence"]["virtual_objects"] == 2
    assert len(result["evidence_sha256"]) == 64
    assert store.get_execution(execution.id).state == "SUCCEEDED"
    with store.sessions() as session:
        assert session.scalar(select(func.count(VirtualObject.id))) == 0
        assert session.scalar(select(func.count(VirtualBucket.id))) == 0
        tombstone = session.scalar(select(SimulationTombstone))
        assert tombstone.resource_id == scenario.id
        assert tombstone.evidence_sha256 == result["evidence_sha256"]
