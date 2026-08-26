from sqlalchemy import create_engine, inspect, select
from sqlalchemy.orm import Session

from app.simulation_migrations import current_revision, migrate
from app.simulation_schema import (
    SIMULATION_SCHEMA_VERSION,
    GeneratorRelease,
    SimulationScenario,
    SimulationSchemaRevision,
    VirtualObject,
)


def test_simulation_schema_is_created_only_by_explicit_migration():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    assert current_revision(engine) == 0
    assert inspect(engine).get_table_names() == []

    assert migrate(engine) == SIMULATION_SCHEMA_VERSION

    names = set(inspect(engine).get_table_names())
    assert "sim_scenarios" in names
    assert "sim_virtual_objects" in names
    assert "sim_generator_releases" in names
    assert "sources" not in names
    assert "tasks" not in names


def test_simulation_migration_is_idempotent():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    migrate(engine)
    migrate(engine)

    with Session(engine) as session:
        revisions = session.scalars(select(SimulationSchemaRevision)).all()
    assert [item.version for item in revisions] == [1, 2, 3, 4]


def test_virtual_object_checksum_accepts_control_evidence_prefix():
    assert VirtualObject.__table__.c.source_sha256.type.length >= len("logical:" + "0" * 64)


def test_data_scenario_defaults_to_one_decimal_terabyte_budget():
    item = SimulationScenario(name="data", fidelity="DATA", seed="stable")
    assert item.physical_budget_bytes is None  # SQL defaults apply on persistence.

    engine = create_engine("sqlite+pysqlite:///:memory:")
    migrate(engine)
    with Session(engine) as session:
        session.add(item)
        session.commit()
        session.refresh(item)
        assert item.physical_budget_bytes == 1_000_000_000_000
