"""Explicit, idempotent migrations for the simulation catalog."""

from __future__ import annotations

from sqlalchemy import Engine, inspect, select, text
from sqlalchemy.orm import Session

from app.simulation_schema import (
    GeneratorRelease,
    SIMULATION_SCHEMA_VERSION,
    SimulationBase,
    SimulationSchemaRevision,
)


class SimulationMigrationError(RuntimeError):
    pass


def current_revision(engine: Engine) -> int:
    if not inspect(engine).has_table(SimulationSchemaRevision.__tablename__):
        return 0
    with Session(engine) as session:
        return int(
            session.scalar(select(SimulationSchemaRevision.version).order_by(
                SimulationSchemaRevision.version.desc()
            ))
            or 0
        )


def migrate(engine: Engine) -> int:
    revision = current_revision(engine)
    if revision > SIMULATION_SCHEMA_VERSION:
        raise SimulationMigrationError(
            f"Database revision {revision} is newer than supported revision "
            f"{SIMULATION_SCHEMA_VERSION}"
        )
    if revision == SIMULATION_SCHEMA_VERSION:
        return revision

    # Migrations are append-only and explicit. They never mutate production
    # tables and never run as a side effect of API startup.
    if revision < 1:
        SimulationBase.metadata.create_all(engine)
        with Session(engine) as session:
            session.add(
                SimulationSchemaRevision(
                    version=1,
                    description="Initial isolated simulator catalog",
                )
            )
            session.commit()
        revision = 1
    if revision < 2:
        GeneratorRelease.__table__.create(engine, checkfirst=True)
        with Session(engine) as session:
            session.add(
                SimulationSchemaRevision(
                    version=2,
                    description="Deterministic generator release lifecycle",
                )
            )
            session.commit()
        revision = 2
    if revision < 3:
        # SQLite does not enforce VARCHAR lengths and fresh schemas already use
        # the current model. PostgreSQL needs an explicit widening migration
        # for catalogs created before logical checksums gained their prefix.
        if engine.dialect.name == "postgresql":
            with engine.begin() as connection:
                connection.execute(text(
                    "ALTER TABLE sim_virtual_objects "
                    "ALTER COLUMN source_sha256 TYPE VARCHAR(128)"
                ))
        with Session(engine) as session:
            session.add(
                SimulationSchemaRevision(
                    version=3,
                    description="Allow prefixed logical SHA-256 evidence",
                )
            )
            session.commit()
    return SIMULATION_SCHEMA_VERSION
