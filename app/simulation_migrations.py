"""Explicit, idempotent migrations for the simulation catalog."""

from __future__ import annotations

from sqlalchemy import Engine, inspect, select
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
    return SIMULATION_SCHEMA_VERSION
