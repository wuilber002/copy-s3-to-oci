#!/usr/bin/env python3
"""Apply the simulator catalog migration to migration_simulation explicitly."""

import os

from sqlalchemy import create_engine

from app.runtime_context import OperationMode, RuntimeContext
from app.simulation_migrations import migrate
from app.simulator_store import simulator_database_url


def main() -> None:
    context = RuntimeContext.from_environ()
    context.validate()
    if context.mode is not OperationMode.SIMULATION:
        raise SystemExit("Refusing migration: RAIJIN_OPERATION_MODE must be SIMULATION")
    revision = migrate(create_engine(simulator_database_url(), pool_pre_ping=True))
    print(f"Simulation schema is at revision {revision}")


if __name__ == "__main__":
    main()
