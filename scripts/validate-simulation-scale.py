#!/usr/bin/env python3
"""Validate a large logical CONTROL catalog without persisting payload bytes."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
import tempfile
import time

from sqlalchemy import create_engine

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.simulation_engine import SimulationEngine
from app.simulation_migrations import migrate
from app.simulator_store import ScenarioCreate, SimulatorStore


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--objects", type=int, default=640_000)
    parser.add_argument("--logical-bytes", type=int, default=100_000_000_000_000)
    parser.add_argument("--page-size", type=int, default=1_000)
    return parser.parse_args()


def main() -> None:
    args = arguments()
    if args.objects <= 0 or args.logical_bytes < 0 or not 1 <= args.page_size <= 1_000:
        raise SystemExit("objects must be positive, logical bytes non-negative, page size 1..1000")
    with tempfile.TemporaryDirectory(prefix="raijin-simulation-scale-") as directory:
        database = Path(directory) / "scale.db"
        engine = create_engine(f"sqlite+pysqlite:///{database}")
        migrate(engine)
        store = SimulatorStore(engine)
        simulation = SimulationEngine(store)
        scenario = store.create_scenario(
            ScenarioCreate(
                name="scale-validation",
                fidelity="CONTROL",
                seed="raijin-scale-validation-v1",
                logical_size_bytes=args.logical_bytes,
            )
        )
        print(
            f"START objects={args.objects} logical_bytes={args.logical_bytes}",
            flush=True,
        )
        started = time.monotonic()
        simulation.materialize(
            scenario.id,
            source_bucket="scale-source",
            destination_bucket="scale-destination",
            region="us-east-1",
            object_count=args.objects,
            logical_size_bytes=args.logical_bytes,
            prefixes=["scale/"],
            storage_class="DEEP_ARCHIVE",
        )
        materialized_seconds = time.monotonic() - started
        print(f"MATERIALIZED seconds={materialized_seconds:.3f}", flush=True)
        execution = store.create_execution(scenario.id)
        listed = listed_bytes = pages = 0
        token = None
        listing_started = time.monotonic()
        while True:
            page = simulation.list_objects(
                execution.id,
                "scale-source",
                "scale/",
                token,
                args.page_size,
            )
            listed += len(page.objects)
            listed_bytes += sum(item.size_bytes for item in page.objects)
            pages += 1
            if pages % 100 == 0:
                print(f"LIST_PROGRESS pages={pages} objects={listed}", flush=True)
            token = page.next_continuation_token
            if not token:
                break
        listing_seconds = time.monotonic() - listing_started
        if listed != args.objects or listed_bytes != args.logical_bytes:
            raise SystemExit(
                f"FAILED: listed {listed}/{args.objects} objects and "
                f"{listed_bytes}/{args.logical_bytes} bytes"
            )
        print(
            "PASS "
            f"objects={listed} logical_bytes={listed_bytes} pages={pages} "
            f"materialize_seconds={materialized_seconds:.3f} "
            f"list_seconds={listing_seconds:.3f} sqlite_bytes={database.stat().st_size}"
        )


if __name__ == "__main__":
    main()
