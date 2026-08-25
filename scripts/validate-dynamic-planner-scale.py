#!/usr/bin/env python3
"""Validate the RAIJIN dynamic planner with a 100 TB / 640k-object catalog."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys
import tempfile
import time

from sqlalchemy import BigInteger, create_engine, func, select
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker


@compiles(BigInteger, "sqlite")
def _compile_big_integer_as_sqlite_integer(_type, _compiler, **_kwargs):
    return "INTEGER"


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--objects", type=int, default=640_000)
    parser.add_argument("--logical-bytes", type=int, default=100_000_000_000_000)
    parser.add_argument("--restore-days", type=int, default=2)
    return parser.parse_args()


def main() -> None:
    args = arguments()
    if args.objects <= 0 or args.logical_bytes < 0 or args.restore_days <= 0:
        raise SystemExit("objects and restore-days must be positive; logical-bytes cannot be negative")
    with tempfile.TemporaryDirectory(prefix="raijin-planner-scale-") as directory:
        password = Path(directory) / "postgres-password"
        password.write_text("scale-test", encoding="utf-8")
        database = Path(directory) / "control-plane.db"
        os.environ["DATABASE_URL"] = f"sqlite+pysqlite:///{database}"
        os.environ["POSTGRES_PASSWORD_FILE"] = str(password)
        os.environ.setdefault("OCI_RUNTIME_CONFIG_FILE", str(Path(directory) / "oci.json"))

        from app.main import (
            Base,
            DynamicWaveCreate,
            ObjectRecord,
            ObjectState,
            Source,
            Wave,
            create_dynamic_waves,
        )

        engine = create_engine(os.environ["DATABASE_URL"])
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        quotient, remainder = divmod(args.logical_bytes, args.objects)
        started = time.monotonic()
        with Session() as session:
            source = Source(
                id=1,
                name="dynamic-planner-scale",
                s3_bucket="logical-source",
                aws_region="us-east-1",
                destination_bucket="logical-destination",
                status="DISCOVERED",
            )
            session.add(source)
            session.commit()
            for start in range(0, args.objects, 10_000):
                end = min(args.objects, start + 10_000)
                session.bulk_insert_mappings(ObjectRecord, [
                    {
                        "id": index + 1,
                        "source_id": source.id,
                        "object_key": f"scale/object-{index:09d}.bin",
                        "size_bytes": quotient + (1 if index < remainder else 0),
                        "state": ObjectState.DISCOVERED,
                        "is_current_revision": True,
                    }
                    for index in range(start, end)
                ])
                session.commit()
            inserted_seconds = time.monotonic() - started
            planned_at = time.monotonic()
            result = create_dynamic_waves(
                source.id,
                DynamicWaveCreate(
                    restore_days=args.restore_days,
                    restore_tier="BULK",
                    schedule_restores=False,
                ),
                session,
            )
            planned_seconds = time.monotonic() - planned_at
            assigned, distinct_objects, assigned_bytes = session.execute(select(
                func.count(ObjectRecord.id),
                func.count(func.distinct(ObjectRecord.id)),
                func.coalesce(func.sum(ObjectRecord.size_bytes), 0),
            ).where(
                ObjectRecord.source_id == source.id,
                ObjectRecord.wave_id.is_not(None),
                ObjectRecord.state == ObjectState.WAVE_ASSIGNED,
            )).one()
            waves = int(session.scalar(select(func.count(Wave.id)).where(
                Wave.source_id == source.id
            )) or 0)
            if (
                int(assigned) != args.objects
                or int(distinct_objects) != args.objects
                or int(assigned_bytes) != args.logical_bytes
                or result["objects"] != args.objects
                or result["bytes"] != args.logical_bytes
            ):
                raise SystemExit(
                    f"FAILED assigned={assigned}/{args.objects} distinct={distinct_objects} "
                    f"bytes={assigned_bytes}/{args.logical_bytes}"
                )
        print(
            "PASS "
            f"objects={args.objects} logical_bytes={args.logical_bytes} waves={waves} "
            f"insert_seconds={inserted_seconds:.3f} planner_seconds={planned_seconds:.3f} "
            f"sqlite_bytes={database.stat().st_size}"
        )


if __name__ == "__main__":
    main()
