#!/usr/bin/env python3
"""Execute every built-in simulator template with its registered stable seed."""

from __future__ import annotations

from pathlib import Path
import json
import re
import sys
import tempfile

from sqlalchemy import create_engine

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.simulated_data import SimulatedDataIntegrityError, consume_and_discard
from app.simulation_engine import SimulationEngine
from app.simulation_migrations import migrate
from app.simulator_store import ScenarioCreate, SimulatorStore


REGISTERED_TEMPLATE_SEEDS = {
    "Ideal control": "raijin-template-ideal-control-v1",
    "Rapid restore": "raijin-template-rapid-restore-v1",
    "Slow restore": "raijin-template-slow-restore-v1",
    "Gradual restore": "raijin-template-gradual-restore-v1",
    "Degraded network": "raijin-template-degraded-network-v1",
    "Oscillating network": "raijin-template-oscillating-network-v1",
    "Partial restore failures": "raijin-template-partial-restore-v1",
    "Insufficient retention": "raijin-template-insufficient-retention-v1",
    "Multipart interrupted": "raijin-template-multipart-interrupted-v1",
    "Deterministic corruption": "raijin-template-corruption-v1",
    "Transient throttling": "raijin-template-throttling-v1",
}


def slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def list_all(cloud: SimulationEngine, execution_id: str, bucket: str) -> list:
    rows = []
    token = None
    while True:
        page = cloud.list_objects(execution_id, bucket, "", token, 100)
        rows.extend(page.objects)
        token = page.next_continuation_token
        if not token:
            return rows


def execute_data_path(
    cloud: SimulationEngine,
    execution_id: str,
    item,
    destination_bucket: str,
) -> None:
    part_size = max(1, (item.size_bytes + 1) // 2)
    upload_id = cloud.create_multipart(
        execution_id,
        destination_bucket,
        item.key,
        item.size_bytes,
        part_size,
        None,
        f"{execution_id}-upload-{item.key}",
    )
    parts = []
    offset = 0
    part_number = 1
    while offset < item.size_bytes:
        length = min(part_size, item.size_bytes - offset)
        last_error = None
        for attempt in range(1, 4):
            chunks = list(cloud.read_range(
                execution_id, item.bucket, item.key, offset, length
            ))
            checksum = consume_and_discard(chunks).checksum_sha256
            try:
                evidence = cloud.upload_part(
                    upload_id, part_number, iter(chunks), length, checksum, attempt
                )
                parts.append(evidence)
                break
            except (TimeoutError, ConnectionError, SimulatedDataIntegrityError) as error:
                last_error = error
        else:
            raise RuntimeError(
                f"Multipart template path did not recover part {part_number}: {last_error}"
            )
        offset += length
        part_number += 1
    clean = list(cloud.read_range(
        execution_id, item.bucket, item.key, 0, item.size_bytes
    ))
    full_checksum = consume_and_discard(clean).checksum_sha256
    cloud.commit_multipart(
        upload_id, parts, full_checksum, f"{execution_id}-commit-{item.key}"
    )


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="raijin-template-validation-") as directory:
        engine = create_engine(f"sqlite+pysqlite:///{Path(directory) / 'templates.db'}")
        migrate(engine)
        store = SimulatorStore(engine)
        store.ensure_generator_release()
        store.ensure_default_templates()
        templates = {item.name: item for item in store.list_templates()}
        missing = set(templates) ^ set(REGISTERED_TEMPLATE_SEEDS)
        if missing:
            raise SystemExit(f"Template seed registry mismatch: {sorted(missing)}")

        results = []
        for name, seed in REGISTERED_TEMPLATE_SEEDS.items():
            template = templates[name]
            object_count = 100 if name == "Partial restore failures" else 8
            logical_size_bytes = 102_400 if object_count > 8 else 65_536
            scenario_name = f"validation-{slug(name)}"
            scenario = store.create_scenario(ScenarioCreate(
                name=scenario_name,
                fidelity=template.fidelity,
                seed=seed,
                logical_size_bytes=logical_size_bytes,
                physical_budget_bytes=1_048_576,
                clock_acceleration=3600,
                retention_days=0,
                quarantine_days=0,
                template_id=template.id,
            ))
            source_bucket = f"source-{slug(name)}"
            destination_bucket = f"destination-{slug(name)}"
            cloud = SimulationEngine(store)
            cloud.materialize(
                scenario.id,
                source_bucket=source_bucket,
                destination_bucket=destination_bucket,
                region="us-east-1",
                object_count=object_count,
                logical_size_bytes=logical_size_bytes,
                prefixes=["validation"],
                storage_class="DEEP_ARCHIVE",
            )
            execution = store.create_execution(scenario.id)
            store.set_execution_state(execution.id, "RUNNING")
            objects = list_all(cloud, execution.id, source_bucket)
            if len(objects) != object_count:
                raise RuntimeError(
                    f"{name}: catalog listed {len(objects)} of {object_count} objects"
                )
            accepted = 0
            for position, item in enumerate(objects):
                result = cloud.restore_object(
                    execution.id,
                    source_bucket,
                    item.key,
                    "BULK",
                    2,
                    f"template-restore-{position}",
                )
                accepted += int(result.accepted)
            configuration = json.loads(template.configuration_json)
            maximum_restore_hours = float(
                configuration.get(
                    "bulk_restore_max_hours",
                    configuration.get("bulk_restore_min_hours", 48),
                )
            )
            store.control_clock(execution.id, "PAUSE")
            store.control_clock(
                execution.id, "ADVANCE", (maximum_restore_hours + 0.25) * 3600
            )
            available = [
                item for item in objects
                if cloud.head_object(execution.id, source_bucket, item.key).restore_expires_at
            ]
            if len(available) != accepted:
                raise RuntimeError(
                    f"{name}: {accepted} restores accepted but {len(available)} available"
                )
            transferred = 0
            for item in available:
                if template.fidelity == "CONTROL":
                    cloud.transfer_logically(
                        execution.id,
                        source_bucket,
                        destination_bucket,
                        item.key,
                        item.size_bytes,
                        f"template-transfer-{item.key}",
                    )
                else:
                    execute_data_path(cloud, execution.id, item, destination_bucket)
                transferred += 1
            store.set_execution_state(execution.id, "SUCCEEDED")
            report = store.execution_report(execution.id)
            if report["state"] != "SUCCEEDED" or report["virtual_objects"] < len(objects):
                raise RuntimeError(f"{name}: incomplete execution evidence")
            if json.loads(template.fault_rules_json) and not report["fault_counts"]:
                raise RuntimeError(f"{name}: registered fault profile was not exercised")
            results.append((name, seed, accepted, transferred))
            print(
                f"PASS template={name!r} seed={seed} "
                f"restore_accepted={accepted} transferred={transferred}",
                flush=True,
            )
        print(
            f"PASS templates={len(results)} registered_seeds={len(results)}",
            flush=True,
        )


if __name__ == "__main__":
    main()
