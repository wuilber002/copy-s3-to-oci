"""Deterministic virtual cloud behavior used by the simulator HTTP service."""

from __future__ import annotations

import base64
from datetime import datetime, timedelta, timezone
import hashlib
import json
from typing import Iterator

from sqlalchemy import and_, func, select, update

from app.backend_contracts import (
    HeadObjectResult,
    ListObjectsPage,
    ObjectDescriptor,
    RestoreObjectResult,
    DescribeRestoreBatchResult,
    RestoreBatchObjectResult,
    MultipartPartEvidence,
    WriteEvidence,
    LogicalTransferResult,
)
from app.simulation_schema import (
    SimulationClock,
    SimulationExecution,
    InjectedFault,
    SimulatedOperation,
    SimulatedRestoreJob,
    SimulatedRestoreObjectResult,
    SimulationScenario,
    SimulatedMultipartPart,
    SimulatedMultipartUpload,
    VirtualBucket,
    VirtualObject,
)
from app.simulated_data import (
    SimulatedDataIntegrityError,
    VirtualContentDescriptor,
    consume_and_discard,
    deterministic_sha256,
    iter_deterministic_range,
)


def aware(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def virtual_now(clock: SimulationClock, real_now: datetime | None = None) -> datetime:
    if clock.paused and clock.paused_virtual_at:
        return aware(clock.paused_virtual_at)
    if int(clock.hold_count or 0) > 0 and clock.held_virtual_at:
        return aware(clock.held_virtual_at)
    current = real_now or datetime.now(timezone.utc)
    elapsed = (current - aware(clock.real_anchor_at)).total_seconds()
    return aware(clock.virtual_anchor_at) + timedelta(seconds=elapsed * clock.acceleration)


def _fraction(seed: str) -> float:
    raw = hashlib.sha256(seed.encode("utf-8")).digest()[:8]
    return int.from_bytes(raw, "big") / ((1 << 64) - 1)


def _weight(seed: str, index: int) -> int:
    """Stable heterogeneous size profile: 60/25/10/5 percent."""
    value = _fraction(f"{seed}:size-band:{index}")
    if value < 0.60:
        return 1
    if value < 0.85:
        return 64
    if value < 0.95:
        return 1024
    return 8192


def _is_archival_storage(storage_class: str) -> bool:
    return storage_class.upper() in {
        "GLACIER",
        "DEEP_ARCHIVE",
        "GLACIER_IR",
        "INTELLIGENT_TIERING_ARCHIVE_ACCESS",
        "INTELLIGENT_TIERING_DEEP_ARCHIVE_ACCESS",
    }


def _restore_is_available(item: VirtualObject, now: datetime) -> bool:
    if not _is_archival_storage(item.storage_class):
        return True
    return bool(
        item.restore_available_at
        and aware(item.restore_available_at) <= now
        and (
            item.restore_expires_at is None
            or now < aware(item.restore_expires_at)
        )
    )


def _expire_restore_if_needed(item: VirtualObject, now: datetime) -> bool:
    if (
        _is_archival_storage(item.storage_class)
        and item.restore_expires_at
        and now >= aware(item.restore_expires_at)
    ):
        item.restore_state = "EXPIRED"
        return True
    return False


def network_conditions(
    execution: SimulationExecution,
    clock: SimulationClock,
    configuration: dict,
    operation_seed: str,
) -> tuple[float, float, bool]:
    """Resolve deterministic network conditions at the current virtual time.

    ``network_profile`` is an ordered list of virtual-hour windows. A profile
    can be cyclic with ``network_profile_period_hours``. This keeps an
    execution reproducible while allowing degradation, outage and recovery.
    """
    throughput = float(configuration.get("network_throughput_mbps", 1100))
    latency_ms = float(configuration.get("per_object_latency_ms", 5))
    unavailable = False
    current = virtual_now(clock)
    anchor = execution.virtual_started_at or clock.virtual_anchor_at
    elapsed_hours = max(0.0, (current - aware(anchor)).total_seconds() / 3600)
    period = float(configuration.get("network_profile_period_hours", 0) or 0)
    profile_hour = elapsed_hours % period if period > 0 else elapsed_hours
    for window in configuration.get("network_profile", []):
        start = float(window.get("start_hour", 0))
        end = float(window.get("end_hour", float("inf")))
        if start <= profile_hour < end:
            throughput = float(window.get("throughput_mbps", throughput))
            latency_ms = float(window.get("latency_ms", latency_ms))
            unavailable = bool(window.get("unavailable", False))
            break
    jitter = max(0.0, float(configuration.get("network_jitter_percent", 0)))
    if jitter:
        factor = 1 + ((_fraction(operation_seed) * 2) - 1) * jitter / 100
        throughput *= max(0.001, factor)
    return max(0.001, throughput), max(0.0, latency_ms) / 1000, unavailable


def _descriptor(item: VirtualObject, bucket_name: str) -> ObjectDescriptor:
    return ObjectDescriptor(
        bucket=bucket_name,
        key=item.object_key,
        version_id=item.version_id,
        size_bytes=item.size_bytes,
        storage_class=item.storage_class,
        etag=f'"sim-{item.content_object_id or item.id}"',
        last_modified=item.last_modified,
        checksum_algorithm="SHA256" if item.source_sha256 else None,
        checksum=item.source_sha256,
        metadata=json.loads(item.metadata_json),
        tags=json.loads(item.tags_json),
    )


class SimulationEngine:
    def __init__(self, store):
        self.store = store

    def _execution_scope(self, session, execution_id: str):
        execution = session.get(SimulationExecution, execution_id)
        if execution is None:
            raise LookupError("Simulation execution not found")
        scenario = session.get(SimulationScenario, execution.scenario_id)
        clock = session.get(SimulationClock, execution.id)
        if scenario is None or clock is None:
            raise RuntimeError("Simulation execution has an incomplete persisted snapshot")
        return execution, scenario, clock

    @staticmethod
    def _matching_fault(
        session,
        execution: SimulationExecution,
        scenario: SimulationScenario,
        clock: SimulationClock,
        operation: str,
        *,
        object_id: str | None = None,
        object_key: str | None = None,
        part_number: int | None = None,
    ) -> tuple[dict | None, int]:
        snapshot = json.loads(execution.immutable_snapshot_json)
        attempt = int(
            session.scalar(
                select(func.count(SimulatedOperation.id)).where(
                    SimulatedOperation.execution_id == execution.id,
                    SimulatedOperation.operation == operation,
                    SimulatedOperation.object_id == object_id,
                    SimulatedOperation.part_number == part_number,
                )
            )
            or 0
        ) + 1
        operation_record = SimulatedOperation(
            execution_id=execution.id,
            operation=operation,
            object_id=object_id,
            part_number=part_number,
            attempt=attempt,
            virtual_at=virtual_now(clock),
        )
        session.add(operation_record)
        for index, rule in enumerate(snapshot.get("fault_rules", [])):
            if str(rule.get("operation", "")).upper() != operation.upper():
                continue
            if rule.get("object_key_contains") and str(rule["object_key_contains"]) not in str(object_key or ""):
                continue
            if rule.get("part_number") is not None and int(rule["part_number"]) != part_number:
                continue
            if rule.get("attempt") is not None and int(rule["attempt"]) != attempt:
                continue
            probability = float(rule.get("probability", 1.0))
            if not 0 <= probability <= 1:
                raise ValueError("Fault probability must be between zero and one")
            # Object database IDs intentionally differ between cloned
            # scenarios. Use the stable object key so seed + snapshot replay
            # selects the exact same probabilistic fault sequence.
            stable_object_identity = object_key or object_id or ""
            score = _fraction(
                f"{scenario.seed}:fault:{index}:{operation}:"
                f"{stable_object_identity}:{part_number}:{attempt}"
            )
            if score > probability:
                continue
            action = str(rule.get("action") or "FAIL").upper()
            operation_record.fault_injected = True
            session.add(
                InjectedFault(
                    execution_id=execution.id,
                    seed=scenario.seed,
                    fault_type=action,
                    object_id=object_id,
                    part_number=part_number,
                    attempt=attempt,
                    operation=operation,
                    virtual_at=virtual_now(clock),
                    details_json=json.dumps(rule, sort_keys=True, separators=(",", ":")),
                )
            )
            session.flush()
            return rule | {"action": action}, attempt
        return None, attempt

    @staticmethod
    def _reserve_physical_bytes(session, execution: SimulationExecution, amount: int) -> None:
        if amount < 0:
            raise ValueError("Physical byte reservation cannot be negative")
        snapshot = json.loads(execution.immutable_snapshot_json)
        if snapshot.get("fidelity") != "DATA":
            raise ValueError("Byte streaming is available only in DATA scenarios")
        result = session.execute(
            update(SimulationExecution)
            .where(
                SimulationExecution.id == execution.id,
                SimulationExecution.physical_bytes_processed + amount
                <= SimulationExecution.physical_budget_bytes,
            )
            .values(
                physical_bytes_processed=SimulationExecution.physical_bytes_processed + amount
            )
        )
        if result.rowcount != 1:
            raise RuntimeError(
                "DATA physical byte budget exhausted; increase it before creating a new scenario"
            )

    def materialize(
        self,
        scenario_id: str,
        *,
        source_bucket: str,
        destination_bucket: str,
        region: str,
        object_count: int,
        logical_size_bytes: int,
        prefixes: list[str],
        storage_class: str,
    ) -> dict:
        if object_count <= 0:
            raise ValueError("object_count must be positive")
        if logical_size_bytes < 0:
            raise ValueError("logical_size_bytes cannot be negative")
        normalized_prefixes = [value.strip().strip("/") for value in prefixes if value.strip()]
        normalized_prefixes = normalized_prefixes or ["simulation"]

        with self.store.sessions() as session:
            scenario = session.get(SimulationScenario, scenario_id)
            if scenario is None:
                raise LookupError("Scenario not found")
            if session.scalar(select(SimulationExecution.id).where(
                SimulationExecution.scenario_id == scenario_id
            ).limit(1)):
                raise ValueError("Scenario catalog is immutable after an execution exists")
            existing = session.scalar(
                select(func.count(VirtualBucket.id)).where(VirtualBucket.scenario_id == scenario_id)
            )
            if existing:
                raise ValueError("Scenario catalog is already materialized and immutable")

            source = VirtualBucket(
                scenario_id=scenario_id, provider="AWS", region=region, name=source_bucket
            )
            destination = VirtualBucket(
                scenario_id=scenario_id, provider="OCI", region=region, name=destination_bucket
            )
            session.add_all([source, destination])
            session.flush()

            weight_total = sum(_weight(scenario.seed, index) for index in range(object_count))
            cumulative_weight = 0
            allocated = 0
            rows: list[dict] = []
            for index in range(object_count):
                cumulative_weight += _weight(scenario.seed, index)
                next_allocated = logical_size_bytes * cumulative_weight // weight_total
                size = next_allocated - allocated
                allocated = next_allocated
                prefix = normalized_prefixes[index % len(normalized_prefixes)]
                content_object_id = hashlib.sha256(
                    f"{scenario.seed}:{index}".encode("utf-8")
                ).hexdigest()[:32]
                object_id = hashlib.sha256(
                    f"{scenario.id}:{content_object_id}".encode("utf-8")
                ).hexdigest()[:32]
                rows.append(
                    {
                        "id": object_id,
                        "scenario_id": scenario_id,
                        "bucket_id": source.id,
                        "object_key": f"{prefix}/object-{index + 1:012d}.bin",
                        "version_id": "v1",
                        "size_bytes": size,
                        "storage_class": storage_class,
                        "last_modified": scenario.created_at,
                        "metadata_json": "{}",
                        "tags_json": "{}",
                        "generator_version": scenario.generator_version,
                        "content_seed": scenario.seed,
                        "content_object_id": content_object_id,
                        "restore_state": "ARCHIVED",
                    }
                )
                if len(rows) == 5000:
                    session.bulk_insert_mappings(VirtualObject, rows)
                    rows.clear()
            if rows:
                session.bulk_insert_mappings(VirtualObject, rows)
            scenario.logical_size_bytes = logical_size_bytes
            configuration = json.loads(scenario.configuration_json)
            configuration["_materialization"] = {
                "source_bucket": source_bucket,
                "destination_bucket": destination_bucket,
                "region": region,
                "object_count": object_count,
                "logical_size_bytes": logical_size_bytes,
                "prefixes": normalized_prefixes,
                "storage_class": storage_class,
            }
            scenario.configuration_json = json.dumps(
                configuration, sort_keys=True, separators=(",", ":")
            )
            session.commit()
            return {
                "source_bucket": source.name,
                "destination_bucket": destination.name,
                "objects": object_count,
                "logical_size_bytes": logical_size_bytes,
            }

    def list_objects(
        self,
        execution_id: str,
        bucket_name: str,
        prefix: str,
        continuation_token: str | None,
        max_keys: int,
    ) -> ListObjectsPage:
        with self.store.sessions() as session:
            _, scenario, _ = self._execution_scope(session, execution_id)
            bucket = session.scalar(
                select(VirtualBucket).where(
                    VirtualBucket.scenario_id == scenario.id,
                    VirtualBucket.name == bucket_name,
                )
            )
            if bucket is None:
                raise LookupError("Virtual bucket not found")
            conditions = [VirtualObject.bucket_id == bucket.id]
            if prefix:
                conditions.append(VirtualObject.object_key.startswith(prefix))
            if continuation_token:
                try:
                    last_key = base64.urlsafe_b64decode(
                        continuation_token.encode("ascii")
                    ).decode("utf-8")
                except Exception as error:
                    raise ValueError("Invalid continuation token") from error
                conditions.append(VirtualObject.object_key > last_key)
            items = list(
                session.scalars(
                    select(VirtualObject)
                    .where(and_(*conditions))
                    .order_by(VirtualObject.object_key)
                    .limit(max_keys + 1)
                ).all()
            )
            has_more = len(items) > max_keys
            visible = items[:max_keys]
            token = None
            if has_more and visible:
                token = base64.urlsafe_b64encode(
                    visible[-1].object_key.encode("utf-8")
                ).decode("ascii")
            return ListObjectsPage(
                objects=[_descriptor(item, bucket.name) for item in visible],
                next_continuation_token=token,
            )

    def head_object(self, execution_id: str, bucket_name: str, key: str) -> HeadObjectResult:
        with self.store.sessions() as session:
            execution, scenario, clock = self._execution_scope(session, execution_id)
            bucket = session.scalar(
                select(VirtualBucket).where(
                    VirtualBucket.scenario_id == scenario.id,
                    VirtualBucket.name == bucket_name,
                )
            )
            if bucket is None:
                return HeadObjectResult(exists=False)
            item = session.scalar(
                select(VirtualObject).where(
                    VirtualObject.bucket_id == bucket.id,
                    VirtualObject.object_key == key,
                )
            )
            if item is None:
                return HeadObjectResult(exists=False)
            now = virtual_now(clock)
            expired = _expire_restore_if_needed(item, now)
            available = _restore_is_available(item, now)
            in_progress = bool(item.restore_requested_at and not available and not expired)
            if expired:
                session.commit()
            elif available and item.restore_state != "AVAILABLE":
                item.restore_state = "AVAILABLE"
                session.commit()
            return HeadObjectResult(
                exists=True,
                descriptor=_descriptor(item, bucket.name),
                restore_in_progress=in_progress,
                restore_expires_at=item.restore_expires_at if available else None,
                simulator_virtual_now=now,
                simulator_recommended_real_poll_seconds=(
                    max(
                        0.05,
                        (aware(item.restore_available_at) - now).total_seconds()
                        / max(clock.acceleration, 0.001),
                    )
                    if in_progress and item.restore_available_at
                    else None
                ),
            )

    def restore_object(
        self,
        execution_id: str,
        bucket_name: str,
        key: str,
        tier: str,
        retention_days: int,
        idempotency_key: str,
    ) -> RestoreObjectResult:
        with self.store.sessions() as session:
            execution, scenario, clock = self._execution_scope(session, execution_id)
            bucket = session.scalar(
                select(VirtualBucket).where(
                    VirtualBucket.scenario_id == scenario.id,
                    VirtualBucket.name == bucket_name,
                )
            )
            item = session.scalar(
                select(VirtualObject).where(
                    VirtualObject.bucket_id == (bucket.id if bucket else ""),
                    VirtualObject.object_key == key,
                )
            )
            if item is None:
                return RestoreObjectResult(
                    accepted=False,
                    error_code="NoSuchKey",
                    error_message="Virtual object not found",
                )
            now = virtual_now(clock)
            if _expire_restore_if_needed(item, now):
                item.restore_requested_at = None
                item.restore_available_at = None
                item.restore_expires_at = None
            if item.restore_requested_at:
                return RestoreObjectResult(
                    accepted=True,
                    already_in_progress=item.restore_state != "AVAILABLE",
                    request_id=idempotency_key,
                )
            fault, attempt = self._matching_fault(
                session,
                execution,
                scenario,
                clock,
                "RESTORE",
                object_id=item.id,
                object_key=item.object_key,
            )
            if fault and fault["action"] in {"FAIL", "REJECT"}:
                session.commit()
                return RestoreObjectResult(
                    accepted=False,
                    request_id=idempotency_key,
                    error_code=str(fault.get("error_code") or "SimulatedRestoreRejected"),
                    error_message=str(fault.get("message") or "Injected deterministic restore failure"),
                )
            config = json.loads(scenario.configuration_json)
            tier_name = tier.upper()
            default_hours = 48 if tier_name == "BULK" else 12
            minimum = float(config.get(f"{tier_name.lower()}_restore_min_hours", default_hours))
            maximum = float(config.get(f"{tier_name.lower()}_restore_max_hours", minimum))
            if maximum < minimum:
                raise ValueError("Restore maximum cannot be lower than minimum")
            delay = minimum + (maximum - minimum) * _fraction(
                f"{scenario.seed}:restore:{item.content_object_id or item.object_key}"
            )
            if fault and fault["action"] == "DELAY":
                delay += float(fault.get("delay_hours", 0))
            requested_at = virtual_now(clock)
            available_at = requested_at + timedelta(hours=delay)
            item.restore_requested_at = requested_at
            item.restore_available_at = available_at
            item.restore_expires_at = available_at + timedelta(days=retention_days)
            item.restore_state = "RESTORING"
            session.commit()
            return RestoreObjectResult(accepted=True, request_id=idempotency_key)

    def create_restore_job(
        self,
        execution_id: str,
        bucket_name: str,
        tier: str,
        retention_days: int,
        object_count: int,
        manifest_sha256: str,
        idempotency_key: str,
    ) -> SimulatedRestoreJob:
        with self.store.sessions() as session:
            execution, scenario, clock = self._execution_scope(session, execution_id)
            bucket = session.scalar(
                select(VirtualBucket).where(
                    VirtualBucket.scenario_id == scenario.id,
                    VirtualBucket.provider == "AWS",
                    VirtualBucket.name == bucket_name,
                )
            )
            if bucket is None:
                raise LookupError("Virtual source bucket not found")
            existing = session.scalar(
                select(SimulatedRestoreJob).where(
                    SimulatedRestoreJob.idempotency_key == idempotency_key
                )
            )
            if existing:
                if (
                    existing.execution_id != execution_id
                    or existing.object_count != object_count
                    or existing.manifest_sha256 != manifest_sha256
                ):
                    raise ValueError("Restore Batch idempotency key conflicts with another manifest")
                return existing
            job = SimulatedRestoreJob(
                execution_id=execution.id,
                idempotency_key=idempotency_key,
                source_bucket_id=bucket.id,
                tier=tier,
                retention_days=retention_days,
                state="PROCESSING",
                object_count=object_count,
                manifest_sha256=manifest_sha256,
                submitted_virtual_at=virtual_now(clock),
            )
            session.add(job)
            session.commit()
            return job

    def append_restore_job_objects(
        self, job_id: str, identities: list[tuple[str, str | None]]
    ) -> None:
        with self.store.sessions() as session:
            job = session.get(SimulatedRestoreJob, job_id)
            if job is None:
                raise LookupError("Simulated restore job not found")
            execution, scenario, clock = self._execution_scope(session, job.execution_id)
            bucket = session.get(VirtualBucket, job.source_bucket_id)
            config = json.loads(execution.immutable_snapshot_json).get("configuration", {})
            tier_name = job.tier.upper()
            default_hours = 48 if tier_name == "BULK" else 12
            minimum = float(config.get(f"{tier_name.lower()}_restore_min_hours", default_hours))
            maximum = float(config.get(f"{tier_name.lower()}_restore_max_hours", minimum))
            if maximum < minimum:
                raise ValueError("Restore maximum cannot be lower than minimum")
            for key, version_id in identities:
                prior = session.scalar(
                    select(SimulatedRestoreObjectResult.id).where(
                        SimulatedRestoreObjectResult.job_id == job.id,
                        SimulatedRestoreObjectResult.object_key == key,
                    )
                )
                if prior:
                    continue
                item = session.scalar(
                    select(VirtualObject).where(
                        VirtualObject.bucket_id == (bucket.id if bucket else ""),
                        VirtualObject.object_key == key,
                    )
                )
                if item is None or (version_id and item.version_id != version_id):
                    session.add(
                        SimulatedRestoreObjectResult(
                            job_id=job.id,
                            object_key=key,
                            version_id=version_id,
                            accepted=False,
                            error_code="NoSuchKey",
                            error_message="Virtual manifest object was not found",
                        )
                    )
                    continue
                now = virtual_now(clock)
                if _expire_restore_if_needed(item, now):
                    item.restore_requested_at = None
                    item.restore_available_at = None
                    item.restore_expires_at = None
                already = bool(item.restore_requested_at)
                fault, _ = self._matching_fault(
                    session,
                    execution,
                    scenario,
                    clock,
                    "RESTORE",
                    object_id=item.id,
                    object_key=item.object_key,
                )
                if fault and fault["action"] in {"FAIL", "REJECT"}:
                    session.add(
                        SimulatedRestoreObjectResult(
                            job_id=job.id,
                            object_id=item.id,
                            object_key=key,
                            version_id=item.version_id,
                            accepted=False,
                            error_code=str(fault.get("error_code") or "SimulatedRestoreRejected"),
                            error_message=str(fault.get("message") or "Injected deterministic restore failure"),
                        )
                    )
                    continue
                if not already:
                    delay = minimum + (maximum - minimum) * _fraction(
                        f"{scenario.seed}:restore:{item.content_object_id or item.object_key}"
                    )
                    if fault and fault["action"] == "DELAY":
                        delay += float(fault.get("delay_hours", 0))
                    requested_at = virtual_now(clock)
                    item.restore_requested_at = requested_at
                    item.restore_available_at = requested_at + timedelta(hours=delay)
                    item.restore_expires_at = item.restore_available_at + timedelta(
                        days=job.retention_days
                    )
                    item.restore_state = "RESTORING"
                session.add(
                    SimulatedRestoreObjectResult(
                        job_id=job.id,
                        object_id=item.id,
                        object_key=key,
                        version_id=item.version_id,
                        accepted=True,
                        already_in_progress=already,
                    )
                )
            session.commit()

    def finalize_restore_job(
        self, job_id: str, observed_manifest_sha256: str
    ) -> SimulatedRestoreJob:
        with self.store.sessions() as session:
            job = session.get(SimulatedRestoreJob, job_id)
            if job is None:
                raise LookupError("Simulated restore job not found")
            if observed_manifest_sha256 != job.manifest_sha256:
                job.state = "FAILED"
                job.result_json = json.dumps({"error": "Manifest SHA-256 mismatch"})
                session.commit()
                raise SimulatedDataIntegrityError("Restore manifest SHA-256 mismatch")
            accepted = int(session.scalar(select(func.count(SimulatedRestoreObjectResult.id)).where(
                SimulatedRestoreObjectResult.job_id == job.id,
                SimulatedRestoreObjectResult.accepted.is_(True),
            )) or 0)
            failed = int(session.scalar(select(func.count(SimulatedRestoreObjectResult.id)).where(
                SimulatedRestoreObjectResult.job_id == job.id,
                SimulatedRestoreObjectResult.accepted.is_(False),
            )) or 0)
            if accepted + failed != job.object_count:
                job.state = "FAILED"
                job.result_json = json.dumps({
                    "error": "Manifest object count mismatch",
                    "expected": job.object_count,
                    "observed": accepted + failed,
                })
                session.commit()
                raise SimulatedDataIntegrityError("Restore manifest object count mismatch")
            _, _, clock = self._execution_scope(session, job.execution_id)
            job.accepted_count, job.failed_count, job.state = accepted, failed, "COMPLETE"
            job.completed_virtual_at = virtual_now(clock)
            session.commit()
            return job

    def describe_restore_job(
        self, job_id: str, continuation_token: str | None, max_results: int
    ) -> DescribeRestoreBatchResult:
        with self.store.sessions() as session:
            job = session.get(SimulatedRestoreJob, job_id)
            if job is None:
                raise LookupError("Simulated restore job not found")
            conditions = [SimulatedRestoreObjectResult.job_id == job.id]
            if continuation_token:
                try:
                    last_key = base64.urlsafe_b64decode(
                        continuation_token.encode("ascii")
                    ).decode("utf-8")
                except Exception as error:
                    raise ValueError("Invalid restore result continuation token") from error
                conditions.append(SimulatedRestoreObjectResult.object_key > last_key)
            rows = list(session.scalars(
                select(SimulatedRestoreObjectResult)
                .where(and_(*conditions))
                .order_by(SimulatedRestoreObjectResult.object_key)
                .limit(max_results + 1)
            ).all())
            visible = rows[:max_results]
            token = None
            if len(rows) > max_results and visible:
                token = base64.urlsafe_b64encode(
                    visible[-1].object_key.encode("utf-8")
                ).decode("ascii")
            return DescribeRestoreBatchResult(
                job_id=job.id,
                status=job.state,
                object_count=job.object_count,
                accepted_count=job.accepted_count,
                failed_count=job.failed_count,
                results=[RestoreBatchObjectResult(
                    key=row.object_key,
                    version_id=row.version_id,
                    accepted=row.accepted,
                    already_in_progress=row.already_in_progress,
                    error_code=row.error_code,
                    error_message=row.error_message,
                ) for row in visible],
                next_continuation_token=token,
            )

    def read_range(
        self,
        execution_id: str,
        bucket_name: str,
        key: str,
        offset: int,
        length: int,
        allow_archived_for_audit: bool = False,
    ) -> Iterator[bytes]:
        with self.store.sessions() as session:
            execution, scenario, clock = self._execution_scope(session, execution_id)
            bucket = session.scalar(
                select(VirtualBucket).where(
                    VirtualBucket.scenario_id == scenario.id,
                    VirtualBucket.name == bucket_name,
                )
            )
            item = session.scalar(
                select(VirtualObject).where(
                    VirtualObject.bucket_id == (bucket.id if bucket else ""),
                    VirtualObject.object_key == key,
                )
            )
            if item is None:
                raise LookupError("Virtual object not found")
            now = virtual_now(clock)
            expired = _expire_restore_if_needed(item, now)
            if bucket.provider == "AWS" and not allow_archived_for_audit and not _restore_is_available(item, now):
                if expired:
                    session.commit()
                raise PermissionError("Virtual archived object is not restored yet")
            fault, _ = self._matching_fault(
                session,
                execution,
                scenario,
                clock,
                "READ_RANGE",
                object_id=item.id,
                object_key=item.object_key,
            )
            if bucket.provider == "AWS":
                self._reserve_physical_bytes(session, execution, length)
            session.commit()
            descriptor = self._content_descriptor(item)
        if fault and fault["action"] == "FAIL":
            raise ConnectionError("Injected deterministic source stream failure")
        chunks = iter_deterministic_range(descriptor, offset=offset, length=length)
        if fault and fault["action"] == "CORRUPT":
            corrupted = False
            for chunk in chunks:
                if not corrupted and chunk:
                    mutable = bytearray(chunk)
                    mutable[0] ^= 0x01
                    chunk, corrupted = bytes(mutable), True
                yield chunk
            return
        yield from chunks

    @staticmethod
    def _content_descriptor(item: VirtualObject) -> VirtualContentDescriptor:
        return VirtualContentDescriptor(
            scenario_seed=item.content_seed,
            object_id=item.content_object_id or item.id,
            object_key=item.object_key,
            object_version=item.version_id,
            size_bytes=item.size_bytes,
            generator_version=item.generator_version,
        )

    def _destination_scope(self, session, execution_id: str, bucket_name: str, key: str):
        _, scenario, _ = self._execution_scope(session, execution_id)
        destination = session.scalar(
            select(VirtualBucket).where(
                VirtualBucket.scenario_id == scenario.id,
                VirtualBucket.provider == "OCI",
                VirtualBucket.name == bucket_name,
            )
        )
        source = session.scalar(
            select(VirtualObject)
            .join(VirtualBucket, VirtualBucket.id == VirtualObject.bucket_id)
            .where(
                VirtualObject.scenario_id == scenario.id,
                VirtualBucket.provider == "AWS",
                VirtualObject.object_key == key,
            )
        )
        if destination is None:
            raise LookupError("Virtual destination bucket not found")
        if source is None:
            raise LookupError("Matching virtual source object not found")
        return scenario, destination, source

    def put_object(
        self,
        execution_id: str,
        bucket_name: str,
        key: str,
        chunks,
        expected_size: int,
        worker_checksum_sha256: str,
        idempotency_key: str,
    ) -> WriteEvidence:
        received = consume_and_discard(chunks, expected_size=expected_size)
        return self.put_object_evidence(
            execution_id,
            bucket_name,
            key,
            received,
            worker_checksum_sha256,
            idempotency_key,
        )

    def transfer_logically(
        self,
        execution_id: str,
        source_bucket_name: str,
        destination_bucket_name: str,
        key: str,
        size_bytes: int,
        idempotency_key: str,
    ) -> LogicalTransferResult:
        """Promote catalog evidence without generating payload in CONTROL."""
        with self.store.sessions() as session:
            execution, scenario, clock = self._execution_scope(session, execution_id)
            snapshot = json.loads(execution.immutable_snapshot_json)
            if snapshot.get("fidelity") != "CONTROL":
                raise ValueError("Logical transfer is available only in CONTROL scenarios")
            source_bucket = session.scalar(select(VirtualBucket).where(
                VirtualBucket.scenario_id == scenario.id,
                VirtualBucket.provider == "AWS",
                VirtualBucket.name == source_bucket_name,
            ))
            destination = session.scalar(select(VirtualBucket).where(
                VirtualBucket.scenario_id == scenario.id,
                VirtualBucket.provider == "OCI",
                VirtualBucket.name == destination_bucket_name,
            ))
            source = session.scalar(select(VirtualObject).where(
                VirtualObject.bucket_id == (source_bucket.id if source_bucket else ""),
                VirtualObject.object_key == key,
            ))
            if source is None or destination is None:
                raise LookupError("Logical transfer source or destination was not found")
            if source.size_bytes != size_bytes:
                raise SimulatedDataIntegrityError("Logical transfer size differs from source")
            now = virtual_now(clock)
            expired = _expire_restore_if_needed(source, now)
            if not _restore_is_available(source, now):
                if expired:
                    session.commit()
                raise PermissionError("Virtual archived object is not restored yet")
            fault, attempt = self._matching_fault(
                session, execution, scenario, clock, "LOGICAL_TRANSFER",
                object_id=source.id, object_key=source.object_key,
            )
            if fault and fault["action"] in {"FAIL", "TIMEOUT", "REJECT"}:
                session.commit()
                raise ConnectionError("Injected deterministic logical transfer failure")
            config = snapshot.get("configuration", {})
            stable_source_identity = source.content_object_id or source.object_key
            throughput_mbps, latency_seconds, unavailable = network_conditions(
                execution,
                clock,
                config,
                f"{scenario.seed}:network:{stable_source_identity}:{attempt}",
            )
            if unavailable:
                session.commit()
                raise ConnectionError("Simulated network profile is unavailable")
            elapsed = size_bytes * 8 / (throughput_mbps * 1_000_000) + latency_seconds
            if fault and fault["action"] == "DELAY":
                elapsed += max(0, float(fault.get("delay_seconds", 0)))
            checksum = "logical:" + hashlib.sha256(
                f"{scenario.seed}:{stable_source_identity}:{source.size_bytes}".encode()
            ).hexdigest()
            target = session.scalar(select(VirtualObject).where(
                VirtualObject.bucket_id == destination.id,
                VirtualObject.object_key == key,
            ))
            if target is None:
                target = VirtualObject(
                    scenario_id=source.scenario_id,
                    bucket_id=destination.id,
                    object_key=source.object_key,
                    version_id=source.version_id,
                    size_bytes=source.size_bytes,
                    storage_class="STANDARD",
                    last_modified=virtual_now(clock),
                    metadata_json=source.metadata_json,
                    tags_json=source.tags_json,
                    generator_version=source.generator_version,
                    content_seed=source.content_seed,
                    content_object_id=source.content_object_id or source.id,
                    source_sha256=checksum,
                    restore_state="AVAILABLE",
                )
                session.add(target)
            session.commit()
            return LogicalTransferResult(
                evidence=WriteEvidence(
                    accepted=True,
                    request_id=idempotency_key,
                    size_bytes=size_bytes,
                    checksum_sha256=checksum,
                    etag=f'"sim-logical-{stable_source_identity}"',
                ),
                simulated_elapsed_seconds=elapsed,
            )

    def put_object_evidence(
        self,
        execution_id: str,
        bucket_name: str,
        key: str,
        received,
        worker_checksum_sha256: str,
        idempotency_key: str,
    ) -> WriteEvidence:
        with self.store.sessions() as session:
            execution, scenario, clock = self._execution_scope(session, execution_id)
            _, destination, source = self._destination_scope(
                session, execution_id, bucket_name, key
            )
            fault, _ = self._matching_fault(
                session,
                execution,
                scenario,
                clock,
                "PUT_OBJECT",
                object_id=source.id,
                object_key=source.object_key,
            )
            if fault and fault["action"] in {"FAIL", "REJECT", "TIMEOUT"}:
                session.commit()
                raise ConnectionError("Injected deterministic destination write failure")
            source_checksum = deterministic_sha256(self._content_descriptor(source))
            if received.checksum_sha256 != source_checksum:
                raise SimulatedDataIntegrityError(
                    "Destination observed content different from deterministic source"
                )
            if worker_checksum_sha256 != source_checksum:
                raise SimulatedDataIntegrityError(
                    "Worker SHA-256 differs from independently generated source content"
                )
            existing = session.scalar(
                select(VirtualObject).where(
                    VirtualObject.bucket_id == destination.id,
                    VirtualObject.object_key == key,
                )
            )
            if existing is None:
                existing = VirtualObject(
                    scenario_id=source.scenario_id,
                    bucket_id=destination.id,
                    object_key=key,
                    version_id=source.version_id,
                    size_bytes=received.size_bytes,
                    storage_class="STANDARD",
                    last_modified=datetime.now(timezone.utc),
                    metadata_json=source.metadata_json,
                    tags_json=source.tags_json,
                    generator_version=source.generator_version,
                    content_seed=source.content_seed,
                    content_object_id=source.content_object_id or source.id,
                    source_sha256=received.checksum_sha256,
                    restore_state="AVAILABLE",
                )
                session.add(existing)
            elif (
                existing.size_bytes != received.size_bytes
                or existing.source_sha256 != received.checksum_sha256
            ):
                raise SimulatedDataIntegrityError(
                    "Idempotent destination key already contains different evidence"
                )
            session.commit()
            return WriteEvidence(
                accepted=True,
                request_id=idempotency_key,
                size_bytes=received.size_bytes,
                checksum_sha256=received.checksum_sha256,
                etag=f'"sim-{existing.id}"',
            )

    def create_multipart(
        self,
        execution_id: str,
        bucket_name: str,
        key: str,
        expected_size: int,
        part_size: int,
        worker_checksum_sha256: str | None,
        idempotency_key: str,
    ) -> str:
        with self.store.sessions() as session:
            _, destination, source = self._destination_scope(
                session, execution_id, bucket_name, key
            )
            if expected_size != source.size_bytes:
                raise SimulatedDataIntegrityError(
                    "Multipart expected size differs from the virtual source"
                )
            existing = session.scalar(
                select(SimulatedMultipartUpload).where(
                    SimulatedMultipartUpload.idempotency_key == idempotency_key,
                )
            )
            if existing:
                if (
                    existing.execution_id != execution_id
                    or existing.object_key != key
                    or existing.expected_size_bytes != expected_size
                    or existing.part_size_bytes != part_size
                ):
                    raise SimulatedDataIntegrityError(
                        "Multipart idempotency key conflicts with another object"
                    )
                return existing.id
            upload = SimulatedMultipartUpload(
                idempotency_key=idempotency_key,
                execution_id=execution_id,
                destination_bucket_id=destination.id,
                source_object_id=source.id,
                object_key=key,
                expected_size_bytes=expected_size,
                part_size_bytes=part_size,
                expected_sha256=worker_checksum_sha256,
            )
            session.add(upload)
            session.commit()
            return upload.id

    def upload_part(
        self,
        upload_id: str,
        part_number: int,
        chunks,
        size_bytes: int,
        worker_checksum_sha256: str,
        attempt: int = 1,
    ) -> MultipartPartEvidence:
        received = consume_and_discard(chunks, expected_size=size_bytes)
        return self.upload_part_evidence(
            upload_id,
            part_number,
            received,
            worker_checksum_sha256,
            attempt,
        )

    def upload_part_evidence(
        self,
        upload_id: str,
        part_number: int,
        received,
        worker_checksum_sha256: str,
        attempt: int = 1,
    ) -> MultipartPartEvidence:
        with self.store.sessions() as session:
            upload = session.get(SimulatedMultipartUpload, upload_id)
            if upload is None or upload.state != "OPEN":
                raise LookupError("Open simulated multipart upload not found")
            source = session.get(VirtualObject, upload.source_object_id)
            if source is None:
                raise RuntimeError("Multipart source evidence is missing")
            execution, scenario, clock = self._execution_scope(session, upload.execution_id)
            fault, _ = self._matching_fault(
                session,
                execution,
                scenario,
                clock,
                "UPLOAD_PART",
                object_id=source.id,
                object_key=source.object_key,
                part_number=part_number,
            )
            if fault and fault["action"] in {"FAIL", "REJECT", "TIMEOUT"}:
                session.commit()
                if fault["action"] == "TIMEOUT":
                    raise TimeoutError("Injected deterministic multipart timeout")
                raise ConnectionError("Injected deterministic multipart part failure")
            offset = (part_number - 1) * upload.part_size_bytes
            size_bytes = received.size_bytes
            if offset + size_bytes > upload.expected_size_bytes:
                raise ValueError("Multipart part exceeds expected object size")
            expected = consume_and_discard(
                iter_deterministic_range(
                    self._content_descriptor(source), offset=offset, length=size_bytes
                )
            )
            if received.checksum_sha256 != expected.checksum_sha256:
                raise SimulatedDataIntegrityError(
                    "Multipart part differs from independently generated source range"
                )
            if worker_checksum_sha256 != received.checksum_sha256:
                raise SimulatedDataIntegrityError(
                    "Worker multipart checksum differs from received bytes"
                )
            part = session.scalar(
                select(SimulatedMultipartPart).where(
                    SimulatedMultipartPart.upload_id == upload_id,
                    SimulatedMultipartPart.part_number == part_number,
                )
            )
            if part is None:
                part = SimulatedMultipartPart(
                    upload_id=upload_id,
                    part_number=part_number,
                    offset_bytes=offset,
                    size_bytes=size_bytes,
                    checksum_sha256=received.checksum_sha256,
                    attempt=attempt,
                )
                session.add(part)
            elif part.size_bytes != size_bytes or part.checksum_sha256 != received.checksum_sha256:
                raise SimulatedDataIntegrityError(
                    "Retried multipart part conflicts with persisted evidence"
                )
            session.commit()
            return MultipartPartEvidence(
                part_number=part_number,
                size_bytes=size_bytes,
                checksum_sha256=received.checksum_sha256,
                etag=f'"sim-part-{part.id}"',
            )

    def commit_multipart(
        self,
        upload_id: str,
        parts: list[MultipartPartEvidence],
        full_checksum_sha256: str | None,
        idempotency_key: str,
    ) -> WriteEvidence:
        with self.store.sessions() as session:
            upload = session.get(SimulatedMultipartUpload, upload_id)
            if upload is None:
                raise LookupError("Simulated multipart upload not found")
            source = session.get(VirtualObject, upload.source_object_id)
            destination = session.get(VirtualBucket, upload.destination_bucket_id)
            if source is None or destination is None:
                raise RuntimeError("Multipart evidence is incomplete")
            if upload.state == "COMMITTED":
                target = session.scalar(
                    select(VirtualObject).where(
                        VirtualObject.bucket_id == destination.id,
                        VirtualObject.object_key == source.object_key,
                    )
                )
                if target is None or not target.source_sha256:
                    raise SimulatedDataIntegrityError(
                        "Committed multipart upload has incomplete destination evidence"
                    )
                return WriteEvidence(
                    accepted=True,
                    request_id=idempotency_key,
                    size_bytes=upload.expected_size_bytes,
                    checksum_sha256=target.source_sha256,
                    etag=f'"sim-multipart-{upload.id}"',
                )
            persisted = list(
                session.scalars(
                    select(SimulatedMultipartPart)
                    .where(SimulatedMultipartPart.upload_id == upload_id)
                    .order_by(SimulatedMultipartPart.part_number)
                ).all()
            )
            requested = sorted(parts, key=lambda value: value.part_number)
            if len(persisted) != len(requested) or any(
                left.part_number != right.part_number
                or left.size_bytes != right.size_bytes
                or left.checksum_sha256 != right.checksum_sha256
                for left, right in zip(persisted, requested)
            ):
                raise SimulatedDataIntegrityError(
                    "Multipart commit manifest differs from persisted parts"
                )
            if sum(part.size_bytes for part in persisted) != upload.expected_size_bytes:
                raise SimulatedDataIntegrityError("Multipart upload is incomplete")
            if full_checksum_sha256 is not None:
                source_checksum = deterministic_sha256(self._content_descriptor(source))
            else:
                source_checksum = hashlib.sha256(
                    "\n".join(
                        f"{part.part_number}:{part.size_bytes}:{part.checksum_sha256}"
                        for part in persisted
                    ).encode("utf-8")
                ).hexdigest()
            if full_checksum_sha256 is not None and full_checksum_sha256 != source_checksum:
                raise SimulatedDataIntegrityError(
                    "Full worker SHA-256 differs from independently generated source"
                )
            manifest = "\n".join(
                f"{part.part_number}:{part.size_bytes}:{part.checksum_sha256}"
                for part in persisted
            ).encode("utf-8")
            upload.manifest_sha256 = hashlib.sha256(manifest).hexdigest()
            upload.expected_sha256 = source_checksum
            upload.state = "COMMITTED"
            upload.committed_at = datetime.now(timezone.utc)
            target = VirtualObject(
                scenario_id=source.scenario_id,
                bucket_id=destination.id,
                object_key=source.object_key,
                version_id=source.version_id,
                size_bytes=source.size_bytes,
                storage_class="STANDARD",
                last_modified=datetime.now(timezone.utc),
                metadata_json=source.metadata_json,
                tags_json=source.tags_json,
                generator_version=source.generator_version,
                content_seed=source.content_seed,
                content_object_id=source.content_object_id or source.id,
                source_sha256=source_checksum,
                restore_state="AVAILABLE",
            )
            session.add(target)
            session.commit()
            return WriteEvidence(
                accepted=True,
                request_id=idempotency_key,
                size_bytes=upload.expected_size_bytes,
                checksum_sha256=source_checksum,
                etag=f'"sim-multipart-{upload.id}"',
            )
