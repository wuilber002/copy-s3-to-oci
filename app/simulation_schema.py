"""Persistent catalog owned by the external RAIJIN simulator.

The simulator never imports the control-plane models.  Its tables use an
explicit ``sim_`` prefix and are created only by ``scripts/migrate-simulation.py``.
This keeps schema changes deliberate and prepares the backend to move to its
own service/database without changing its contract.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
import uuid

from sqlalchemy import BigInteger, Boolean, DateTime, Float, Index, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from app.runtime_context import SIMULATOR_CONTRACT_VERSION
from app.simulated_data import GENERATOR_VERSION


SIMULATION_SCHEMA_VERSION = 3
DEFAULT_DATA_PHYSICAL_BUDGET_BYTES = 1_000_000_000_000
DEFAULT_RETENTION_DAYS = 60
DEFAULT_QUARANTINE_DAYS = 30


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def new_id() -> str:
    return str(uuid.uuid4())


class ScenarioState(str, Enum):
    ACTIVE = "ACTIVE"
    DEPRECATED = "DEPRECATED"
    PURGE_ELIGIBLE = "PURGE_ELIGIBLE"
    PURGED = "PURGED"


class ExecutionState(str, Enum):
    CREATED = "CREATED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class SimulationBase(DeclarativeBase):
    pass


class SimulationSchemaRevision(SimulationBase):
    __tablename__ = "sim_schema_revisions"

    version: Mapped[int] = mapped_column(Integer, primary_key=True)
    applied_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    description: Mapped[str] = mapped_column(String(255))


class GeneratorRelease(SimulationBase):
    """Lifecycle marker for deterministic byte-generator implementations.

    Runtime housekeeping marks code as removable only after no reproducible
    scenario references it. Actual source-code removal remains a release-time
    decision and never happens automatically inside the running service.
    """

    __tablename__ = "sim_generator_releases"

    version: Mapped[str] = mapped_column(String(64), primary_key=True)
    state: Mapped[str] = mapped_column(String(24), default="ACTIVE", index=True)
    referenced_scenarios: Mapped[int] = mapped_column(Integer, default=0)
    deprecated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    purge_eligible_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class ScenarioTemplate(SimulationBase):
    __tablename__ = "sim_scenario_templates"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    name: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    description: Mapped[str] = mapped_column(Text, default="")
    fidelity: Mapped[str] = mapped_column(String(16), index=True)
    configuration_json: Mapped[str] = mapped_column(Text, default="{}")
    fault_rules_json: Mapped[str] = mapped_column(Text, default="[]")
    active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


class SimulationScenario(SimulationBase):
    __tablename__ = "sim_scenarios"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    name: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    fidelity: Mapped[str] = mapped_column(String(16), index=True)
    seed: Mapped[str] = mapped_column(String(255))
    state: Mapped[str] = mapped_column(
        String(24), default=ScenarioState.ACTIVE.value, index=True
    )
    logical_size_bytes: Mapped[int] = mapped_column(BigInteger, default=0)
    physical_budget_bytes: Mapped[int] = mapped_column(
        BigInteger, default=DEFAULT_DATA_PHYSICAL_BUDGET_BYTES
    )
    clock_acceleration: Mapped[float] = mapped_column(Float, default=3600.0)
    contract_version: Mapped[str] = mapped_column(
        String(32), default=SIMULATOR_CONTRACT_VERSION
    )
    generator_version: Mapped[str] = mapped_column(String(64), default=GENERATOR_VERSION)
    template_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    template_snapshot_json: Mapped[str] = mapped_column(Text, default="{}")
    configuration_json: Mapped[str] = mapped_column(Text, default="{}")
    fault_rules_json: Mapped[str] = mapped_column(Text, default="[]")
    retention_days: Mapped[int] = mapped_column(Integer, default=DEFAULT_RETENTION_DAYS)
    quarantine_days: Mapped[int] = mapped_column(Integer, default=DEFAULT_QUARANTINE_DAYS)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    terminal_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    deprecated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    purge_eligible_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class SimulationExecution(SimulationBase):
    __tablename__ = "sim_executions"
    __table_args__ = (
        Index("ix_sim_execution_scenario_created", "scenario_id", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    scenario_id: Mapped[str] = mapped_column(String(36), index=True)
    correlation_id: Mapped[str] = mapped_column(String(36), unique=True, index=True)
    state: Mapped[str] = mapped_column(
        String(24), default=ExecutionState.CREATED.value, index=True
    )
    immutable_snapshot_json: Mapped[str] = mapped_column(Text)
    physical_budget_bytes: Mapped[int] = mapped_column(BigInteger)
    physical_bytes_processed: Mapped[int] = mapped_column(BigInteger, default=0)
    real_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    real_finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    virtual_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    virtual_finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class SimulationClock(SimulationBase):
    __tablename__ = "sim_clocks"

    execution_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    real_anchor_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    virtual_anchor_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    acceleration: Mapped[float] = mapped_column(Float, default=3600.0)
    paused: Mapped[bool] = mapped_column(Boolean, default=False)
    paused_virtual_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


class VirtualBucket(SimulationBase):
    __tablename__ = "sim_virtual_buckets"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    scenario_id: Mapped[str] = mapped_column(String(36), index=True)
    provider: Mapped[str] = mapped_column(String(8), index=True)
    account_or_tenancy: Mapped[str] = mapped_column(String(255), default="simulated")
    region: Mapped[str] = mapped_column(String(64))
    name: Mapped[str] = mapped_column(String(255), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class VirtualObject(SimulationBase):
    __tablename__ = "sim_virtual_objects"
    __table_args__ = (
        Index("ix_sim_objects_bucket_key_version", "bucket_id", "object_key", "version_id", unique=True),
        Index("ix_sim_objects_scenario_restore", "scenario_id", "restore_state"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    scenario_id: Mapped[str] = mapped_column(String(36), index=True)
    bucket_id: Mapped[str] = mapped_column(String(36), index=True)
    object_key: Mapped[str] = mapped_column(String(2048))
    version_id: Mapped[str] = mapped_column(String(255), default="v1")
    size_bytes: Mapped[int] = mapped_column(BigInteger)
    storage_class: Mapped[str] = mapped_column(String(64), default="DEEP_ARCHIVE")
    last_modified: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    metadata_json: Mapped[str] = mapped_column(Text, default="{}")
    tags_json: Mapped[str] = mapped_column(Text, default="{}")
    generator_version: Mapped[str] = mapped_column(String(64), default=GENERATOR_VERSION)
    content_seed: Mapped[str] = mapped_column(String(255))
    # Destination rows retain their own catalog identity while regenerating
    # the exact byte stream of the source object they represent.
    content_object_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    # CONTROL executions prefix deterministic evidence with ``logical:``;
    # DATA executions store the ordinary 64-character SHA-256 hex digest.
    source_sha256: Mapped[str | None] = mapped_column(String(128), nullable=True)
    restore_state: Mapped[str] = mapped_column(String(32), default="ARCHIVED", index=True)
    restore_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    restore_available_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    restore_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class SimulatedRestoreJob(SimulationBase):
    __tablename__ = "sim_restore_jobs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    execution_id: Mapped[str] = mapped_column(String(36), index=True)
    idempotency_key: Mapped[str] = mapped_column(String(36), unique=True, index=True)
    source_bucket_id: Mapped[str] = mapped_column(String(36), index=True)
    tier: Mapped[str] = mapped_column(String(16))
    retention_days: Mapped[int] = mapped_column(Integer)
    state: Mapped[str] = mapped_column(String(32), default="CREATED", index=True)
    object_count: Mapped[int] = mapped_column(Integer, default=0)
    accepted_count: Mapped[int] = mapped_column(Integer, default=0)
    failed_count: Mapped[int] = mapped_column(Integer, default=0)
    manifest_sha256: Mapped[str] = mapped_column(String(64))
    result_json: Mapped[str] = mapped_column(Text, default="{}")
    submitted_real_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    submitted_virtual_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    completed_virtual_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class SimulatedRestoreObjectResult(SimulationBase):
    __tablename__ = "sim_restore_object_results"
    __table_args__ = (
        Index("ix_sim_restore_result_job_key", "job_id", "object_key", unique=True),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    job_id: Mapped[str] = mapped_column(String(36), index=True)
    object_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    object_key: Mapped[str] = mapped_column(String(2048))
    version_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    accepted: Mapped[bool] = mapped_column(Boolean)
    already_in_progress: Mapped[bool] = mapped_column(Boolean, default=False)
    error_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)


class SimulatedMultipartUpload(SimulationBase):
    __tablename__ = "sim_multipart_uploads"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    idempotency_key: Mapped[str] = mapped_column(String(36), unique=True, index=True)
    execution_id: Mapped[str] = mapped_column(String(36), index=True)
    destination_bucket_id: Mapped[str] = mapped_column(String(36), index=True)
    source_object_id: Mapped[str] = mapped_column(String(36), index=True)
    object_key: Mapped[str] = mapped_column(String(2048))
    expected_size_bytes: Mapped[int] = mapped_column(BigInteger)
    part_size_bytes: Mapped[int] = mapped_column(BigInteger)
    expected_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    state: Mapped[str] = mapped_column(String(24), default="OPEN", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    committed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    manifest_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)


class SimulatedMultipartPart(SimulationBase):
    __tablename__ = "sim_multipart_parts"
    __table_args__ = (
        Index("ix_sim_multipart_part_unique", "upload_id", "part_number", unique=True),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    upload_id: Mapped[str] = mapped_column(String(36), index=True)
    part_number: Mapped[int] = mapped_column(Integer)
    offset_bytes: Mapped[int] = mapped_column(BigInteger)
    size_bytes: Mapped[int] = mapped_column(BigInteger)
    checksum_sha256: Mapped[str] = mapped_column(String(64))
    attempt: Mapped[int] = mapped_column(Integer, default=1)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class InjectedFault(SimulationBase):
    __tablename__ = "sim_injected_faults"
    __table_args__ = (
        Index("ix_sim_fault_replay", "execution_id", "object_id", "part_number", "attempt"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    execution_id: Mapped[str] = mapped_column(String(36), index=True)
    seed: Mapped[str] = mapped_column(String(255))
    fault_type: Mapped[str] = mapped_column(String(64), index=True)
    object_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    part_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    attempt: Mapped[int] = mapped_column(Integer, default=1)
    operation: Mapped[str] = mapped_column(String(64))
    virtual_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    details_json: Mapped[str] = mapped_column(Text, default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class SimulatedOperation(SimulationBase):
    __tablename__ = "sim_operations"
    __table_args__ = (
        Index("ix_sim_operation_attempt", "execution_id", "operation", "object_id", "part_number", "attempt"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    execution_id: Mapped[str] = mapped_column(String(36), index=True)
    operation: Mapped[str] = mapped_column(String(64), index=True)
    object_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    part_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    attempt: Mapped[int] = mapped_column(Integer)
    virtual_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    fault_injected: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class SimulationTombstone(SimulationBase):
    __tablename__ = "sim_tombstones"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    resource_type: Mapped[str] = mapped_column(String(64), index=True)
    resource_id: Mapped[str] = mapped_column(String(36), index=True)
    resource_name: Mapped[str] = mapped_column(String(255))
    reason: Mapped[str] = mapped_column(Text)
    evidence_sha256: Mapped[str] = mapped_column(String(64))
    purged_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
