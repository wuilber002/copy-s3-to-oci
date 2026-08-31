from __future__ import annotations

"""Local control plane for a durable S3-to-OCI migration.

AWS and OCI transfer adapters are deliberately not invoked by API requests.
They are scheduled workers; this keeps configuration, inventory, waves and
leases durable even if a VM is restarted during a long restore.
"""

from datetime import datetime, timedelta, timezone
from enum import Enum
try:  # Keep local validation possible on Oracle Linux Python 3.10 too.
    from enum import StrEnum
except ImportError:  # pragma: no cover - exercised only on Python < 3.11
    class StrEnum(str, Enum):
        pass
from typing import Generator
import os
import csv
import io
import json
import base64
import gzip
import math
import re
import shutil
import uuid
from urllib.request import Request, urlopen
from urllib.parse import urlparse

from fastapi import Depends, FastAPI, File, Form, HTTPException, Query, UploadFile, Request as FastAPIRequest
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import BigInteger, Boolean, DateTime, Float, ForeignKey, Index, Integer, String, Text, UniqueConstraint, and_, case, create_engine, func, inspect, or_, select, text, update
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, aliased, mapped_column, relationship, sessionmaker

from app.cloud_backends import backend_for
from app.runtime_context import RAIJIN_SERVICE_VERSION, SIMULATOR_SERVICE_VERSION, load_runtime_context
from app.simulator_admin import SimulatorAdminClient, SimulatorAdminError

RAIJU_MIN_WORKERS = 5
# The continuous lane must stay bounded even when a source contains millions
# of restored objects.  These are intentionally internal implementation
# pages, not operator-facing batch limits: Raikou still dispatches only the
# configured 100/20-object batches.
TRANSFER_LANE_ADMISSION_PAGE_SIZE = 1_000
TRANSFER_LANE_PRIORITY_REFRESH_PAGE_SIZE = 1_000
TRANSFER_LANE_CLAIM_CANDIDATE_PAGE_SIZE = 256


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def source_scheduler_clock(source: "Source"):
    """Return the source clock used only by migration planning.

    Durable queue leases, retry delays and task availability deliberately keep
    using the real system clock.  Only simulated cloud time is accelerated.
    """
    return cloud_backend.clock(source.simulation_execution_id)


def restore_availability_poll_delay_seconds(accepted_at: datetime | None, now: datetime,
                                            restore_tier: str, partial_availability: bool = False,
                                            transfer_strategy: str = "AFTER_ALL_RESTORED",
                                            pending_objects: int = 0) -> int:
    """Choose a low-cost polling cadence after AWS accepted a restore request.

    Batch execution is polled closely only until its per-object acceptance
    report exists.  Availability then starts at two hours and becomes more
    frequent only near the service window. Once some objects are available,
    an early-transfer source checks every five minutes so it can release the
    next objects promptly. The conservative strategy still waits thirty
    minutes because it cannot transfer before the whole wave is ready.
    """
    if partial_availability:
        if transfer_strategy != "AS_OBJECTS_AVAILABLE":
            return 30 * 60
        # Early release should be responsive without turning a large wave into
        # an expensive HEAD storm. The next interval grows with the remaining
        # set, and falls naturally as objects become available.
        if pending_objects <= 5_000:
            return 5 * 60
        if pending_objects <= 50_000:
            return 10 * 60
        return 30 * 60
    expected_window = {
        "BULK": 48 * 60 * 60,
        "STANDARD": 12 * 60 * 60,
        "EXPEDITED": 30 * 60,
    }.get(str(restore_tier).upper(), 48 * 60 * 60)
    elapsed = max(0, (now - accepted_at).total_seconds()) if accepted_at else 0
    if elapsed < expected_window * 0.5:
        return 2 * 60 * 60
    if elapsed < expected_window * 0.75:
        return 60 * 60
    return 30 * 60


def read_secret(path: str) -> str:
    with open(path, "r", encoding="utf-8") as secret_file:
        return secret_file.read().strip()


database_url = os.environ["DATABASE_URL"]
runtime_context = load_runtime_context()
cloud_backend = backend_for(runtime_context)
password = read_secret(os.environ["POSTGRES_PASSWORD_FILE"])
parsed_database_url = make_url(database_url)
if parsed_database_url.drivername.startswith("postgres"):
    database_url = parsed_database_url.set(password=password).render_as_string(
        hide_password=False
    )
platform_status_file = os.environ.get("PLATFORM_STATUS_FILE", "/run/platform-status/status.json")
oci_runtime_config_file = os.environ.get("OCI_RUNTIME_CONFIG_FILE", "/run/oci-runtime/oci-runtime.json")
DYNAMIC_PLATFORM_MAX_BYTES = 10 * 1024**4
DYNAMIC_PLATFORM_MAX_OBJECTS = 500_000
engine = create_engine(database_url, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)


class Base(DeclarativeBase):
    pass


class ObjectState(StrEnum):
    DISCOVERED = "DISCOVERED"
    WAVE_ASSIGNED = "WAVE_ASSIGNED"
    RESTORE_REQUESTED = "RESTORE_REQUESTED"
    RESTORE_REQUEST_ACCEPTED = "RESTORE_REQUEST_ACCEPTED"
    RESTORING = "RESTORING"
    RESTORED = "RESTORED"
    TRANSFERRING = "TRANSFERRING"
    TRANSFERRED = "TRANSFERRED"
    VERIFIED = "VERIFIED"
    FAILED = "FAILED"


class TaskState(StrEnum):
    READY = "READY"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class TransferQueueState(StrEnum):
    """Lifecycle of one object in Raikou's durable continuous lane."""
    AVAILABLE = "AVAILABLE"
    READY = "READY"
    LEASED = "LEASED"
    MULTIPART_RESUME = "MULTIPART_RESUME"
    TRANSFERRED = "TRANSFERRED"
    RETRY_WAIT = "RETRY_WAIT"
    EXPIRED = "EXPIRED"
    REAPPROVAL_REQUIRED = "REAPPROVAL_REQUIRED"
    CANCELLED = "CANCELLED"


ARCHIVE_STORAGE_CLASSES = {
    "GLACIER", "DEEP_ARCHIVE", "INTELLIGENT_TIERING_ARCHIVE_ACCESS",
    "INTELLIGENT_TIERING_DEEP_ARCHIVE_ACCESS",
}

# OCI Resource Search uses this resource type for OCI Vault Secret metadata.
# It deliberately retrieves only Secret identifiers and metadata; values are
# read later, one at a time, only to validate the registered JSON schema.
OCI_VAULT_SECRET_SEARCH_QUERY = "query vaultsecret resources"


class Source(Base):
    __tablename__ = "sources"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(128), unique=True)
    s3_bucket: Mapped[str] = mapped_column(String(255))
    # Kept as the canonical first prefix for compatibility with prior reports.
    # New sources use the normalized ``source_prefixes`` table below.
    s3_prefix: Mapped[str] = mapped_column(String(1024), default="")
    aws_region: Mapped[str] = mapped_column(String(64))
    aws_bucket_region: Mapped[str | None] = mapped_column(String(64), nullable=True)
    aws_connection_id: Mapped[int | None] = mapped_column(ForeignKey("aws_connections.id"), nullable=True, index=True)
    backend_kind: Mapped[str] = mapped_column(String(16), default="REAL", index=True)
    simulation_scenario_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    simulation_execution_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    simulation_correlation_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    simulation_tenant_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    simulation_project_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    simulation_fidelity: Mapped[str | None] = mapped_column(String(16), nullable=True)
    destination_bucket: Mapped[str] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(32), default="CONFIGURED")
    # A business ordering is intentionally only a tie-breaker.  Deadline risk
    # always wins, but an operator can make one source preferred when several
    # equally safe queues later share a project-level lane.
    business_priority: Mapped[int] = mapped_column(Integer, default=999, index=True)
    discovery_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # ``discovery_elapsed_seconds`` is durable work time, rather than wall-clock
    # time since an operator pressed the button.  This keeps the UI useful when
    # a discovery is resumed after a worker or VM restart.
    discovery_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    discovery_elapsed_seconds: Mapped[float] = mapped_column(Float, default=0)
    discovery_completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    discovery_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Checkpoint after each successfully committed ListObjectsV2 page.  This
    # is deliberately an AWS continuation token, not a key, so S3 remains the
    # authority on the exact next page after an interrupted discovery.
    discovery_continuation_token: Mapped[str | None] = mapped_column(Text, nullable=True)
    # A multi-prefix scan resumes both its S3 token and the prefix being read.
    discovery_prefix_index: Mapped[int] = mapped_column(Integer, default=0)
    discovery_pages_completed: Mapped[int] = mapped_column(Integer, default=0)
    discovery_objects_inserted: Mapped[int] = mapped_column(BigInteger, default=0)
    # The data origin is operational evidence, not presentation-only state.
    # A later controlled rediscovery advances the generation without erasing
    # the prior inventory or the migration history attached to it.
    last_discovery_mode: Mapped[str | None] = mapped_column(String(32), nullable=True)
    discovery_generation: Mapped[int] = mapped_column(Integer, default=0)
    archived_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    destination_validation_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    destination_validation_status: Mapped[str | None] = mapped_column(String(32), nullable=True)
    destination_missing_count: Mapped[int] = mapped_column(Integer, default=0)
    destination_size_mismatch_count: Mapped[int] = mapped_column(Integer, default=0)
    destination_metadata_mismatch_count: Mapped[int] = mapped_column(Integer, default=0)
    destination_extra_count: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    objects: Mapped[list[ObjectRecord]] = relationship(back_populates="source")
    waves: Mapped[list[Wave]] = relationship(back_populates="source")
    aws_connection: Mapped["AwsConnection | None"] = relationship(back_populates="sources")
    prefixes: Mapped[list["SourcePrefix"]] = relationship(back_populates="source", cascade="all, delete-orphan")


class AwsConnection(Base):
    """Immutable local identity for one customer-managed AWS credential Secret."""
    __tablename__ = "aws_connections"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    label: Mapped[str] = mapped_column(String(255), unique=True)
    secret_ocid: Mapped[str] = mapped_column(String(255), unique=True)
    aws_account_id: Mapped[str] = mapped_column(String(12), index=True)
    default_region: Mapped[str] = mapped_column(String(64))
    control_bucket: Mapped[str] = mapped_column(String(255))
    # Per-account ceilings keep one aggressive source from surprising a
    # customer with request bursts.  They affect future API calls only.
    discovery_requests_per_second: Mapped[int] = mapped_column(Integer, default=10)
    restore_poll_requests_per_second: Mapped[int] = mapped_column(Integer, default=10)
    restore_poll_concurrency: Mapped[int] = mapped_column(Integer, default=10)
    archived_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    sources: Mapped[list["Source"]] = relationship(back_populates="aws_connection")


class CostPricing(Base):
    """Customer-maintained unit prices for one AWS connection/region.

    Public AWS and OCI prices are regional and contractual discounts are common,
    so the control plane never hard-codes a price as if it were an invoice.
    ``None`` means the operator has not supplied that unit price yet.
    """
    __tablename__ = "cost_pricing"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    aws_connection_id: Mapped[int] = mapped_column(ForeignKey("aws_connections.id"), unique=True, index=True)
    currency: Mapped[str] = mapped_column(String(8), default="USD")
    reference: Mapped[str] = mapped_column(String(1024), default="")
    expected_restore_poll_cycles: Mapped[int] = mapped_column(Integer, default=24)
    include_aws_transfer_out: Mapped[bool] = mapped_column(default=True)
    include_oci_costs: Mapped[bool] = mapped_column(default=True)
    aws_batch_job_usd: Mapped[float | None] = mapped_column(Float, nullable=True)
    aws_batch_object_usd_per_1000: Mapped[float | None] = mapped_column(Float, nullable=True)
    aws_s3_put_list_usd_per_1000: Mapped[float | None] = mapped_column(Float, nullable=True)
    aws_s3_get_usd_per_1000: Mapped[float | None] = mapped_column(Float, nullable=True)
    aws_glacier_bulk_retrieval_usd_per_gib: Mapped[float | None] = mapped_column(Float, nullable=True)
    aws_glacier_standard_retrieval_usd_per_gib: Mapped[float | None] = mapped_column(Float, nullable=True)
    aws_deep_archive_bulk_retrieval_usd_per_gib: Mapped[float | None] = mapped_column(Float, nullable=True)
    aws_deep_archive_standard_retrieval_usd_per_gib: Mapped[float | None] = mapped_column(Float, nullable=True)
    aws_transfer_out_usd_per_gib: Mapped[float | None] = mapped_column(Float, nullable=True)
    aws_restore_temp_standard_usd_per_gib_month: Mapped[float | None] = mapped_column(Float, nullable=True)
    oci_put_usd_per_10000: Mapped[float | None] = mapped_column(Float, nullable=True)
    oci_get_usd_per_10000: Mapped[float | None] = mapped_column(Float, nullable=True)
    oci_storage_usd_per_gib_month: Mapped[float | None] = mapped_column(Float, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class GlobalAwsPricing(Base):
    """Cached public AWS Price List values, one row per source region/currency."""
    __tablename__ = "global_aws_pricing"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    aws_region: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    currency: Mapped[str] = mapped_column(String(8), default="USD")
    rates_json: Mapped[str] = mapped_column(Text, default="{}")
    source_url: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    fetched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)


class AwsSecretCache(Base):
    """Non-sensitive cache of Secrets checked during an explicit refresh."""
    __tablename__ = "aws_secret_cache"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    secret_ocid: Mapped[str] = mapped_column(String(255), unique=True)
    name: Mapped[str] = mapped_column(String(255))
    compartment_id: Mapped[str] = mapped_column(String(255))
    schema_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    connection_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    aws_account_id: Mapped[str | None] = mapped_column(String(12), nullable=True)
    default_region: Mapped[str | None] = mapped_column(String(64), nullable=True)
    valid: Mapped[bool] = mapped_column(default=False, index=True)
    validation_error: Mapped[str | None] = mapped_column(String(512), nullable=True)
    refreshed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)


class ObjectRecord(Base):
    __tablename__ = "objects"
    # Fresh installations receive the indexes required by the high-volume
    # inventory path. Existing production databases receive them through the
    # explicit online migration script, never implicitly at application boot.
    __table_args__ = (
        Index("ix_objects_source_key_id", "source_id", "object_key", "id"),
        Index("ix_objects_source_state_key_id", "source_id", "state", "object_key", "id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"), index=True)
    # A rediscovery can find a new revision of a key that was already
    # migrated.  Keep the old record immutable as evidence and make the new
    # revision the only one eligible for subsequent planning.
    is_current_revision: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    previous_object_id: Mapped[int | None] = mapped_column(ForeignKey("objects.id"), nullable=True, index=True)
    superseded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    object_key: Mapped[str] = mapped_column(String(2048))
    version_id: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    size_bytes: Mapped[int] = mapped_column(BigInteger)
    etag: Mapped[str | None] = mapped_column(String(128), nullable=True)
    storage_class: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_modified: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    metadata_json: Mapped[str] = mapped_column(Text, default="{}")
    tags_json: Mapped[str] = mapped_column(Text, default="{}")
    source_checksum: Mapped[str | None] = mapped_column(String(256), nullable=True)
    destination_checksum: Mapped[str | None] = mapped_column(String(256), nullable=True)
    checksum_algorithm: Mapped[str | None] = mapped_column(String(32), nullable=True)
    restore_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    restore_attempt_id: Mapped[int | None] = mapped_column(ForeignKey("restore_attempts.id"), nullable=True, index=True)
    restored_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    # S3 exposes this timestamp in the Restore / RestoreStatus response.  It
    # is the expiry of the temporary Standard copy, not the archived object.
    restore_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    transferred_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    transfer_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    transfer_elapsed_seconds: Mapped[float] = mapped_column(Float, default=0)
    # The planner stores its estimate separately from measured elapsed work.
    # It is advisory only and never drives a worker without an explicit queue.
    planned_transfer_seconds: Mapped[float] = mapped_column(Float, default=0)
    transfer_progress_bytes: Mapped[int] = mapped_column(BigInteger, default=0)
    transfer_progress_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    transfer_rate_mbps: Mapped[float] = mapped_column(Float, default=0)
    delivery_integrity_algorithm: Mapped[str | None] = mapped_column(String(32), nullable=True)
    delivery_integrity_checksum: Mapped[str | None] = mapped_column(String(256), nullable=True)
    delivery_integrity_status: Mapped[str | None] = mapped_column(String(32), nullable=True)
    delivery_integrity_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # OCI multipart checkpoints are intentionally durable.  They make a VM
    # restart resume only the missing parts instead of discarding a large file.
    multipart_upload_id: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    multipart_part_size: Mapped[int | None] = mapped_column(Integer, nullable=True)
    multipart_parts_json: Mapped[str] = mapped_column(Text, default="{}")
    multipart_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    audit_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    audit_progress_bytes: Mapped[int] = mapped_column(BigInteger, default=0)
    audit_progress_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    audit_rate_mbps: Mapped[float] = mapped_column(Float, default=0)
    integrity_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    integrity_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    state: Mapped[str] = mapped_column(String(32), default=ObjectState.DISCOVERED)
    wave_id: Mapped[int | None] = mapped_column(ForeignKey("waves.id"), nullable=True, index=True)
    source: Mapped[Source] = relationship(back_populates="objects")
    wave: Mapped[Wave | None] = relationship(back_populates="objects")


class OciBucketCache(Base):
    __tablename__ = "oci_bucket_cache"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    bucket_ocid: Mapped[str] = mapped_column(String(255), unique=True)
    name: Mapped[str] = mapped_column(String(255), index=True)
    compartment_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    compartment_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    lifecycle_state: Mapped[str | None] = mapped_column(String(64), nullable=True)
    refreshed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)


class Wave(Base):
    __tablename__ = "waves"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"), index=True)
    name: Mapped[str] = mapped_column(String(128))
    max_bytes: Mapped[int] = mapped_column(BigInteger)
    restore_days: Mapped[int] = mapped_column(Integer)
    restore_tier: Mapped[str] = mapped_column(String(16))
    # The policy is immutable after the wave enters restore processing. Dynamic
    # pipelines always use AS_OBJECTS_AVAILABLE; manual waves may opt into the
    # conservative all-available policy at creation time.
    transfer_release_policy: Mapped[str] = mapped_column(
        String(32), default="AS_OBJECTS_AVAILABLE"
    )
    status: Mapped[str] = mapped_column(String(32), default="DRAFT")
    batch_job_id: Mapped[str | None] = mapped_column(String(128), nullable=True, unique=True)
    batch_job_status: Mapped[str | None] = mapped_column(String(64), nullable=True)
    manifest_key: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    manifest_etag: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_poll_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    poll_count: Mapped[int] = mapped_column(Integer, default=0)
    pipeline_run_id: Mapped[int | None] = mapped_column(ForeignKey("dynamic_pipeline_runs.id"), nullable=True, index=True)
    availability_head_requests: Mapped[int] = mapped_column(BigInteger, default=0)
    availability_poll_elapsed_seconds: Mapped[float] = mapped_column(Float, default=0)
    availability_throttle_retries: Mapped[int] = mapped_column(Integer, default=0)
    last_availability_poll_objects: Mapped[int] = mapped_column(Integer, default=0)
    last_availability_poll_seconds: Mapped[float] = mapped_column(Float, default=0)
    planner_mode: Mapped[str] = mapped_column(String(32), default="MANUAL")
    predicted_transfer_seconds: Mapped[float] = mapped_column(Float, default=0)
    prediction_samples: Mapped[int] = mapped_column(Integer, default=0)
    # Raiju decides this value batch by batch.  Persisting the current
    # allocation makes the Status view report the actual dynamic concurrency
    # instead of presenting a fixed, misleading worker count.
    active_transfer_workers: Mapped[int] = mapped_column(Integer, default=0)
    planned_restore_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    planned_transfer_start_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    # Observed milestones use the simulator virtual clock only for simulated
    # sources.  Real sources retain their existing object-level wall-clock
    # evidence.  Keeping both avoids mixing time domains in the flight board.
    restore_requested_virtual_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    first_restore_available_virtual_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_restore_available_virtual_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    transfer_started_virtual_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    transfer_completed_virtual_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # A simulated transfer keeps one durable virtual-clock hold between the
    # final availability poll and the worker claim.  Without it a fast
    # simulation clock can expire the restored copy during that queue gap.
    simulation_transfer_clock_held: Mapped[bool] = mapped_column(Boolean, default=False)
    restore_reapproval_required: Mapped[bool] = mapped_column(Boolean, default=False)
    restore_reapproval_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    restore_reapproval_detected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    source: Mapped[Source] = relationship(back_populates="waves")
    objects: Mapped[list[ObjectRecord]] = relationship(back_populates="wave")
    tasks: Mapped[list[Task]] = relationship(back_populates="wave")


class DynamicPipelineRun(Base):
    """Durable, adaptive pipeline control record for dynamic waves."""
    __tablename__ = "dynamic_pipeline_runs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"), index=True)
    planner_version: Mapped[str] = mapped_column(String(32), default="v1")
    status: Mapped[str] = mapped_column(String(32), default="PLANNED", index=True)
    target_max_bytes: Mapped[int] = mapped_column(BigInteger, default=0)
    target_transfer_seconds: Mapped[int] = mapped_column(Integer, default=0)
    max_objects: Mapped[int] = mapped_column(Integer, default=0)
    restore_safety_seconds: Mapped[int] = mapped_column(Integer, default=0)
    restore_horizon_waves: Mapped[int] = mapped_column(Integer, default=3)
    restore_days: Mapped[int] = mapped_column(Integer, default=0)
    restore_tier: Mapped[str] = mapped_column(String(16), default="BULK")
    transfer_strategy: Mapped[str] = mapped_column(String(32), default="AFTER_ALL_RESTORED")
    scheduled_restores: Mapped[bool] = mapped_column(default=False)
    selection_prefix: Mapped[str] = mapped_column(String(1024), default="")
    next_sequence: Mapped[int] = mapped_column(Integer, default=1)
    historical_samples: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    source: Mapped[Source] = relationship()


class RestoreAttempt(Base):
    """Immutable evidence for one S3 Batch Operations restore submission."""
    __tablename__ = "restore_attempts"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    wave_id: Mapped[int] = mapped_column(ForeignKey("waves.id"), index=True)
    aws_region: Mapped[str] = mapped_column(String(64))
    job_id: Mapped[str | None] = mapped_column(String(128), nullable=True, unique=True)
    job_status: Mapped[str | None] = mapped_column(String(64), nullable=True)
    manifest_key: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    manifest_etag: Mapped[str | None] = mapped_column(String(128), nullable=True)
    expected_objects: Mapped[int] = mapped_column(Integer, default=0)
    succeeded_objects: Mapped[int] = mapped_column(Integer, default=0)
    failed_objects: Mapped[int] = mapped_column(Integer, default=0)
    report_manifest_key: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    report_manifest_etag: Mapped[str | None] = mapped_column(String(128), nullable=True)
    failure_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Exact request accounting for Batch confirmation/report evidence.  This
    # lets an operator distinguish charged control-plane calls from targeted
    # per-object availability HeadObject calls.
    batch_describe_requests: Mapped[int] = mapped_column(Integer, default=0)
    completion_report_list_requests: Mapped[int] = mapped_column(Integer, default=0)
    completion_report_get_requests: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class RestoreObjectResult(Base):
    """Per-object outcome imported from the S3 Batch completion report."""
    __tablename__ = "restore_object_results"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    attempt_id: Mapped[int] = mapped_column(ForeignKey("restore_attempts.id"), index=True)
    object_id: Mapped[int] = mapped_column(ForeignKey("objects.id"), index=True)
    task_status: Mapped[str] = mapped_column(String(32), default="PENDING")
    http_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    report_key: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    report_etag: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


# AWS records RestoreAlreadyInProgress as a failed S3 Batch task even though
# the desired operational state already exists: a restore request is active
# for that object. Preserve the raw AWS result, but treat this code as
# accepted-equivalent when deciding whether availability polling may proceed.
RESTORE_ACCEPTED_EQUIVALENT_ERROR_CODES = frozenset({"RestoreAlreadyInProgress"})


def restore_result_diagnostics(session: Session, attempt_id: int) -> dict:
    """Aggregate immutable per-object Batch evidence into an operator diagnosis."""
    rows = session.execute(
        select(RestoreObjectResult, ObjectRecord.object_key)
        .join(ObjectRecord, ObjectRecord.id == RestoreObjectResult.object_id)
        .where(RestoreObjectResult.attempt_id == attempt_id)
        .order_by(RestoreObjectResult.id)
    ).all()
    raw_succeeded = accepted_equivalent = unexpected_failed = pending = 0
    grouped: dict[tuple[str, int | None, str], dict] = {}
    for result, object_key in rows:
        status = (result.task_status or "PENDING").upper()
        code = result.error_code or ("PENDING_EVIDENCE" if status == "PENDING" else "UNKNOWN")
        if status == "SUCCEEDED":
            raw_succeeded += 1
            continue
        if status == "PENDING":
            pending += 1
        elif code in RESTORE_ACCEPTED_EQUIVALENT_ERROR_CODES:
            accepted_equivalent += 1
        else:
            unexpected_failed += 1
        message = (result.error_message or "No detail returned by AWS").split(" (Service:", 1)[0][:500]
        key = (code, result.http_status, message)
        reason = grouped.setdefault(key, {"code": code, "http_status": result.http_status,
                                          "message": message, "count": 0, "sample_keys": []})
        reason["count"] += 1
        if len(reason["sample_keys"]) < 3:
            reason["sample_keys"].append(object_key)
    reasons = sorted(grouped.values(), key=lambda item: (-item["count"], item["code"]))
    effective_accepted = raw_succeeded + accepted_equivalent
    action_required = bool(unexpected_failed or pending)
    if action_required:
        leading = reasons[0] if reasons else {"code": "UNKNOWN", "count": unexpected_failed + pending}
        summary = f"{leading['count']} object(s) reported {leading['code']}"
        recommendation = "Review the AWS reason below, correct the cause, then reprocess only this wave."
    elif accepted_equivalent:
        summary = (f"{accepted_equivalent} object(s) were already being restored; AWS returned "
                   "RestoreAlreadyInProgress and no duplicate restore is required")
        recommendation = "Continue availability polling; do not submit another restore job."
    else:
        summary = "All object restore requests were accepted by AWS"
        recommendation = "Continue availability polling."
    return {"raw_succeeded": raw_succeeded, "accepted_equivalent": accepted_equivalent,
            "effective_accepted": effective_accepted, "unexpected_failed": unexpected_failed,
            "pending_evidence": pending, "action_required": action_required,
            "summary": summary, "recommended_action": recommendation, "reasons": reasons}


class Task(Base):
    __tablename__ = "tasks"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    wave_id: Mapped[int] = mapped_column(ForeignKey("waves.id"), index=True)
    kind: Mapped[str] = mapped_column(String(32))
    state: Mapped[str] = mapped_column(String(16), default=TaskState.READY, index=True)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    worker_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    wave: Mapped[Wave] = relationship(back_populates="tasks")


class TransferQueueItem(Base):
    """Durable object-level work owned by the continuous transfer lane.

    Waves own restore and cost evidence; this record owns only the right to
    copy an already available object. The uniqueness constraint is the core
    idempotency boundary across polling, restart and retry.
    """
    __tablename__ = "transfer_queue_items"
    __table_args__ = (
        UniqueConstraint("wave_id", "object_id", name="uq_transfer_queue_wave_object"),
        Index("ix_transfer_queue_dispatch", "source_id", "state", "priority_score", "restore_expires_at", "id"),
        Index("ix_transfer_queue_wave_state", "wave_id", "state", "id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    # Reserved now for the future project-scoped lane. It intentionally has no
    # foreign key until the migration-project aggregate is introduced.
    project_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True, index=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"), index=True)
    wave_id: Mapped[int] = mapped_column(ForeignKey("waves.id"), index=True)
    object_id: Mapped[int] = mapped_column(ForeignKey("objects.id"), index=True)
    size_bytes: Mapped[int] = mapped_column(BigInteger)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    restore_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    predicted_transfer_seconds: Mapped[float] = mapped_column(Float, default=0)
    priority_score: Mapped[int] = mapped_column(Integer, default=0, index=True)
    priority_band: Mapped[str] = mapped_column(String(16), default="NORMAL", index=True)
    state: Mapped[str] = mapped_column(String(32), default=TransferQueueState.AVAILABLE, index=True)
    lease_token: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    lease_owner: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    dispatch_batch_id: Mapped[int | None] = mapped_column(ForeignKey("transfer_dispatch_batches.id"), nullable=True, index=True)
    preemption_cooldown_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    # A critical item can be reserved for the Raiju that is closest to a safe
    # object boundary.  Both sides are durable so a process restart cannot
    # turn a cooperative handoff into an invisible best-effort decision.
    preemption_successor_item_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True, index=True)
    preemption_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    decision_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    priority_details_json: Mapped[str] = mapped_column(Text, default="{}")
    last_dispatched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    preemption_count: Mapped[int] = mapped_column(Integer, default=0)
    transferred_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class TransferDispatchBatch(Base):
    """Durable Raikou decision for one continuous-lane dispatch cycle.

    Queue items retain their individual lease and retry ownership.  This
    aggregate only records the decision that grouped them, which makes
    priority, preemption and worker-capacity behaviour explainable without
    treating a batch as an atomic transfer unit.
    """
    __tablename__ = "transfer_dispatch_batches"
    __table_args__ = (
        Index("ix_transfer_dispatch_batches_source_started", "source_id", "started_at"),
        Index("ix_transfer_dispatch_batches_wave_started", "wave_id", "started_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"), index=True)
    wave_id: Mapped[int | None] = mapped_column(ForeignKey("waves.id"), nullable=True, index=True)
    task_id: Mapped[int | None] = mapped_column(ForeignKey("tasks.id"), nullable=True, index=True)
    priority_band: Mapped[str] = mapped_column(String(16), default="NORMAL", index=True)
    priority_score: Mapped[int] = mapped_column(Integer, default=0)
    object_limit: Mapped[int] = mapped_column(Integer, default=100)
    byte_limit: Mapped[int] = mapped_column(BigInteger, default=1024**3)
    object_count: Mapped[int] = mapped_column(Integer, default=0)
    bytes_planned: Mapped[int] = mapped_column(BigInteger, default=0)
    worker_target: Mapped[int] = mapped_column(Integer, default=0)
    state: Mapped[str] = mapped_column(String(24), default="CLAIMED", index=True)
    reason: Mapped[str] = mapped_column(Text, default="continuous lane dispatch")
    preempted_batch_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True, index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)


class TransferLaneSegment(Base):
    """An immutable observed interval of object work in the continuous lane.

    A wave may produce many of these intervals.  They are deliberately kept
    below the wave aggregate so the flight board can show true interleaving,
    not one invented blue bar per wave.
    """
    __tablename__ = "transfer_lane_segments"
    __table_args__ = (
        Index("ix_transfer_lane_segments_wave_started", "wave_id", "started_at"),
        Index("ix_transfer_lane_segments_source_started", "source_id", "started_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"), index=True)
    wave_id: Mapped[int] = mapped_column(ForeignKey("waves.id"), index=True)
    queue_item_id: Mapped[int] = mapped_column(ForeignKey("transfer_queue_items.id"), index=True)
    worker_slot: Mapped[int | None] = mapped_column(Integer, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    bytes_transferred: Mapped[int] = mapped_column(BigInteger, default=0)
    object_count: Mapped[int] = mapped_column(Integer, default=1)
    entry_reason: Mapped[str] = mapped_column(Text, default="lane dispatch")
    exit_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    nearest_expiry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class DiscoveryJob(Base):
    """Durable, observable queue record for one remote S3 discovery run."""
    __tablename__ = "discovery_jobs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"), index=True)
    mode: Mapped[str] = mapped_column(String(32), default="REMOTE_LIST")
    inventory_manifest_uri: Mapped[str | None] = mapped_column(String(4096), nullable=True)
    inventory_file_index: Mapped[int] = mapped_column(Integer, default=0)
    inventory_rows_completed: Mapped[int] = mapped_column(BigInteger, default=0)
    is_rediscovery: Mapped[bool] = mapped_column(default=False)
    justification: Mapped[str | None] = mapped_column(Text, nullable=True)
    objects_new: Mapped[int] = mapped_column(BigInteger, default=0)
    objects_updated: Mapped[int] = mapped_column(BigInteger, default=0)
    objects_changed: Mapped[int] = mapped_column(BigInteger, default=0)
    state: Mapped[str] = mapped_column(String(16), default=TaskState.READY, index=True)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    worker_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    source: Mapped[Source] = relationship()


class Event(Base):
    __tablename__ = "events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    source_id: Mapped[int | None] = mapped_column(ForeignKey("sources.id"), nullable=True, index=True)
    wave_id: Mapped[int | None] = mapped_column(ForeignKey("waves.id"), nullable=True, index=True)
    kind: Mapped[str] = mapped_column(String(64), index=True)
    message: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)


class DiscoveryChange(Base):
    """A durable observation that must not rewrite a migrated object record."""
    __tablename__ = "discovery_changes"
    __table_args__ = (Index("ix_discovery_changes_source_detected", "source_id", "detected_at"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"), index=True)
    discovery_job_id: Mapped[int | None] = mapped_column(ForeignKey("discovery_jobs.id"), nullable=True, index=True)
    object_key: Mapped[str] = mapped_column(String(2048), index=True)
    change_type: Mapped[str] = mapped_column(String(32), default="MODIFIED")
    previous_size_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    current_size_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    previous_etag: Mapped[str | None] = mapped_column(String(128), nullable=True)
    current_etag: Mapped[str | None] = mapped_column(String(128), nullable=True)
    previous_version_id: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    current_version_id: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    previous_last_modified: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    current_last_modified: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    reprocessed_object_id: Mapped[int | None] = mapped_column(ForeignKey("objects.id"), nullable=True, index=True)
    reprocessed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)


def discovery_row_changed(existing: ObjectRecord, row: dict) -> bool:
    """Compare only evidence supplied by both discovery methods.

    Inventory exports can omit ETag/LastModified fields; an absent field is not
    proof that an already tracked object changed.
    """
    if int(existing.size_bytes) != int(row["size_bytes"]):
        return True
    for field in ("etag", "version_id", "last_modified"):
        current, incoming = getattr(existing, field), row.get(field)
        if current is not None and incoming is not None and current != incoming:
            return True
    return False


def merge_discovery_rows(session: Session, source: Source, rows: list[dict], job: DiscoveryJob | None = None) -> tuple[int, int, int]:
    """Merge a rediscovery without mutating records already assigned to waves.

    New keys become eligible for subsequent waves. Changed historical keys are
    recorded separately so the operator can decide how to migrate the new S3
    revision while the evidence for the previous migration remains intact.
    """
    if not rows:
        return 0, 0, 0
    keys = [row["object_key"] for row in rows]
    existing = {obj.object_key: obj for obj in session.scalars(
        select(ObjectRecord).where(ObjectRecord.source_id == source.id, ObjectRecord.is_current_revision.is_(True), ObjectRecord.object_key.in_(keys))
    )}
    fresh: list[dict] = []
    new = updated = changed = 0
    for row in rows:
        prior = existing.get(row["object_key"])
        if prior is None:
            fresh.append(row)
            new += 1
            continue
        if not discovery_row_changed(prior, row):
            continue
        # Any wave assignment is audit evidence. Do not silently repoint an
        # already planned/restored/transferred object to a newer S3 revision.
        if prior.wave_id is not None or prior.state != ObjectState.DISCOVERED:
            duplicate = session.scalar(select(DiscoveryChange.id).where(
                DiscoveryChange.source_id == source.id,
                DiscoveryChange.object_key == row["object_key"],
                DiscoveryChange.change_type == "MODIFIED",
                DiscoveryChange.current_size_bytes == row["size_bytes"],
                DiscoveryChange.current_etag == row.get("etag"),
            ).limit(1))
            if not duplicate:
                session.add(DiscoveryChange(source_id=source.id, discovery_job_id=job.id if job else None,
                                            object_key=row["object_key"], previous_size_bytes=prior.size_bytes,
                                            current_size_bytes=row["size_bytes"], previous_etag=prior.etag,
                                            current_etag=row.get("etag"), previous_version_id=prior.version_id,
                                            current_version_id=row.get("version_id"), previous_last_modified=prior.last_modified,
                                            current_last_modified=row.get("last_modified")))
                changed += 1
            continue
        # Even when it is still unassigned, retain a concise observation for
        # the operator. The row itself can safely be refreshed because no wave
        # evidence references it yet.
        session.add(DiscoveryChange(source_id=source.id, discovery_job_id=job.id if job else None,
                                    object_key=row["object_key"], change_type="UPDATED",
                                    previous_size_bytes=prior.size_bytes, current_size_bytes=row["size_bytes"],
                                    previous_etag=prior.etag, current_etag=row.get("etag"),
                                    previous_version_id=prior.version_id, current_version_id=row.get("version_id"),
                                    previous_last_modified=prior.last_modified, current_last_modified=row.get("last_modified")))
        for field in ("size_bytes", "version_id", "etag", "storage_class", "last_modified", "source_checksum", "checksum_algorithm"):
            if field in row and row[field] is not None:
                setattr(prior, field, row[field])
        updated += 1
        changed += 1
    if fresh:
        session.bulk_insert_mappings(ObjectRecord, fresh)
    if job:
        job.objects_new += new
        job.objects_updated += updated
        job.objects_changed += changed
    return new, updated, changed


class RuntimeSettings(Base):
    __tablename__ = "runtime_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    max_throughput_mbps: Mapped[int] = mapped_column(Integer, default=1100)
    multipart_part_size_mib: Mapped[int] = mapped_column(Integer, default=64)
    default_wave_size_bytes: Mapped[int] = mapped_column(BigInteger, default=10 * 1024**4)
    default_restore_days: Mapped[int] = mapped_column(Integer, default=7)
    default_restore_tier: Mapped[str] = mapped_column(String(16), default="BULK")
    task_lease_seconds: Mapped[int] = mapped_column(Integer, default=300)
    aws_migration_role_arn: Mapped[str] = mapped_column(String(2048), default="")
    aws_batch_role_arn: Mapped[str] = mapped_column(String(2048), default="")
    aws_control_bucket: Mapped[str] = mapped_column(String(255), default="")
    aws_control_prefix: Mapped[str] = mapped_column(String(1024), default="s3-oci-control/")
    preserve_s3_tags: Mapped[bool] = mapped_column(default=True)
    cost_estimation_enabled: Mapped[bool] = mapped_column(default=False)
    cost_include_aws_transfer_out: Mapped[bool] = mapped_column(default=True)
    cost_pricing_auto_refresh_enabled: Mapped[bool] = mapped_column(default=True)
    cost_pricing_refresh_days: Mapped[int] = mapped_column(Integer, default=7)
    activity_auto_refresh_enabled: Mapped[bool] = mapped_column(default=True)
    activity_refresh_seconds: Mapped[int] = mapped_column(Integer, default=15)
    dynamic_wave_target_seconds: Mapped[int] = mapped_column(Integer, default=12 * 3600)
    dynamic_wave_max_objects: Mapped[int] = mapped_column(Integer, default=50000)
    dynamic_restore_safety_seconds: Mapped[int] = mapped_column(Integer, default=6 * 3600)
    dynamic_restore_horizon_waves: Mapped[int] = mapped_column(Integer, default=3)
    # Raikou starts with two restore slots and may grow only up to this
    # operator-defined ceiling after it has timing evidence.
    dynamic_restore_max_slots: Mapped[int] = mapped_column(Integer, default=4)
    continuous_transfer_min_buffer_seconds: Mapped[int] = mapped_column(Integer, default=3 * 3600)
    continuous_transfer_target_buffer_seconds: Mapped[int] = mapped_column(Integer, default=6 * 3600)
    continuous_transfer_max_buffer_seconds: Mapped[int] = mapped_column(Integer, default=24 * 3600)
    continuous_transfer_batch_max_objects: Mapped[int] = mapped_column(Integer, default=100)
    continuous_transfer_batch_max_bytes: Mapped[int] = mapped_column(BigInteger, default=1024**3)
    continuous_transfer_critical_batch_max_objects: Mapped[int] = mapped_column(Integer, default=20)
    continuous_transfer_critical_batch_max_bytes: Mapped[int] = mapped_column(BigInteger, default=256 * 1024**2)
    continuous_transfer_critical_priority: Mapped[int] = mapped_column(Integer, default=90)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class SourcePrefix(Base):
    """One durable S3 prefix belonging to a source migration scope."""
    __tablename__ = "source_prefixes"
    __table_args__ = (UniqueConstraint("source_id", "prefix", name="uq_source_prefixes_source_prefix"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"), index=True)
    prefix: Mapped[str] = mapped_column(String(1024), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    source: Mapped[Source] = relationship(back_populates="prefixes")


def normalize_source_prefixes(prefixes: list[str] | None, legacy_prefix: str = "") -> list[str]:
    """Return a stable, non-overlapping source scope.

    Multiple prefixes are a union, never an instruction to repeatedly scan the
    same key.  Reject overlapping values inside one source because that would
    duplicate discovery, restore and transfer work.  Different test sources
    may intentionally overlap; the UI makes that operational risk explicit.
    """
    values = prefixes if prefixes else [legacy_prefix]
    normalized = sorted({str(value or "").strip().lstrip("/") for value in values})
    if not normalized:
        return [""]
    if "" in normalized and len(normalized) > 1:
        raise HTTPException(status_code=422, detail="A whole-bucket scope cannot be combined with other S3 prefixes")
    for index, prefix in enumerate(normalized):
        for other in normalized[index + 1:]:
            if other.startswith(prefix):
                raise HTTPException(status_code=422, detail=f"Overlapping S3 prefixes are not allowed in the same source: '{prefix}' and '{other}'")
    return normalized


def source_prefix_values(source: Source) -> list[str]:
    values = [row.prefix for row in source.prefixes]
    return normalize_source_prefixes(values, source.s3_prefix)


def source_key_in_scope(source: Source, object_key: str) -> bool:
    return any(not prefix or object_key.startswith(prefix) for prefix in source_prefix_values(source))


def s3_prefixes_overlap(left: str, right: str) -> bool:
    """Whether two normalized S3 prefix scopes can address the same key."""
    return not left or not right or left.startswith(right) or right.startswith(left)


def active_source_scope_conflicts(session: Session, bucket: str, prefixes: list[str],
                                  exclude_source_id: int | None = None) -> list[dict]:
    """Find active sources whose S3 scope intersects the requested scope.

    S3 has no folder boundary: ``app`` also includes ``app-images``.  The
    comparison intentionally follows S3's literal prefix semantics instead of
    inventing path semantics, so no object can be discovered twice unnoticed.
    """
    query = select(Source).where(Source.s3_bucket == bucket, Source.archived_at.is_(None))
    if exclude_source_id is not None:
        query = query.where(Source.id != exclude_source_id)
    conflicts: list[dict] = []
    for source in session.scalars(query):
        for requested in prefixes:
            for existing in source_prefix_values(source):
                if s3_prefixes_overlap(requested, existing):
                    conflicts.append({"source_id": source.id, "source_name": source.name,
                                      "requested_prefix": requested, "existing_prefix": existing})
    return conflicts


def require_non_overlapping_source_scope(session: Session, source: Source) -> None:
    """Block new operational work for an ambiguous active source scope."""
    # Scenario catalogs are isolated by execution. Equal virtual bucket and
    # prefix labels in different scenarios cannot address the same object.
    if runtime_context.is_simulation:
        return
    conflicts = active_source_scope_conflicts(session, source.s3_bucket, source_prefix_values(source), source.id)
    if conflicts:
        names = ", ".join(sorted({item["source_name"] for item in conflicts}))
        raise HTTPException(
            status_code=409,
            detail=(f"S3 scope overlaps active source(s): {names}. Archive the conflicting source or remove the overlapping prefix."),
        )


class SourceCreate(BaseModel):
    name: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{1,127}$")
    s3_bucket: str
    # ``s3_prefix`` remains accepted for API compatibility; new callers use
    # the normalized list and may provide as many independent prefixes as
    # needed for one source scope.
    s3_prefix: str = ""
    s3_prefixes: list[str] = Field(default_factory=list, max_length=1000)
    aws_region: str
    aws_connection_id: int | None = None
    destination_bucket: str
    # Lower values are preferred only when deadline risk is equivalent.  The
    # default deliberately means "no business preference".
    business_priority: int = Field(default=999, ge=1, le=999)


class SourceUpdate(SourceCreate):
    pass


class SimulationScenarioBootstrap(BaseModel):
    name: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{1,127}$")
    fidelity: str = Field(pattern="^(CONTROL|DATA)$")
    seed: str = Field(min_length=1, max_length=255)
    logical_size_bytes: int = Field(ge=0)
    object_count: int = Field(gt=0, le=10_000_000)
    physical_budget_bytes: int = Field(default=1_000_000_000_000, ge=0)
    clock_acceleration: float = Field(default=3600.0, gt=0)
    retention_days: int = Field(default=60, ge=0, le=3650)
    quarantine_days: int = Field(default=30, ge=0, le=3650)
    source_bucket: str = Field(min_length=1, max_length=255)
    destination_bucket: str = Field(min_length=1, max_length=255)
    region: str = Field(default="us-east-1", min_length=1, max_length=64)
    prefixes: list[str] = Field(default_factory=lambda: ["simulation"], min_length=1, max_length=1000)
    storage_class: str = Field(default="DEEP_ARCHIVE", max_length=64)
    template_id: str | None = None
    configuration: dict = Field(default_factory=dict)
    fault_rules: list = Field(default_factory=list)


class SimulationExecutionClone(BaseModel):
    name: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{1,127}$")
    fidelity: str | None = Field(default=None, pattern="^(CONTROL|DATA)$")
    physical_budget_bytes: int | None = Field(default=None, ge=0)


class SimulationScenarioPurge(BaseModel):
    confirmation: str
    reason: str = Field(min_length=10, max_length=2000)


class SimulationTemplateWrite(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    description: str = Field(default="", max_length=4000)
    fidelity: str = Field(pattern="^(CONTROL|DATA)$")
    configuration: dict = Field(default_factory=dict)
    fault_rules: list = Field(default_factory=list)


class OperationModeSwitchRequest(BaseModel):
    target_mode: str = Field(pattern="^(REAL|SIMULATION)$")
    confirmed: bool = False


class LegacySourceConnectionMigration(BaseModel):
    """One-way adoption of a pre-connection source without changing audit data."""
    aws_connection_id: int = Field(gt=0)


class AwsConnectionCreate(BaseModel):
    secret_ocid: str = Field(min_length=20, max_length=255)
    label: str = Field(min_length=1, max_length=255)


class InventoryManifestImport(BaseModel):
    manifest_uri: str = Field(min_length=10, max_length=4096)


class RediscoveryRequest(BaseModel):
    justification: str = Field(min_length=11, max_length=4000)


class RediscoveryManifestImport(InventoryManifestImport, RediscoveryRequest):
    pass


class CostPricingUpdate(BaseModel):
    currency: str = Field(default="USD", min_length=3, max_length=8, pattern=r"^[A-Za-z]{3,8}$")
    reference: str = Field(default="", max_length=1024)
    expected_restore_poll_cycles: int = Field(default=24, ge=1, le=1000)
    include_aws_transfer_out: bool = True
    include_oci_costs: bool = True
    aws_batch_job_usd: float | None = Field(default=None, ge=0)
    aws_batch_object_usd_per_1000: float | None = Field(default=None, ge=0)
    aws_s3_put_list_usd_per_1000: float | None = Field(default=None, ge=0)
    aws_s3_get_usd_per_1000: float | None = Field(default=None, ge=0)
    aws_glacier_bulk_retrieval_usd_per_gib: float | None = Field(default=None, ge=0)
    aws_glacier_standard_retrieval_usd_per_gib: float | None = Field(default=None, ge=0)
    aws_deep_archive_bulk_retrieval_usd_per_gib: float | None = Field(default=None, ge=0)
    aws_deep_archive_standard_retrieval_usd_per_gib: float | None = Field(default=None, ge=0)
    aws_transfer_out_usd_per_gib: float | None = Field(default=None, ge=0)
    aws_restore_temp_standard_usd_per_gib_month: float | None = Field(default=None, ge=0)
    oci_put_usd_per_10000: float | None = Field(default=None, ge=0)
    oci_get_usd_per_10000: float | None = Field(default=None, ge=0)
    oci_storage_usd_per_gib_month: float | None = Field(default=None, ge=0)


class AwsConnectionOperationalLimitsUpdate(BaseModel):
    discovery_requests_per_second: int = Field(default=10, ge=1, le=100)
    restore_poll_requests_per_second: int = Field(default=10, ge=1, le=100)
    restore_poll_concurrency: int = Field(default=10, ge=1, le=64)


class InventoryItem(BaseModel):
    object_key: str
    size_bytes: int = Field(ge=0)
    version_id: str | None = None
    etag: str | None = None
    storage_class: str | None = None
    last_modified: datetime | None = None
    metadata_json: str = "{}"
    tags_json: str = "{}"
    source_checksum: str | None = None
    checksum_algorithm: str | None = None


class InventoryImport(BaseModel):
    items: list[InventoryItem] = Field(min_length=1, max_length=10000)


class WaveCreate(BaseModel):
    name: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{1,127}$")
    max_bytes: int = Field(gt=0, le=10 * 1024**4)
    restore_days: int = Field(ge=1, le=30)
    restore_tier: str = Field(pattern="^(BULK|STANDARD)$")
    transfer_release_policy: str = Field(
        default="AFTER_ALL_RESTORED",
        pattern="^(AFTER_ALL_RESTORED|AS_OBJECTS_AVAILABLE)$",
    )


class WaveReprocessRequest(BaseModel):
    """Explicit approval is mandatory only when another paid restore is needed."""
    approve_new_restore: bool = False


class AutomaticWaveCreate(BaseModel):
    max_bytes: int = Field(gt=0, le=10 * 1024**4)
    restore_days: int = Field(ge=1, le=30)
    restore_tier: str = Field(pattern="^(BULK|STANDARD)$")
    prefix: str = Field(default="", max_length=1024)
    transfer_release_policy: str = Field(
        default="AFTER_ALL_RESTORED",
        pattern="^(AFTER_ALL_RESTORED|AS_OBJECTS_AVAILABLE)$",
    )


class DynamicWaveCreate(BaseModel):
    """Start an adaptive dynamic pipeline; restore scheduling is mandatory."""
    restore_days: int = Field(ge=1, le=30)
    restore_tier: str = Field(pattern="^(BULK|STANDARD)$")
    prefix: str = Field(default="", max_length=1024)


class ClaimRequest(BaseModel):
    worker_id: str = Field(min_length=1, max_length=128)
    lease_seconds: int = Field(default=300, ge=30, le=3600)


class TaskUpdate(BaseModel):
    worker_id: str = Field(min_length=1, max_length=128)
    error: str | None = Field(default=None, max_length=8000)
    retry_after_seconds: int = Field(default=300, ge=30, le=86400)


class RuntimeSettingsUpdate(BaseModel):
    # Reject retired controls explicitly.  Operational concurrency, the
    # dynamic pipeline and the real worker are no longer operator switches.
    model_config = ConfigDict(extra="forbid")

    max_throughput_mbps: int = Field(ge=1, le=1200)
    multipart_part_size_mib: int = Field(ge=16, le=512)
    default_wave_size_bytes: int = Field(gt=0, le=10 * 1024**4)
    default_restore_days: int = Field(ge=1, le=30)
    default_restore_tier: str = Field(pattern="^(BULK|STANDARD)$")
    task_lease_seconds: int = Field(ge=30, le=3600)
    preserve_s3_tags: bool = True
    cost_estimation_enabled: bool = False
    cost_include_aws_transfer_out: bool | None = None
    cost_pricing_auto_refresh_enabled: bool = True
    cost_pricing_refresh_days: int = Field(default=7, ge=1, le=90)
    dynamic_restore_safety_seconds: int = Field(default=6 * 3600, ge=0, le=7 * 24 * 3600)
    dynamic_restore_horizon_waves: int = Field(default=3, ge=1, le=20)
    dynamic_restore_max_slots: int = Field(default=4, ge=2, le=4)
    continuous_transfer_min_buffer_seconds: int = Field(default=3 * 3600, ge=0, le=7 * 24 * 3600)
    continuous_transfer_target_buffer_seconds: int = Field(default=6 * 3600, ge=0, le=7 * 24 * 3600)
    continuous_transfer_max_buffer_seconds: int = Field(default=24 * 3600, ge=0, le=14 * 24 * 3600)
    continuous_transfer_batch_max_objects: int = Field(default=100, ge=1, le=10_000)
    continuous_transfer_batch_max_bytes: int = Field(default=1024**3, ge=1024**2, le=10 * 1024**3)
    continuous_transfer_critical_batch_max_objects: int = Field(default=20, ge=1, le=1_000)
    continuous_transfer_critical_batch_max_bytes: int = Field(default=256 * 1024**2, ge=1024**2, le=10 * 1024**3)
    continuous_transfer_critical_priority: int = Field(default=90, ge=1, le=100)


class GlobalOutboundCostUpdate(BaseModel):
    include_aws_transfer_out: bool


class ActivityRefreshSettingsUpdate(BaseModel):
    enabled: bool
    seconds: int = Field(ge=5, le=300)


class DeepAuditStart(BaseModel):
    confirmed: bool = False


class IntegrityEvidence(BaseModel):
    source_checksum: str | None = Field(default=None, max_length=256)
    destination_checksum: str | None = Field(default=None, max_length=256)
    checksum_algorithm: str = Field(pattern="^(SHA256|MD5)$")
    verified: bool
    error: str | None = Field(default=None, max_length=4000)


app = FastAPI(title="S3 to OCI Migration", version=RAIJIN_SERVICE_VERSION)
app.mount("/static", StaticFiles(directory="app/static"), name="static")


@app.middleware("http")
async def simulation_foundation_guard(request: FastAPIRequest, call_next):
    """Keep Simulation isolated while exposing the normal Raijin control plane.

    Operational APIs work against the isolated simulation database and the
    simulated cloud backend.  Endpoints whose only purpose is discovering or
    configuring real AWS/OCI resources remain unavailable as defence in depth;
    Simulation never receives cloud credentials in the first place.
    """
    from app.runtime_context import mode_switch_requested
    if request.method not in {"GET", "HEAD", "OPTIONS"} and mode_switch_requested() and request.url.path != "/api/runtime/mode":
        return JSONResponse(
            status_code=503,
            content={"detail": "RAIJIN is draining for an operation-mode switch"},
        )
    if runtime_context.is_simulation:
        path = request.url.path
        real_cloud_only = (
            path == "/api/readiness"
            or path.startswith("/api/oci/")
            or path.startswith("/api/aws-secrets")
            or path.startswith("/api/aws-connections")
        )
        # The regular source form creates cloud-backed sources. Simulated
        # sources are created from immutable scenarios on /simulation.
        real_source_configuration = (
            (path == "/api/sources" and request.method == "POST")
            or (path.startswith("/api/sources/") and request.method == "PUT")
            or path.endswith("/migrate-aws-connection")
        )
        if real_cloud_only or real_source_configuration:
            return JSONResponse(
                status_code=503,
                content={
                    "detail": "This real-cloud configuration endpoint is unavailable in Simulation mode",
                    "operation_mode": runtime_context.mode.value,
                },
            )
    response = await call_next(request)
    # Sources, waves and tasks are operational state.  A browser must never
    # reuse an earlier /api/sources response after a scenario/source is
    # created, archived or otherwise changed.
    if request.url.path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-store"
    return response


@app.on_event("startup")
def create_schema() -> None:
    # A Simulation process must prove it is talking to the expected external
    # backend before touching its isolated schema. Operations may still be
    # disabled while the following simulator phases are being implemented.
    cloud_backend.readiness(require_operations=False)
    Base.metadata.create_all(engine)
    # Lightweight additive migrations keep the single-VM deployment upgradeable. No
    # destructive schema operation is performed automatically.
    expected_columns = {
        "is_current_revision": "BOOLEAN NOT NULL DEFAULT TRUE",
        "previous_object_id": "BIGINT",
        "superseded_at": "TIMESTAMP WITH TIME ZONE",
        "source_checksum": "VARCHAR(256)",
        "destination_checksum": "VARCHAR(256)",
        "checksum_algorithm": "VARCHAR(32)",
        "restore_requested_at": "TIMESTAMP WITH TIME ZONE",
        "restore_attempt_id": "BIGINT",
        "restored_at": "TIMESTAMP WITH TIME ZONE",
        "restore_expires_at": "TIMESTAMP WITH TIME ZONE",
        "transferred_at": "TIMESTAMP WITH TIME ZONE",
        "transfer_started_at": "TIMESTAMP WITH TIME ZONE",
        "transfer_elapsed_seconds": "DOUBLE PRECISION NOT NULL DEFAULT 0",
        "planned_transfer_seconds": "DOUBLE PRECISION NOT NULL DEFAULT 0",
        "transfer_progress_bytes": "BIGINT NOT NULL DEFAULT 0",
        "transfer_progress_at": "TIMESTAMP WITH TIME ZONE",
        "transfer_rate_mbps": "DOUBLE PRECISION NOT NULL DEFAULT 0",
        "delivery_integrity_algorithm": "VARCHAR(32)",
        "delivery_integrity_checksum": "VARCHAR(256)",
        "delivery_integrity_status": "VARCHAR(32)",
        "delivery_integrity_verified_at": "TIMESTAMP WITH TIME ZONE",
        "multipart_upload_id": "VARCHAR(255)",
        "multipart_part_size": "INTEGER",
        "multipart_parts_json": "TEXT NOT NULL DEFAULT '{}'",
        "multipart_updated_at": "TIMESTAMP WITH TIME ZONE",
        "audit_started_at": "TIMESTAMP WITH TIME ZONE",
        "audit_progress_bytes": "BIGINT NOT NULL DEFAULT 0",
        "audit_progress_at": "TIMESTAMP WITH TIME ZONE",
        "audit_rate_mbps": "DOUBLE PRECISION NOT NULL DEFAULT 0",
        "integrity_verified_at": "TIMESTAMP WITH TIME ZONE",
        "integrity_error": "TEXT",
    }
    existing_columns = {column["name"] for column in inspect(engine).get_columns("objects")}
    runtime_columns = {
        "aws_migration_role_arn": "VARCHAR(2048) NOT NULL DEFAULT ''",
        "aws_batch_role_arn": "VARCHAR(2048) NOT NULL DEFAULT ''",
        "aws_control_bucket": "VARCHAR(255) NOT NULL DEFAULT ''",
        "aws_control_prefix": "VARCHAR(1024) NOT NULL DEFAULT 's3-oci-control/'",
        "preserve_s3_tags": "BOOLEAN NOT NULL DEFAULT TRUE",
        "cost_estimation_enabled": "BOOLEAN NOT NULL DEFAULT FALSE",
        "cost_include_aws_transfer_out": "BOOLEAN NOT NULL DEFAULT TRUE",
        "cost_pricing_auto_refresh_enabled": "BOOLEAN NOT NULL DEFAULT TRUE",
        "cost_pricing_refresh_days": "INTEGER NOT NULL DEFAULT 7",
        "multipart_part_size_mib": "INTEGER NOT NULL DEFAULT 64",
        "activity_auto_refresh_enabled": "BOOLEAN NOT NULL DEFAULT TRUE",
        "activity_refresh_seconds": "INTEGER NOT NULL DEFAULT 15",
        "dynamic_wave_target_seconds": "INTEGER NOT NULL DEFAULT 43200",
        "dynamic_wave_max_objects": "INTEGER NOT NULL DEFAULT 50000",
        "dynamic_restore_safety_seconds": "INTEGER NOT NULL DEFAULT 21600",
        "dynamic_restore_horizon_waves": "INTEGER NOT NULL DEFAULT 3",
        "dynamic_restore_max_slots": "INTEGER NOT NULL DEFAULT 4",
        "continuous_transfer_min_buffer_seconds": "INTEGER NOT NULL DEFAULT 10800",
        "continuous_transfer_target_buffer_seconds": "INTEGER NOT NULL DEFAULT 21600",
        "continuous_transfer_max_buffer_seconds": "INTEGER NOT NULL DEFAULT 86400",
        "continuous_transfer_batch_max_objects": "INTEGER NOT NULL DEFAULT 100",
        "continuous_transfer_batch_max_bytes": "BIGINT NOT NULL DEFAULT 1073741824",
        "continuous_transfer_critical_batch_max_objects": "INTEGER NOT NULL DEFAULT 20",
        "continuous_transfer_critical_batch_max_bytes": "BIGINT NOT NULL DEFAULT 268435456",
        "continuous_transfer_critical_priority": "INTEGER NOT NULL DEFAULT 90",
    }
    source_columns = {"discovery_requested_at": "TIMESTAMP WITH TIME ZONE", "discovery_started_at": "TIMESTAMP WITH TIME ZONE", "discovery_elapsed_seconds": "DOUBLE PRECISION NOT NULL DEFAULT 0", "discovery_completed_at": "TIMESTAMP WITH TIME ZONE", "discovery_error": "TEXT", "discovery_continuation_token": "TEXT", "discovery_prefix_index": "INTEGER NOT NULL DEFAULT 0", "discovery_pages_completed": "INTEGER NOT NULL DEFAULT 0", "discovery_objects_inserted": "BIGINT NOT NULL DEFAULT 0", "last_discovery_mode": "VARCHAR(32)", "discovery_generation": "INTEGER NOT NULL DEFAULT 0", "aws_connection_id": "INTEGER", "aws_bucket_region": "VARCHAR(64)", "backend_kind": "VARCHAR(16) NOT NULL DEFAULT 'REAL'", "simulation_scenario_id": "VARCHAR(36)", "simulation_execution_id": "VARCHAR(36)", "simulation_correlation_id": "VARCHAR(36)", "simulation_tenant_id": "VARCHAR(36)", "simulation_project_id": "VARCHAR(36)", "simulation_fidelity": "VARCHAR(16)", "business_priority": "INTEGER NOT NULL DEFAULT 999"}
    source_columns["archived_at"] = "TIMESTAMP WITH TIME ZONE"
    source_columns.update({"destination_validation_at": "TIMESTAMP WITH TIME ZONE", "destination_validation_status": "VARCHAR(32)", "destination_missing_count": "INTEGER NOT NULL DEFAULT 0", "destination_size_mismatch_count": "INTEGER NOT NULL DEFAULT 0", "destination_metadata_mismatch_count": "INTEGER NOT NULL DEFAULT 0", "destination_extra_count": "INTEGER NOT NULL DEFAULT 0"})
    wave_columns = {"batch_job_id": "VARCHAR(128)", "batch_job_status": "VARCHAR(64)", "manifest_key": "VARCHAR(2048)", "manifest_etag": "VARCHAR(128)", "last_poll_at": "TIMESTAMP WITH TIME ZONE", "poll_count": "INTEGER NOT NULL DEFAULT 0", "pipeline_run_id": "BIGINT", "availability_head_requests": "BIGINT NOT NULL DEFAULT 0", "availability_poll_elapsed_seconds": "DOUBLE PRECISION NOT NULL DEFAULT 0", "availability_throttle_retries": "INTEGER NOT NULL DEFAULT 0", "last_availability_poll_objects": "INTEGER NOT NULL DEFAULT 0", "last_availability_poll_seconds": "DOUBLE PRECISION NOT NULL DEFAULT 0", "planner_mode": "VARCHAR(32) NOT NULL DEFAULT 'MANUAL'", "predicted_transfer_seconds": "DOUBLE PRECISION NOT NULL DEFAULT 0", "prediction_samples": "INTEGER NOT NULL DEFAULT 0", "active_transfer_workers": "INTEGER NOT NULL DEFAULT 0", "planned_restore_at": "TIMESTAMP WITH TIME ZONE", "planned_transfer_start_at": "TIMESTAMP WITH TIME ZONE", "restore_requested_virtual_at": "TIMESTAMP WITH TIME ZONE", "first_restore_available_virtual_at": "TIMESTAMP WITH TIME ZONE", "last_restore_available_virtual_at": "TIMESTAMP WITH TIME ZONE", "transfer_started_virtual_at": "TIMESTAMP WITH TIME ZONE", "transfer_completed_virtual_at": "TIMESTAMP WITH TIME ZONE", "simulation_transfer_clock_held": "BOOLEAN NOT NULL DEFAULT FALSE", "restore_reapproval_required": "BOOLEAN NOT NULL DEFAULT FALSE", "restore_reapproval_reason": "TEXT", "restore_reapproval_detected_at": "TIMESTAMP WITH TIME ZONE", "transfer_release_policy": "VARCHAR(32) NOT NULL DEFAULT 'AS_OBJECTS_AVAILABLE'"}
    existing_runtime_columns = {column["name"] for column in inspect(engine).get_columns("runtime_settings")}
    existing_source_columns = {column["name"] for column in inspect(engine).get_columns("sources")}
    existing_wave_columns = {column["name"] for column in inspect(engine).get_columns("waves")}
    existing_bucket_columns = {column["name"] for column in inspect(engine).get_columns("oci_bucket_cache")}
    existing_cost_pricing_columns = {column["name"] for column in inspect(engine).get_columns("cost_pricing")}
    existing_discovery_job_columns = {column["name"] for column in inspect(engine).get_columns("discovery_jobs")}
    existing_connection_columns = {column["name"] for column in inspect(engine).get_columns("aws_connections")}
    existing_run_columns = {column["name"] for column in inspect(engine).get_columns("dynamic_pipeline_runs")}
    existing_restore_attempt_columns = {column["name"] for column in inspect(engine).get_columns("restore_attempts")}
    existing_lane_columns = {column["name"] for column in inspect(engine).get_columns("transfer_queue_items")}
    existing_discovery_change_columns = {column["name"] for column in inspect(engine).get_columns("discovery_changes")}
    with engine.begin() as connection:
        if engine.dialect.name == "postgresql" and "simulation_enabled" in existing_runtime_columns:
            connection.execute(text("ALTER TABLE runtime_settings DROP COLUMN simulation_enabled"))
        for column, sql_type in expected_columns.items():
            if column not in existing_columns:
                connection.execute(text(f"ALTER TABLE objects ADD COLUMN {column} {sql_type}"))
        for column, sql_type in runtime_columns.items():
            if column not in existing_runtime_columns:
                connection.execute(text(f"ALTER TABLE runtime_settings ADD COLUMN {column} {sql_type}"))
        lane_columns = {
            "dispatch_batch_id": "BIGINT",
            "preemption_cooldown_until": "TIMESTAMP WITH TIME ZONE",
            "preemption_successor_item_id": "BIGINT",
            "preemption_requested_at": "TIMESTAMP WITH TIME ZONE",
        }
        for column, sql_type in lane_columns.items():
            if column not in existing_lane_columns:
                connection.execute(text(f"ALTER TABLE transfer_queue_items ADD COLUMN {column} {sql_type}"))
        for column, sql_type in source_columns.items():
            if column not in existing_source_columns:
                connection.execute(text(f"ALTER TABLE sources ADD COLUMN {column} {sql_type}"))
        for column, sql_type in {
            "priority_details_json": "TEXT NOT NULL DEFAULT '{}'",
            "last_dispatched_at": "TIMESTAMP WITH TIME ZONE",
            "preemption_count": "INTEGER NOT NULL DEFAULT 0",
        }.items():
            if column not in existing_lane_columns:
                connection.execute(text(f"ALTER TABLE transfer_queue_items ADD COLUMN {column} {sql_type}"))
        # Preserve visibility for discoveries completed before elapsed-time was
        # introduced.  New/retried discoveries use precise accumulated work
        # time; this one-time backfill is the best durable historical value.
        if engine.dialect.name == "postgresql":
            connection.execute(text("""
                UPDATE sources
                SET discovery_elapsed_seconds = EXTRACT(EPOCH FROM (discovery_completed_at - discovery_requested_at))
                WHERE discovery_elapsed_seconds = 0
                  AND discovery_completed_at IS NOT NULL
                  AND discovery_requested_at IS NOT NULL
            """))
        elif engine.dialect.name == "sqlite":
            connection.execute(text("""
                UPDATE sources
                SET discovery_elapsed_seconds = (julianday(discovery_completed_at) - julianday(discovery_requested_at)) * 86400.0
                WHERE discovery_elapsed_seconds = 0
                  AND discovery_completed_at IS NOT NULL
                  AND discovery_requested_at IS NOT NULL
            """))
        for column, sql_type in wave_columns.items():
            if column not in existing_wave_columns:
                connection.execute(text(f"ALTER TABLE waves ADD COLUMN {column} {sql_type}"))
        for column, sql_type in {
            "discovery_requests_per_second": "INTEGER NOT NULL DEFAULT 10",
            "restore_poll_requests_per_second": "INTEGER NOT NULL DEFAULT 10",
            "restore_poll_concurrency": "INTEGER NOT NULL DEFAULT 10",
        }.items():
            if column not in existing_connection_columns:
                connection.execute(text(f"ALTER TABLE aws_connections ADD COLUMN {column} {sql_type}"))
        for column, sql_type in {
            "restore_horizon_waves": "INTEGER NOT NULL DEFAULT 3",
            "selection_prefix": "VARCHAR(1024) NOT NULL DEFAULT ''",
            "next_sequence": "INTEGER NOT NULL DEFAULT 1",
        }.items():
            if column not in existing_run_columns:
                connection.execute(text(f"ALTER TABLE dynamic_pipeline_runs ADD COLUMN {column} {sql_type}"))
        for column, sql_type in {
            "batch_describe_requests": "INTEGER NOT NULL DEFAULT 0",
            "completion_report_list_requests": "INTEGER NOT NULL DEFAULT 0",
            "completion_report_get_requests": "INTEGER NOT NULL DEFAULT 0",
        }.items():
            if column not in existing_restore_attempt_columns:
                connection.execute(text(f"ALTER TABLE restore_attempts ADD COLUMN {column} {sql_type}"))
        if "include_aws_transfer_out" not in existing_cost_pricing_columns:
            connection.execute(text("ALTER TABLE cost_pricing ADD COLUMN include_aws_transfer_out BOOLEAN NOT NULL DEFAULT TRUE"))
        if "include_oci_costs" not in existing_cost_pricing_columns:
            connection.execute(text("ALTER TABLE cost_pricing ADD COLUMN include_oci_costs BOOLEAN NOT NULL DEFAULT TRUE"))
        if "compartment_name" not in existing_bucket_columns:
            connection.execute(text("ALTER TABLE oci_bucket_cache ADD COLUMN compartment_name VARCHAR(255)"))
        for column, sql_type in {
            "mode": "VARCHAR(32) NOT NULL DEFAULT 'REMOTE_LIST'",
            "inventory_manifest_uri": "VARCHAR(4096)",
            "inventory_file_index": "INTEGER NOT NULL DEFAULT 0",
            "inventory_rows_completed": "BIGINT NOT NULL DEFAULT 0",
            "is_rediscovery": "BOOLEAN NOT NULL DEFAULT FALSE",
            "justification": "TEXT",
            "objects_new": "BIGINT NOT NULL DEFAULT 0",
            "objects_updated": "BIGINT NOT NULL DEFAULT 0",
            "objects_changed": "BIGINT NOT NULL DEFAULT 0",
        }.items():
            if column not in existing_discovery_job_columns:
                connection.execute(text(f"ALTER TABLE discovery_jobs ADD COLUMN {column} {sql_type}"))
        for column, sql_type in {
            "previous_version_id": "VARCHAR(1024)", "current_version_id": "VARCHAR(1024)",
            "previous_last_modified": "TIMESTAMP WITH TIME ZONE", "current_last_modified": "TIMESTAMP WITH TIME ZONE",
            "reprocessed_object_id": "BIGINT", "reprocessed_at": "TIMESTAMP WITH TIME ZONE",
        }.items():
            if column not in existing_discovery_change_columns:
                connection.execute(text(f"ALTER TABLE discovery_changes ADD COLUMN {column} {sql_type}"))
    # Preserve prior dynamic waves as a single, clearly labelled historical
    # run per source. Their existing plans, object timestamps and task/event
    # history remain authoritative; this only creates the missing grouping.
    with SessionLocal() as session:
        # Existing single-prefix sources are adopted as a one-row normalized
        # scope without changing their source identity or audit records.
        for source in session.scalars(select(Source)):
            if not source.prefixes:
                session.add(SourcePrefix(source_id=source.id, prefix=source.s3_prefix or ""))
        legacy_source_ids = list(session.scalars(select(Wave.source_id).where(
            Wave.planner_mode == "DYNAMIC", Wave.pipeline_run_id.is_(None)
        ).distinct()))
        for source_id in legacy_source_ids:
            run = DynamicPipelineRun(source_id=source_id, planner_version="v1-legacy-import",
                                     status="HISTORICAL")
            session.add(run); session.flush()
            for wave in session.scalars(select(Wave).where(
                Wave.source_id == source_id, Wave.planner_mode == "DYNAMIC", Wave.pipeline_run_id.is_(None)
            )):
                wave.pipeline_run_id = run.id
        # Backfill the visual/audit origin tag for inventories completed by
        # releases before discovery provenance was stored on the source.
        for source in session.scalars(select(Source).where(
            Source.last_discovery_mode.is_(None), Source.discovery_completed_at.is_not(None)
        )):
            latest = session.scalar(select(DiscoveryJob).where(DiscoveryJob.source_id == source.id)
                                    .order_by(DiscoveryJob.id.desc()).limit(1))
            source.last_discovery_mode = latest.mode if latest else "LEGACY"
            source.discovery_generation = max(1, int(source.discovery_generation or 0))
        # Planner v1/v2 added the operational safety allowance to the first
        # AWS service window. Correct active forecasts once: the first handoff
        # is SLA-only, while safety continues to advance only future restore
        # submissions. Observed restore/task evidence is never rewritten.
        active_legacy_runs = list(session.scalars(select(DynamicPipelineRun).where(
            DynamicPipelineRun.scheduled_restores.is_(True),
            DynamicPipelineRun.planner_version.in_(["v1", "v2-adaptive"]),
            DynamicPipelineRun.status.not_in(["COMPLETED", "HISTORICAL"]),
        )))
        migration_now = utcnow()
        for run in active_legacy_runs:
            waves = list(session.scalars(select(Wave).where(
                Wave.pipeline_run_id == run.id
            ).order_by(Wave.planned_transfer_start_at.nulls_last(), Wave.id)))
            if not waves:
                run.planner_version = "v3-service-window"
                continue
            first_request_at = session.scalar(select(func.min(ObjectRecord.restore_requested_at)).join(
                Wave, ObjectRecord.wave_id == Wave.id
            ).where(Wave.pipeline_run_id == run.id))
            anchor = first_request_at or waves[0].planned_restore_at or run.created_at or migration_now
            cursor = anchor + timedelta(seconds=restore_service_window_seconds(waves[0].restore_tier))
            for wave in waves:
                wave.planned_transfer_start_at = cursor
                has_submission = session.scalar(select(Task.id).where(
                    Task.wave_id == wave.id, Task.kind == "SUBMIT_BATCH_RESTORE"
                ).limit(1)) is not None
                if wave.status == "RESTORE_SCHEDULED" and not has_submission:
                    restore_lead = restore_service_window_seconds(wave.restore_tier) + int(
                        run.restore_safety_seconds or 0
                    )
                    wave.planned_restore_at = max(migration_now, cursor - timedelta(seconds=restore_lead))
                cursor += timedelta(seconds=max(1, int(wave.predicted_transfer_seconds or 1)))
            run.planner_version = "v3-service-window"
            record_event(session, "DYNAMIC_SCHEDULE_SEMANTICS_UPGRADED",
                         f"Pipeline run {run.id} now separates the AWS service window from operational safety",
                         source_id=run.source_id)
        # Before the simulator surfaced restore expiry as a typed 409, this
        # exact condition appeared as an opaque logical-transfer HTTP 500.
        # Upgrade only that known signature to the explicit paid-restore
        # approval state; unrelated failed transfers retain their original
        # failure diagnosis.
        legacy_expiry_waves = list(session.scalars(
            select(Wave)
            .join(Source, Wave.source_id == Source.id)
            .join(Task, Task.wave_id == Wave.id)
            .where(
                Source.backend_kind == "SIMULATED",
                Wave.status == "FAILED",
                Wave.restore_reapproval_required.is_(False),
                Task.kind == "TRANSFER_CONTINUOUS",
                Task.state == TaskState.FAILED,
                Task.error.contains("logical-transfer"),
                Task.error.contains("HTTP Error 500"),
            )
            .distinct()
        ))
        for wave in legacy_expiry_waves:
            wave.status = "RESTORE_REAPPROVAL_REQUIRED"
            wave.restore_reapproval_required = True
            wave.restore_reapproval_reason = (
                "A previous simulator logical-transfer response reported HTTP 500 while a restored "
                "copy was being transferred. The current release treats this known condition as "
                "restore unavailability; a new restore requires explicit approval because it may incur AWS charges."
            )
            wave.restore_reapproval_detected_at = migration_now
            record_event(
                session,
                "RESTORE_REAPPROVAL_REQUIRED",
                "Historic simulator restore-expiry failure converted to explicit operator reapproval required",
                source_id=wave.source_id,
                wave_id=wave.id,
            )
        session.commit()


def get_session() -> Generator[Session, None, None]:
    with SessionLocal() as session:
        yield session


def source_or_404(session: Session, source_id: int) -> Source:
    source = session.get(Source, source_id)
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")
    return source


def wave_or_404(session: Session, wave_id: int) -> Wave:
    wave = session.get(Wave, wave_id)
    if not wave:
        raise HTTPException(status_code=404, detail="Wave not found")
    return wave


def refresh_dynamic_pipeline_run(session: Session, run: DynamicPipelineRun) -> str:
    """Derive and persist the durable lifecycle of one dynamic pipeline run."""
    statuses = list(session.scalars(select(Wave.status).where(Wave.pipeline_run_id == run.id)))
    succeeded = {"COMPLETED", "VERIFIED", "TRANSFERRED"}
    if statuses and all(status in succeeded for status in statuses):
        remaining = session.scalar(select(func.count(ObjectRecord.id)).where(
            *discovered_object_filters(run.source_id, run.selection_prefix)
        )) or 0
        # With a bounded horizon, completed waves do not mean the pipeline is
        # complete while undispatched inventory still exists.
        if remaining:
            run.status = "SCHEDULED"
            run.completed_at = None
        elif run.status != "COMPLETED":
            completed_at = session.scalar(select(func.max(ObjectRecord.transferred_at)).where(
                ObjectRecord.wave_id.in_(select(Wave.id).where(Wave.pipeline_run_id == run.id))
            ))
            run.status, run.completed_at = "COMPLETED", completed_at or utcnow()
    elif any(status in {"FAILED", "TRANSFERRED_WITH_ERRORS", "RESTORE_REQUEST_FAILED", "VERIFICATION_FAILED"} for status in statuses):
        if run.status != "COMPLETED":
            run.status = "NEEDS_ATTENTION"
    elif any(status in {"RESTORING", "RESTORED", "TRANSFERRING"} for status in statuses):
        if run.status not in {"COMPLETED", "HISTORICAL"}:
            run.status = "IN_PROGRESS"
    elif run.status not in {"COMPLETED", "HISTORICAL"}:
        run.status = "SCHEDULED" if run.scheduled_restores else "PLANNED"
    return run.status


def task_or_404(session: Session, task_id: int) -> Task:
    task = session.get(Task, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


def record_event(session: Session, kind: str, message: str, source_id: int | None = None, wave_id: int | None = None) -> None:
    session.add(Event(kind=kind, message=message, source_id=source_id, wave_id=wave_id))


def runtime_settings(session: Session) -> RuntimeSettings:
    settings = session.get(RuntimeSettings, 1)
    if not settings:
        settings = RuntimeSettings(id=1)
        session.add(settings)
        session.commit()
    return settings


def transfer_priority(score_deadline: datetime | None, predicted_seconds: float,
                      settings: RuntimeSettings, now: datetime | None = None,
                      *, wave_available_ratio: float = 0, age_seconds: float = 0,
                      source_business_priority: int = 999, size_bytes: int = 0) -> tuple[int, str, str, dict]:
    """Return an explainable 0–100 urgency score from remaining restore slack.

    Missing expiry is normal for non-archived objects. Such work stays normal
    priority; an archived copy with the least remaining viable slack always
    wins over it.
    """
    now = now or utcnow()
    # PostgreSQL preserves timezone metadata, while SQLite contract tests do
    # not.  Normalize both sides at this boundary so the same priority and
    # expiry semantics apply to real and simulated control databases.
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    if score_deadline is not None and score_deadline.tzinfo is None:
        score_deadline = score_deadline.replace(tzinfo=timezone.utc)
    # Deadline owns the dominant part of the score.  Availability, age and
    # business order can only break ties among viable work; none can postpone
    # an expiring restored copy.
    fairness = min(12, int(max(0, age_seconds) // 3600))
    wave_bonus = min(8, int(max(0, min(1, wave_available_ratio)) * 8))
    business_bonus = max(0, min(5, 6 - int(source_business_priority or 999)))
    small_object_bonus = 2 if 0 < size_bytes <= 16 * 1024**2 else 0
    details = {"deadline": 0, "wave_availability": wave_bonus, "fairness": fairness,
               "business_priority": business_bonus, "small_object": small_object_bonus}
    if score_deadline is None:
        score = min(59, 10 + fairness + wave_bonus + business_bonus + small_object_bonus)
        details["total"] = score
        return score, "NORMAL", "FIFO work elevated only by age, availability and source order", details
    remaining = max(0.0, (score_deadline - now).total_seconds())
    retry_budget = max(60.0, float(predicted_seconds) * .20)
    reserve = float(settings.continuous_transfer_min_buffer_seconds or 0)
    slack = remaining - max(1.0, predicted_seconds) - retry_budget - reserve
    if slack <= 0: base, band, reason = 100, "CRITICAL", "Deadline is inside transfer, retry and operational reserve"
    elif slack <= 30 * 60: base, band, reason = 95, "CRITICAL", "Less than 30 minutes of viable transfer slack"
    elif slack <= 2 * 3600: base, band, reason = 85, "URGENT", "Less than two hours of viable transfer slack"
    elif slack <= 6 * 3600: base, band, reason = 70, "ELEVATED", "Less than six hours of viable transfer slack"
    else: base, band, reason = 40, "NORMAL", "Viable transfer slack remains"
    # Criticality is never diluted or inflated past its deterministic bound.
    score = base if base >= 90 else min(89, base + fairness + wave_bonus + business_bonus + small_object_bonus)
    details.update({"deadline": base, "slack_seconds": int(slack), "total": score})
    return score, ("CRITICAL" if score >= 90 else "URGENT" if score >= 80 else "ELEVATED" if score >= 60 else "NORMAL"), reason, details


def enqueue_available_transfer_objects(session: Session, wave: Wave,
                                       observed_at: datetime | None = None) -> int:
    """Idempotently expose newly restored objects to the continuous lane.

    Polling is the sole source of this evidence, so this adds no AWS API call.
    A later state reconciliation can safely invoke it repeatedly because the
    unique `(wave_id, object_id)` boundary is checked before insertion.
    """
    settings = runtime_settings(session)
    observed_at = observed_at or utcnow()
    # Never materialize an entire restored wave in Python. A large Deep
    # Archive wave can release millions of objects over time; each Raikou pass
    # admits one bounded page and the next pass continues idempotently.
    queued_object = aliased(TransferQueueItem)
    restored = list(session.scalars(
        select(ObjectRecord)
        .outerjoin(
            queued_object,
            and_(queued_object.wave_id == ObjectRecord.wave_id,
                 queued_object.object_id == ObjectRecord.id),
        )
        .where(
            ObjectRecord.wave_id == wave.id,
            ObjectRecord.state == ObjectState.RESTORED,
            queued_object.id.is_(None),
        )
        .order_by(ObjectRecord.id)
        .limit(TRANSFER_LANE_ADMISSION_PAGE_SIZE)
    ))
    if not restored:
        return 0
    created = 0
    # The page size must not distort the availability signal.  Calculate the
    # whole-wave ratio inside PostgreSQL rather than treating this 1,000-row
    # admission page as the complete observed availability of a large wave.
    total_wave_objects, available_wave_objects = session.execute(
        select(
            func.count(ObjectRecord.id),
            func.coalesce(func.sum(case((ObjectRecord.state.in_([
                ObjectState.RESTORED, ObjectState.TRANSFERRING,
                ObjectState.TRANSFERRED, ObjectState.VERIFIED,
            ]), 1), else_=0)), 0),
        ).where(ObjectRecord.wave_id == wave.id)
    ).one()
    restored_ratio = int(available_wave_objects or 0) / max(1, int(total_wave_objects or 0))
    for obj in restored:
        score, band, reason, details = transfer_priority(
            obj.restore_expires_at, float(obj.planned_transfer_seconds or 0), settings, observed_at,
            wave_available_ratio=restored_ratio, source_business_priority=wave.source.business_priority,
            size_bytes=int(obj.size_bytes or 0),
        )
        session.add(TransferQueueItem(
            source_id=wave.source_id,
            wave_id=wave.id,
            object_id=obj.id,
            size_bytes=obj.size_bytes,
            available_at=obj.restored_at or observed_at,
            restore_expires_at=obj.restore_expires_at,
            predicted_transfer_seconds=float(obj.planned_transfer_seconds or 0),
            priority_score=score,
            priority_band=band,
            state=TransferQueueState.READY,
            decision_reason=reason,
            priority_details_json=json.dumps(details, sort_keys=True),
        ))
        created += 1
    if created:
        record_event(
            session, "CONTINUOUS_TRANSFER_ITEMS_READY",
            f"Raikou released {created} restored object(s) to the continuous transfer lane",
            source_id=wave.source_id, wave_id=wave.id,
        )
    return created


def refresh_transfer_queue_priorities(session: Session, source_id: int | None = None,
                                      now: datetime | None = None) -> int:
    """Refresh urgency before every dispatch without scanning inventory rows."""
    settings = runtime_settings(session)
    query = select(TransferQueueItem).where(TransferQueueItem.state.in_([
        TransferQueueState.AVAILABLE, TransferQueueState.READY,
        TransferQueueState.RETRY_WAIT, TransferQueueState.MULTIPART_RESUME,
    ]))
    if source_id is not None:
        query = query.where(TransferQueueItem.source_id == source_id)
    changed = 0
    reference_now = now or utcnow()
    if reference_now.tzinfo is None:
        reference_now = reference_now.replace(tzinfo=timezone.utc)
    # A priority refresh happens before every free Raiju slot. Refreshing all
    # source rows would eventually turn a 100-object dispatch into a full
    # queue scan. The order gives expiry precedence even if an old score has
    # not been refreshed in this pass; the bounded page then recalculates the
    # candidates that can actually be claimed next.
    items = list(session.scalars(
        query.order_by(
            TransferQueueItem.restore_expires_at.is_(None),
            TransferQueueItem.restore_expires_at,
            TransferQueueItem.priority_score.desc(),
            TransferQueueItem.available_at,
            TransferQueueItem.id,
        ).limit(TRANSFER_LANE_PRIORITY_REFRESH_PAGE_SIZE)
    ))
    if not items:
        return 0
    wave_ids = {item.wave_id for item in items}
    source_ids = {item.source_id for item in items}
    # This runs before each dispatch.  Aggregate once per wave instead of
    # issuing two inventory counts for every queued object.
    wave_counts = {
        int(wave_id): (int(total or 0), int(available or 0))
        for wave_id, total, available in session.execute(
            select(
                ObjectRecord.wave_id,
                func.count(ObjectRecord.id),
                func.coalesce(func.sum(case((ObjectRecord.state.in_([
                    ObjectState.RESTORED, ObjectState.TRANSFERRING,
                    ObjectState.TRANSFERRED, ObjectState.VERIFIED,
                ]), 1), else_=0)), 0),
            ).where(ObjectRecord.wave_id.in_(wave_ids)).group_by(ObjectRecord.wave_id)
        )
    }
    source_priorities = {
        int(id_): int(priority or 999)
        for id_, priority in session.execute(
            select(Source.id, Source.business_priority).where(Source.id.in_(source_ids))
        )
    }
    for item in items:
        total, available = wave_counts.get(item.wave_id, (1, 0))
        total = max(1, total)
        score, band, reason, details = transfer_priority(
            item.restore_expires_at, item.predicted_transfer_seconds, settings, reference_now,
            wave_available_ratio=available / total,
            age_seconds=(reference_now - (item.available_at.replace(tzinfo=timezone.utc) if item.available_at and item.available_at.tzinfo is None else item.available_at or reference_now)).total_seconds(),
            source_business_priority=source_priorities.get(item.source_id, 999),
            size_bytes=int(item.size_bytes or 0),
        )
        if (item.priority_score, item.priority_band, item.decision_reason, item.priority_details_json) != (score, band, reason, json.dumps(details, sort_keys=True)):
            item.priority_score, item.priority_band, item.decision_reason = score, band, reason
            item.priority_details_json = json.dumps(details, sort_keys=True)
            changed += 1
        expiry_at = item.restore_expires_at
        if expiry_at is not None and expiry_at.tzinfo is None:
            expiry_at = expiry_at.replace(tzinfo=timezone.utc)
        if expiry_at and expiry_at <= reference_now and item.state != TransferQueueState.TRANSFERRED:
            item.state, item.decision_reason = TransferQueueState.REAPPROVAL_REQUIRED, "Temporary restored copy expired before transfer"
            changed += 1
    return changed


def continuous_lane_backlog_seconds(session: Session, source_id: int) -> float:
    """Return predicted copy time currently consumable by Raiju for a source.

    This deliberately counts only object-level entries already released by
    availability polling.  It is therefore a local, free measurement of the
    real transfer stock and never triggers an AWS request.
    """
    return float(session.scalar(select(func.coalesce(func.sum(
        TransferQueueItem.predicted_transfer_seconds
    ), 0)).where(
        TransferQueueItem.source_id == source_id,
        TransferQueueItem.state.in_([
            TransferQueueState.AVAILABLE, TransferQueueState.READY,
            TransferQueueState.LEASED, TransferQueueState.MULTIPART_RESUME,
            TransferQueueState.RETRY_WAIT,
        ]),
    )) or 0)


def settings_dict(settings: RuntimeSettings) -> dict:
    return {"max_throughput_mbps": settings.max_throughput_mbps,
            "multipart_part_size_mib": settings.multipart_part_size_mib,
            "default_wave_size_bytes": settings.default_wave_size_bytes, "default_restore_days": settings.default_restore_days,
            "default_restore_tier": settings.default_restore_tier, "task_lease_seconds": settings.task_lease_seconds,
            "preserve_s3_tags": settings.preserve_s3_tags, "cost_estimation_enabled": settings.cost_estimation_enabled,
            "cost_include_aws_transfer_out": settings.cost_include_aws_transfer_out,
            "cost_pricing_auto_refresh_enabled": settings.cost_pricing_auto_refresh_enabled,
            "cost_pricing_refresh_days": settings.cost_pricing_refresh_days,
            "activity_auto_refresh_enabled": settings.activity_auto_refresh_enabled,
            "activity_refresh_seconds": settings.activity_refresh_seconds,
            "dynamic_restore_safety_seconds": settings.dynamic_restore_safety_seconds,
            "dynamic_restore_horizon_waves": settings.dynamic_restore_horizon_waves,
            "dynamic_restore_max_slots": settings.dynamic_restore_max_slots,
            "continuous_transfer_min_buffer_seconds": settings.continuous_transfer_min_buffer_seconds,
            "continuous_transfer_target_buffer_seconds": settings.continuous_transfer_target_buffer_seconds,
            "continuous_transfer_max_buffer_seconds": settings.continuous_transfer_max_buffer_seconds,
            "continuous_transfer_batch_max_objects": settings.continuous_transfer_batch_max_objects,
            "continuous_transfer_batch_max_bytes": settings.continuous_transfer_batch_max_bytes,
            "continuous_transfer_critical_batch_max_objects": settings.continuous_transfer_critical_batch_max_objects,
            "continuous_transfer_critical_batch_max_bytes": settings.continuous_transfer_critical_batch_max_bytes,
            "continuous_transfer_critical_priority": settings.continuous_transfer_critical_priority,
            "updated_at": settings.updated_at}


def safe_aws_error_summary(error: Exception) -> str:
    """Return diagnostic context without exposing credentials or request data."""
    response = getattr(error, "response", {}) or {}
    metadata = response.get("ResponseMetadata", {}) if isinstance(response, dict) else {}
    detail = response.get("Error", {}) if isinstance(response, dict) else {}
    status = metadata.get("HTTPStatusCode")
    code = detail.get("Code")
    context = " ".join(str(value) for value in (status, code) if value)
    return f"{type(error).__name__}{f' ({context})' if context else ''}"


def safe_oci_error_summary(error: Exception) -> str:
    """Return OCI status/code only, never request identifiers or service text."""
    status = getattr(error, "status", None)
    code = getattr(error, "code", None)
    context = " ".join(str(value) for value in (status, code) if value)
    return f"{type(error).__name__}{f' ({context})' if context else ''}"


def read_oci_runtime_config() -> dict:
    with open(oci_runtime_config_file, encoding="utf-8") as config_file:
        return json.load(config_file)


AWS_CONNECTION_SCHEMA_VERSION = 1


def parse_aws_connection_payload(content: str) -> dict:
    """Validate a Secret payload without ever returning its credential values."""
    try:
        payload = json.loads(content)
    except json.JSONDecodeError as error:
        raise ValueError("Secret content is not valid JSON") from error
    if not isinstance(payload, dict) or payload.get("schema_version") != AWS_CONNECTION_SCHEMA_VERSION:
        raise ValueError(f"Expected schema_version {AWS_CONNECTION_SCHEMA_VERSION}")
    required = ("connection_name", "aws_account_id", "default_region", "bootstrap_access_key_id",
                "bootstrap_secret_access_key", "migration_role_arn", "batch_operations_role_arn", "control_bucket")
    missing = [name for name in required if not isinstance(payload.get(name), str) or not payload[name].strip()]
    if missing:
        raise ValueError(f"Missing or empty field(s): {', '.join(missing)}")
    placeholders = [name for name in required if payload[name].strip().startswith("REPLACE_") or "..." in payload[name]]
    if placeholders:
        raise ValueError(f"Placeholder value(s) must be replaced: {', '.join(placeholders)}")
    account_id = payload["aws_account_id"].strip()
    if not re.fullmatch(r"[0-9]{12}", account_id):
        raise ValueError("aws_account_id must contain 12 digits")
    for field in ("migration_role_arn", "batch_operations_role_arn"):
        if not re.fullmatch(rf"arn:(aws|aws-us-gov|aws-cn):iam::{account_id}:role/.+", payload[field].strip()):
            raise ValueError(f"{field} must be a role ARN for aws_account_id")
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", payload["control_bucket"].strip()):
        raise ValueError("control_bucket is not a valid S3 bucket name")
    return {name: value.strip() if isinstance(value, str) else value for name, value in payload.items()}


def aws_secret_payload(secret_ocid: str) -> dict:
    """Read one OCI Secret only in the backend and return validated data."""
    import oci
    signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
    bundle = oci.secrets.SecretsClient({}, signer=signer).get_secret_bundle(secret_ocid).data
    return parse_aws_connection_payload(base64.b64decode(bundle.secret_bundle_content.content).decode("utf-8").strip())


def connection_or_404(session: Session, connection_id: int) -> AwsConnection:
    connection = session.get(AwsConnection, connection_id)
    if not connection:
        raise HTTPException(status_code=404, detail="AWS connection not found")
    return connection


def connection_summary(session: Session, connection: AwsConnection) -> dict:
    sources = session.scalar(select(func.count(Source.id)).where(Source.aws_connection_id == connection.id)) or 0
    return {"id": connection.id, "label": connection.label, "secret_ocid": connection.secret_ocid,
            "aws_account_id": connection.aws_account_id, "default_region": connection.default_region,
            "control_bucket": connection.control_bucket, "archived_at": connection.archived_at,
            "operational_limits": {"discovery_requests_per_second": connection.discovery_requests_per_second,
                                   "restore_poll_requests_per_second": connection.restore_poll_requests_per_second,
                                   "restore_poll_concurrency": connection.restore_poll_concurrency},
            "created_at": connection.created_at, "sources": int(sources)}


PRICING_RATE_FIELDS = (
    "aws_batch_job_usd", "aws_batch_object_usd_per_1000", "aws_s3_put_list_usd_per_1000",
    "aws_s3_get_usd_per_1000", "aws_glacier_bulk_retrieval_usd_per_gib",
    "aws_glacier_standard_retrieval_usd_per_gib", "aws_deep_archive_bulk_retrieval_usd_per_gib",
    "aws_deep_archive_standard_retrieval_usd_per_gib", "aws_transfer_out_usd_per_gib",
    "aws_restore_temp_standard_usd_per_gib_month", "oci_put_usd_per_10000",
    "oci_get_usd_per_10000", "oci_storage_usd_per_gib_month",
)

# The database retains the original calculation units (GiB and, for Batch
# objects, per 1,000) so existing connection overrides remain correct.  The
# operator-facing contract deliberately mirrors the AWS Price List: decimal
# GB and one million Batch objects.  Conversion happens exactly at the API
# boundary and is never hidden inside a displayed price.
DECIMAL_GB_PER_GIB = 1024**3 / 1_000_000_000
PUBLIC_GB_RATE_FIELDS = {
    "aws_glacier_bulk_retrieval_usd_per_gib",
    "aws_glacier_standard_retrieval_usd_per_gib",
    "aws_deep_archive_bulk_retrieval_usd_per_gib",
    "aws_deep_archive_standard_retrieval_usd_per_gib",
    "aws_transfer_out_usd_per_gib",
    "aws_restore_temp_standard_usd_per_gib_month",
}
PUBLIC_BATCH_MILLION_OBJECT_FIELD = "aws_batch_object_usd_per_1000"


def public_rate_value(field: str, value: float | None) -> float | None:
    """Convert an internal calculation rate to its AWS Price List unit."""
    if value is None:
        return None
    if field in PUBLIC_GB_RATE_FIELDS:
        return float(value) / DECIMAL_GB_PER_GIB
    if field == PUBLIC_BATCH_MILLION_OBJECT_FIELD:
        return float(value) * 1000
    return float(value)


def internal_rate_value(field: str, value: float | None) -> float | None:
    """Convert an operator supplied Price List value to calculation units."""
    if value is None:
        return None
    if field in PUBLIC_GB_RATE_FIELDS:
        return float(value) * DECIMAL_GB_PER_GIB
    if field == PUBLIC_BATCH_MILLION_OBJECT_FIELD:
        return float(value) / 1000
    return float(value)


def public_rate_unit(field: str) -> str:
    if field == "aws_batch_job_usd":
        return "USD/job"
    if field == PUBLIC_BATCH_MILLION_OBJECT_FIELD:
        return "USD/1,000,000 objects"
    if field in {"aws_s3_put_list_usd_per_1000", "aws_s3_get_usd_per_1000"}:
        return "USD/1,000 requests"
    if field == "aws_restore_temp_standard_usd_per_gib_month":
        return "USD/GB-month"
    if field in PUBLIC_GB_RATE_FIELDS:
        return "USD/GB"
    return "USD/unit"


def pricing_or_create(session: Session, connection_id: int) -> CostPricing:
    pricing = session.scalar(select(CostPricing).where(CostPricing.aws_connection_id == connection_id))
    if not pricing:
        pricing = CostPricing(aws_connection_id=connection_id)
        session.add(pricing)
        session.flush()
    return pricing


def pricing_dict(pricing: CostPricing) -> dict:
    return {"aws_connection_id": pricing.aws_connection_id, "currency": pricing.currency,
            "reference": pricing.reference, "expected_restore_poll_cycles": pricing.expected_restore_poll_cycles,
            "include_aws_transfer_out": pricing.include_aws_transfer_out,
            "include_oci_costs": pricing.include_oci_costs,
            **{field: public_rate_value(field, getattr(pricing, field)) for field in PRICING_RATE_FIELDS},
            "rate_units": {field: public_rate_unit(field) for field in PRICING_RATE_FIELDS},
            "updated_at": pricing.updated_at}


AWS_PUBLIC_S3_REGION_URL = "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/{region}/index.json"
AWS_PUBLIC_TRANSFER_REGION_URL = "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AWSDataTransfer/current/{region}/index.json"


def first_on_demand_usd(terms: dict) -> float | None:
    def visit(value) -> float | None:
        if not isinstance(value, dict):
            return None
        price = (value.get("pricePerUnit") or {}).get("USD")
        if price is not None:
            try:
                return float(price)
            except (TypeError, ValueError):
                return None
        for child in value.values():
            found = visit(child)
            if found is not None:
                return found
        return None
    found = visit(terms.get("OnDemand") or {})
    if found is not None:
        return found
    return None


def public_s3_rates_from_catalog(catalog: dict) -> dict[str, float]:
    """Map stable S3 Price List attributes to Raijin's narrow cost model.

    AWS changes SKUs over time, so matching intentionally uses multiple public
    attributes rather than any SKU. Unmapped entries remain absent and are
    displayed as not estimated instead of silently using an unrelated rate.
    """
    rates: dict[str, float] = {}
    for product in (catalog.get("products") or {}).values():
        attributes = product.get("attributes") or {}
        blob = " ".join(str(attributes.get(key, "")).lower() for key in (
            "group", "groupDescription", "feeCode", "feeDescription", "usagetype", "operation",
            "storageClass", "volumeType", "productFamily"
        )).replace("-", " ").replace("_", " ").replace("/", " ")
        sku = product.get("sku")
        price = first_on_demand_usd({"OnDemand": (catalog.get("terms") or {}).get("OnDemand", {}).get(sku, {})}) if sku else None
        if price is None:
            continue
        # AWS publishes byte/GB prices in decimal GB. Keep calculation rates
        # normalized to GiB; ``global_pricing_summary`` exposes the original
        # AWS units separately for the operator-facing catalog.
        data_price = price * (1024**3 / 1_000_000_000)
        batch_fee = str(attributes.get("feeDescription") or "").lower()
        usage_type = str(attributes.get("usagetype") or "")
        is_standard_storage = attributes.get("storageClass") == "General Purpose" and (
            usage_type == "TimedStorage-ByteHrs" or re.fullmatch(r"[A-Z]+\d+-TimedStorage-ByteHrs", usage_type) is not None
        )
        if "batch" in blob and "job" in blob:
            rates.setdefault("aws_batch_job_usd", price)
        elif "per object fee for object operations performed by batch operations" in batch_fee:
            rates.setdefault("aws_batch_object_usd_per_1000", price * 1000)
        elif "tier1" in blob or "put/copy/post/list" in blob:
            rates.setdefault("aws_s3_put_list_usd_per_1000", price * 1000)
        elif "tier2" in blob or "get and all other" in blob:
            rates.setdefault("aws_s3_get_usd_per_1000", price * 1000)
        elif "deep" in blob and "retrieval" in blob and "bulk" in blob:
            rates.setdefault("aws_deep_archive_bulk_retrieval_usd_per_gib", data_price)
        elif "deep" in blob and "retrieval" in blob and "standard" in blob:
            rates.setdefault("aws_deep_archive_standard_retrieval_usd_per_gib", data_price)
        elif "glacier" in blob and "retrieval" in blob and "bulk" in blob and "deep" not in blob:
            rates.setdefault("aws_glacier_bulk_retrieval_usd_per_gib", data_price)
        elif "glacier" in blob and "retrieval" in blob and "standard" in blob and "deep" not in blob:
            rates.setdefault("aws_glacier_standard_retrieval_usd_per_gib", data_price)
        elif is_standard_storage:
            rates.setdefault("aws_restore_temp_standard_usd_per_gib_month", data_price)
    return rates


def public_transfer_rates_from_catalog(catalog: dict) -> dict[str, float]:
    """Return the regional AWS-to-external transfer price only.

    S3's own catalog includes MRAP and intra-AWS transfer entries that look
    similar but are not the Internet/OCI egress charge.  AWSDataTransfer has a
    precise regional product: ``AWS Outbound`` to ``External``.
    """
    terms = (catalog.get("terms") or {}).get("OnDemand") or {}
    for product in (catalog.get("products") or {}).values():
        attributes = product.get("attributes") or {}
        if (attributes.get("toLocation") != "External" or attributes.get("transferType") != "AWS Outbound"
                or attributes.get("fromLocation") in {None, "", "Global"}):
            continue
        price = first_on_demand_usd({"OnDemand": terms.get(product.get("sku"), {})})
        if price is not None:
            return {"aws_transfer_out_usd_per_gib": price * (1024**3 / 1_000_000_000)}
    return {}


def refresh_global_aws_pricing(session: Session, aws_region: str) -> GlobalAwsPricing:
    """Fetch the public regional Amazon S3 catalog with bounded network IO."""
    row = session.scalar(select(GlobalAwsPricing).where(GlobalAwsPricing.aws_region == aws_region))
    if not row:
        row = GlobalAwsPricing(aws_region=aws_region)
        session.add(row)
        session.flush()
    s3_url = AWS_PUBLIC_S3_REGION_URL.format(region=aws_region)
    transfer_url = AWS_PUBLIC_TRANSFER_REGION_URL.format(region=aws_region)
    try:
        def read_catalog(url: str) -> dict:
            request = Request(url, headers={"User-Agent": "raijin-data-migration/0.4"})
            with urlopen(request, timeout=60) as response:
                payload = response.read(64 * 1024 * 1024 + 1)
            if len(payload) > 64 * 1024 * 1024:
                raise RuntimeError("Public AWS Price List regional catalog exceeds 64 MiB safety limit")
            return json.loads(payload.decode("utf-8"))

        rates = public_s3_rates_from_catalog(read_catalog(s3_url))
        rates.update(public_transfer_rates_from_catalog(read_catalog(transfer_url)))
        if not rates:
            raise RuntimeError("No supported AWS rates were found in the public regional catalogs")
        row.rates_json, row.source_url, row.fetched_at, row.error = json.dumps(rates, sort_keys=True), f"{s3_url}; {transfer_url}", utcnow(), None
        record_event(session, "GLOBAL_AWS_PRICING_REFRESHED", f"Public AWS S3 pricing refreshed for {aws_region}; {len(rates)} rate(s) mapped")
    except Exception as error:
        row.error = f"{type(error).__name__}: {error}"[:8000]
        record_event(session, "GLOBAL_AWS_PRICING_REFRESH_FAILED", f"Public AWS S3 pricing refresh failed for {aws_region}: {type(error).__name__}")
        raise
    return row


def global_pricing_summary(session: Session, aws_region: str) -> dict:
    row = session.scalar(select(GlobalAwsPricing).where(GlobalAwsPricing.aws_region == aws_region))
    rates = json.loads(row.rates_json or "{}") if row else {}
    return {"aws_region": aws_region, "currency": row.currency if row else "USD", "rates": rates,
            "display_rates": {field: public_rate_value(field, value) for field, value in rates.items()},
            "rate_units": {field: public_rate_unit(field) for field in rates},
            "source_url": row.source_url if row else None, "fetched_at": row.fetched_at if row else None,
            "error": row.error if row else None}


def active_pricing_regions(session: Session) -> list[str]:
    """Include legacy source regions as well as current connection defaults.

    New sources inherit their connection region, but existing migration history
    can legitimately retain an older source region. Its wave must use the same
    public price list as the S3 operations it records.
    """
    connection_regions = session.scalars(select(AwsConnection.default_region).where(AwsConnection.archived_at.is_(None)))
    source_regions = session.scalars(select(Source.aws_region).where(Source.archived_at.is_(None)))
    return sorted({region for region in [*connection_regions, *source_regions] if region})


def refresh_due_global_aws_pricing(session: Session) -> None:
    """Best-effort periodic public catalog refresh; never blocks migration work."""
    settings = runtime_settings(session)
    if not settings.cost_estimation_enabled or not settings.cost_pricing_auto_refresh_enabled:
        return
    cutoff = utcnow() - timedelta(days=settings.cost_pricing_refresh_days)
    regions = active_pricing_regions(session)
    for region in regions:
        cached = session.scalar(select(GlobalAwsPricing).where(GlobalAwsPricing.aws_region == region))
        if cached and cached.fetched_at and cached.fetched_at >= cutoff:
            continue
        try:
            refresh_global_aws_pricing(session, region)
            session.commit()
        except Exception:
            session.commit()


def wave_cost_estimate(session: Session, wave: Wave) -> dict:
    """Build a transparent, deliberately conservative cost estimate.

    It is an estimate of billable units generated by Raijin, never a promise of
    the customer's AWS/OCI invoice. Rates remain blank until the customer
    supplies their regional/contractual prices for the connection.
    """
    source = wave.source
    # Simulated sources intentionally have no Secret-backed AWS connection.
    # They still use the same public catalog and billable-unit model, but do
    # not offer per-account contractual overrides or OCI price overrides.
    pricing = pricing_or_create(session, source.aws_connection_id) if source.aws_connection_id else None
    public = global_pricing_summary(session, source.aws_region)
    public_rates = public["rates"]
    settings = runtime_settings(session)
    objects = list(session.scalars(select(ObjectRecord).where(ObjectRecord.wave_id == wave.id)))
    source_objects = session.scalar(select(func.count(ObjectRecord.id)).where(ObjectRecord.source_id == source.id)) or 0
    total_bytes = sum(int(obj.size_bytes or 0) for obj in objects)
    total_gib = total_bytes / 1024**3
    archived = [obj for obj in objects if (obj.storage_class or "").upper() in ARCHIVE_STORAGE_CLASSES]
    glacier_gib = sum(obj.size_bytes for obj in archived if (obj.storage_class or "").upper() == "GLACIER") / 1024**3
    deep_gib = sum(obj.size_bytes for obj in archived if (obj.storage_class or "").upper() == "DEEP_ARCHIVE") / 1024**3
    unpriced_archive_gib = sum(obj.size_bytes for obj in archived if (obj.storage_class or "").upper() not in {"GLACIER", "DEEP_ARCHIVE"}) / 1024**3
    part_bytes = max(16 * 1024**2, int(settings.multipart_part_size_mib) * 1024**2)
    oci_put_operations = 0
    for obj in objects:
        effective_part = max(part_bytes, math.ceil(int(obj.size_bytes or 0) / 10000))
        oci_put_operations += 1 if obj.size_bytes <= 16 * 1024**2 else math.ceil(obj.size_bytes / effective_part) + 2
    wave_pages = math.ceil(len(objects) / 1000) if objects else 0
    components: list[dict] = []
    missing: list[str] = []
    missing_by_category: dict[str, list[str]] = {"one_time": [], "recurring": [], "optional": []}

    def add(key: str, label: str, quantity: float, unit: str, rate_field: str, divisor: float = 1, category: str = "one_time") -> None:
        if not quantity:
            return
        custom_rate = getattr(pricing, rate_field) if pricing is not None else None
        rate = custom_rate if custom_rate is not None else public_rates.get(rate_field)
        display_quantity = quantity * DECIMAL_GB_PER_GIB if rate_field in PUBLIC_GB_RATE_FIELDS else quantity
        display_unit = unit.replace("GiB", "GB") if rate_field in PUBLIC_GB_RATE_FIELDS else unit
        entry = {"key": key, "label": label, "quantity": quantity, "unit": unit,
                 "display_quantity": display_quantity, "display_unit": display_unit,
                 "rate_field": rate_field, "rate": rate, "category": category,
                 "rate_display": public_rate_value(rate_field, rate), "rate_unit": public_rate_unit(rate_field),
                 "rate_source": "connection" if custom_rate is not None else ("aws_public" if rate is not None else "missing")}
        if rate is None:
            entry["cost"] = None
            missing.append(rate_field)
            missing_by_category[category].append(rate_field)
        else:
            entry["cost"] = round((quantity / divisor) * float(rate), 6)
        components.append(entry)

    if archived:
        add("batch_job", "S3 Batch Operations job", 1, "job", "aws_batch_job_usd")
        add("batch_objects", "S3 Batch Operations object tasks", len(archived), "objects", "aws_batch_object_usd_per_1000", 1000)
        add("manifest_put", "S3 manifest write", 1, "requests", "aws_s3_put_list_usd_per_1000", 1000)
        poll_cycles = pricing.expected_restore_poll_cycles if pricing is not None else 24
        add("restore_polling", "S3 restore availability checks (HeadObject)", len(archived) * poll_cycles, "requests", "aws_s3_get_usd_per_1000", 1000)
        add("restore_temp", "Temporary S3 Standard restored copy", (glacier_gib + deep_gib) * wave.restore_days / 30, "GiB-month", "aws_restore_temp_standard_usd_per_gib_month")
        retrieval_field = "aws_glacier_bulk_retrieval_usd_per_gib" if wave.restore_tier == "BULK" else "aws_glacier_standard_retrieval_usd_per_gib"
        add("glacier_retrieval", f"S3 Glacier {wave.restore_tier} retrieval", glacier_gib, "GiB", retrieval_field)
        retrieval_field = "aws_deep_archive_bulk_retrieval_usd_per_gib" if wave.restore_tier == "BULK" else "aws_deep_archive_standard_retrieval_usd_per_gib"
        add("deep_archive_retrieval", f"S3 Deep Archive {wave.restore_tier} retrieval", deep_gib, "GiB", retrieval_field)
    add("discovery", "Allocated S3 discovery ListObjectsV2 pages", wave_pages, "requests", "aws_s3_put_list_usd_per_1000", 1000)
    add("source_reads", "S3 object reads during transfer", len(objects), "requests", "aws_s3_get_usd_per_1000", 1000)
    if settings.preserve_s3_tags:
        add("tag_reads", "S3 object-tag reads", len(objects), "requests", "aws_s3_get_usd_per_1000", 1000)
    connection_allows_aws_transfer_out = pricing.include_aws_transfer_out if pricing is not None else True
    include_aws_transfer_out = settings.cost_include_aws_transfer_out and connection_allows_aws_transfer_out
    include_oci_costs = pricing.include_oci_costs if pricing is not None else False
    if include_aws_transfer_out:
        add("aws_transfer_out", "AWS data transfer out to OCI", total_gib, "GiB", "aws_transfer_out_usd_per_gib")
    if include_oci_costs:
        add("oci_writes", "OCI Object Storage write operations", oci_put_operations, "operations", "oci_put_usd_per_10000", 10000)
        add("oci_storage", "OCI destination storage (one month)", total_gib, "GiB-month", "oci_storage_usd_per_gib_month", category="recurring")
        add("deep_audit", "Optional deep SHA-256 audit OCI reads", len(objects), "operations", "oci_get_usd_per_10000", 10000, category="optional")
    one_time = [item["cost"] for item in components if item["category"] == "one_time"]
    recurring = [item["cost"] for item in components if item["category"] == "recurring"]
    optional = [item["cost"] for item in components if item["category"] == "optional"]
    def estimated(values: list[float | None]) -> float:
        return round(sum(value for value in values if value is not None), 6)

    # Keep the requested-retention estimate intact, but expose a separate
    # observed figure once the copy has actually run.  S3 bills the temporary
    # Standard copy for the time it remains restored; using transfer timestamps
    # here makes the operational report auditable without pretending it is an
    # invoice (AWS billing granularity and account agreements still apply).
    observed_temp_gib_months = 0.0
    observed_temp_objects = 0
    observed_temp_in_progress = 0
    now = utcnow()
    for obj in archived:
        restored_at = obj.restored_at
        if restored_at is None:
            continue
        if restored_at.tzinfo is None:
            restored_at = restored_at.replace(tzinfo=timezone.utc)
        end_at = obj.transferred_at or obj.restore_expires_at or now
        if end_at.tzinfo is None:
            end_at = end_at.replace(tzinfo=timezone.utc)
        if obj.transferred_at is None and obj.restore_expires_at is None:
            observed_temp_in_progress += 1
        observed_temp_objects += 1
        elapsed_seconds = max(0.0, (end_at - restored_at).total_seconds())
        observed_temp_gib_months += (int(obj.size_bytes or 0) / 1024**3) * elapsed_seconds / (30 * 24 * 3600)
    observed_temp_rate = (
        getattr(pricing, "aws_restore_temp_standard_usd_per_gib_month")
        if pricing is not None else None
    )
    if observed_temp_rate is None:
        observed_temp_rate = public_rates.get("aws_restore_temp_standard_usd_per_gib_month")
    observed_temp_cost = (
        round(observed_temp_gib_months * float(observed_temp_rate), 6)
        if observed_temp_rate is not None else None
    )
    return {"wave_id": wave.id, "connection_id": source.aws_connection_id,
            "connection_label": source.aws_connection.label if source.aws_connection else "AWS public Price List (simulation)",
            "currency": pricing.currency.upper() if pricing is not None else public["currency"].upper(),
            "pricing_reference": pricing.reference if pricing is not None else "Public AWS Price List; no contractual overrides in Simulation mode",
            "pricing_updated_at": pricing.updated_at if pricing is not None else public.get("fetched_at"),
            "global_pricing": public,
            "complete": not missing_by_category["one_time"] and not unpriced_archive_gib,
            "missing_rates": sorted(set(missing)), "unpriced_archive_gib": round(unpriced_archive_gib, 6),
            "totals": {"one_time": estimated(one_time), "recurring_monthly": estimated(recurring), "optional_deep_audit": estimated(optional)},
            "total_completeness": {"one_time": not missing_by_category["one_time"] and not unpriced_archive_gib,
                                   "recurring_monthly": not missing_by_category["recurring"],
                                   "optional_deep_audit": not missing_by_category["optional"]},
            "quantities": {"objects": len(objects), "archive_objects": len(archived), "bytes": total_bytes,
                           "source_inventory_objects": int(source_objects), "multipart_part_mib": settings.multipart_part_size_mib,
                           "estimated_poll_cycles": pricing.expected_restore_poll_cycles if pricing is not None else 24,
                           "oci_write_operations": oci_put_operations},
            "observed_temporary_restore": {
                "objects": observed_temp_objects,
                "in_progress_objects": observed_temp_in_progress,
                "gib_months": round(observed_temp_gib_months, 9),
                "rate": observed_temp_rate,
                "cost": observed_temp_cost,
                "complete": observed_temp_objects == len(archived) and not observed_temp_in_progress,
                "note": "Valor observado até a transferência, expiração conhecida ou o instante atual; não substitui a estimativa de retenção solicitada.",
            },
            "components": components}


def aws_connection_configuration(connection: AwsConnection, secret: dict) -> dict:
    """Non-sensitive Secret fields safe to display to an authenticated local operator."""
    return {
        "id": connection.id,
        "label": connection.label,
        "secret_ocid": connection.secret_ocid,
        "schema_version": secret["schema_version"],
        "connection_name": secret["connection_name"],
        "aws_account_id": secret["aws_account_id"],
        "default_region": secret["default_region"],
        "migration_role_arn": secret["migration_role_arn"],
        "batch_operations_role_arn": secret["batch_operations_role_arn"],
        "control_bucket": secret["control_bucket"],
        "redacted_fields": ["bootstrap_access_key_id", "bootstrap_secret_access_key"],
    }


@app.get("/", response_class=HTMLResponse)
def dashboard() -> str:
    with open("app/static/index.html", encoding="utf-8") as page:
        return (page.read()
                .replace("{{RAIJIN_VERSION}}", RAIJIN_SERVICE_VERSION)
                .replace("{{FUJIN_VERSION}}", SIMULATOR_SERVICE_VERSION))


@app.get("/simulation", response_class=HTMLResponse)
def simulation_console() -> str:
    if runtime_context.is_real:
        raise HTTPException(status_code=404, detail="Simulation console is available only in Simulation mode")
    with open("app/static/simulation.html", encoding="utf-8") as page:
        return page.read()


@app.get("/healthz")
def healthcheck(session: Session = Depends(get_session)) -> dict:
    session.execute(select(1))
    readiness = cloud_backend.readiness(require_operations=False)
    return {"status": "ok", "operation_mode": runtime_context.mode.value,
            "backend_operations_enabled": readiness.operations_enabled,
            "backend_contract_version": readiness.contract_version}


@app.get("/api/runtime")
def runtime_identity() -> dict:
    readiness = cloud_backend.readiness(require_operations=False)
    return {"operation_mode": runtime_context.mode.value,
            "raijin_version": RAIJIN_SERVICE_VERSION,
            "fujin_version": SIMULATOR_SERVICE_VERSION,
            "backend_operations_enabled": readiness.operations_enabled,
            "backend_contract_version": readiness.contract_version,
            "backend_capabilities": list(readiness.capabilities)}


@app.get("/api/runtime/clock")
def runtime_clock(source_id: int | None = Query(default=None, ge=1),
                  session: Session = Depends(get_session)) -> dict:
    """Expose the real system clock and the active source's simulation clock."""
    source = session.get(Source, source_id) if source_id else None
    if source_id and source is None:
        raise HTTPException(status_code=404, detail="Source not found")
    if runtime_context.is_simulation and source is None:
        source = session.scalar(
            select(Source).join(DynamicPipelineRun, DynamicPipelineRun.source_id == Source.id).where(
                Source.archived_at.is_(None),
                DynamicPipelineRun.status.not_in(["COMPLETED", "HISTORICAL"]),
            ).order_by(DynamicPipelineRun.created_at.desc(), DynamicPipelineRun.id.desc()).limit(1)
        )
    # The header clock is informational. Under an intentionally busy
    # simulation backend it must never turn a transient simulator timeout into
    # a failed page heartbeat or block the rest of the console from loading.
    clock = None
    clock_available = True
    if source:
        try:
            clock = source_scheduler_clock(source)
        except (OSError, TimeoutError, ValueError):
            clock_available = False
    # A simulated source uses a deliberately discrete clock.  Expose the
    # active durable phase so the UI can distinguish a harmless polling hold
    # from a transfer hold or a genuinely idle/paused execution.
    active_task_kind = None
    if source and runtime_context.is_simulation:
        active_task_kind = session.scalar(
            select(Task.kind)
            .join(Wave, Wave.id == Task.wave_id)
            .where(Wave.source_id == source.id, Task.state == TaskState.RUNNING)
            .order_by(Task.created_at.desc(), Task.id.desc())
            .limit(1)
        )
    system_now = clock.real_now if clock else utcnow()
    return {
        "operation_mode": runtime_context.mode.value,
        "source_id": source.id if source else None,
        "source_name": source.name if source else None,
        "execution_id": source.simulation_execution_id if source else None,
        "system_now": system_now,
        "virtual_now": clock.effective_now if runtime_context.is_simulation and source else None,
        "acceleration": clock.acceleration if clock else 1.0,
        "paused": clock.paused if clock else False,
        # A held clock is deliberately different from a manually paused one:
        # it protects a source's restore-retention window while the real
        # worker streams or polls objects.  The browser must not extrapolate
        # it at the configured acceleration rate.
        "held": clock.held if clock else False,
        "activity": active_task_kind,
        "clock_available": clock_available,
    }


@app.post("/api/runtime/mode", status_code=202)
def request_operation_mode_switch(
    payload: OperationModeSwitchRequest, session: Session = Depends(get_session)
) -> dict:
    """Request a host-mediated, fail-closed runtime switch.

    The container receives no Podman or systemd privilege. It can only write a
    target token to the dedicated request mount; the host-side command repeats
    the durable queue audit before replacing any process.
    """
    if not payload.confirmed:
        raise HTTPException(status_code=422, detail="Explicit mode-switch confirmation is required")
    if payload.target_mode == runtime_context.mode.value:
        raise HTTPException(status_code=409, detail=f"RAIJIN is already in {payload.target_mode} mode")
    active_tasks = int(session.scalar(select(func.count(Task.id)).where(
        Task.state.in_([TaskState.READY, TaskState.RUNNING])
    )) or 0)
    active_discoveries = int(session.scalar(select(func.count(DiscoveryJob.id)).where(
        DiscoveryJob.state.in_([TaskState.READY, TaskState.RUNNING])
    )) or 0)
    if active_tasks or active_discoveries:
        raise HTTPException(
            status_code=409,
            detail=f"Mode switch blocked by {active_tasks} task(s) and {active_discoveries} discovery job(s)",
        )
    request_file = os.environ.get("RAIJIN_MODE_REQUEST_FILE", "/run/mode-control/request")
    try:
        temporary = f"{request_file}.{os.getpid()}.tmp"
        with open(temporary, "w", encoding="ascii") as target:
            target.write(payload.target_mode + "\n")
        os.replace(temporary, request_file)
    except OSError as error:
        raise HTTPException(status_code=503, detail="Host mode-control channel is unavailable") from error
    return {"accepted": True, "current_mode": runtime_context.mode.value,
            "target_mode": payload.target_mode, "state": "DRAINING"}


def require_simulation_mode() -> None:
    if runtime_context.is_real or not runtime_context.simulator_base_url:
        raise HTTPException(status_code=409, detail="This operation is available only in Simulation mode")


@app.get("/api/simulation/scenarios")
def simulation_scenarios() -> list[dict]:
    require_simulation_mode()
    cloud_backend.readiness(require_operations=True)
    return SimulatorAdminClient(runtime_context.simulator_base_url).list_scenarios()


@app.get("/api/simulation/templates")
def simulation_templates() -> list[dict]:
    require_simulation_mode()
    cloud_backend.readiness(require_operations=True)
    return SimulatorAdminClient(runtime_context.simulator_base_url).list_templates()


@app.post("/api/simulation/templates", status_code=201)
def create_simulation_template(payload: SimulationTemplateWrite) -> dict:
    require_simulation_mode()
    cloud_backend.readiness(require_operations=True)
    return SimulatorAdminClient(runtime_context.simulator_base_url).create_template(
        payload.model_dump()
    )


@app.put("/api/simulation/templates/{template_id}")
def update_simulation_template(template_id: str, payload: SimulationTemplateWrite) -> dict:
    require_simulation_mode()
    cloud_backend.readiness(require_operations=True)
    return SimulatorAdminClient(runtime_context.simulator_base_url).update_template(
        template_id, payload.model_dump()
    )


@app.post("/api/simulation/scenarios", status_code=201)
def create_simulation_scenario(
    payload: SimulationScenarioBootstrap, session: Session = Depends(get_session)
) -> dict:
    """Create one immutable simulator execution and its isolated Raijin source."""
    require_simulation_mode()
    cloud_backend.readiness(require_operations=True)
    if session.scalar(select(Source.id).where(Source.name == payload.name)):
        raise HTTPException(status_code=409, detail="Simulation source name already exists")
    prefixes = normalize_source_prefixes(payload.prefixes)
    admin = SimulatorAdminClient(runtime_context.simulator_base_url, timeout_seconds=600)
    try:
        scenario = admin.create_scenario(
            {
                "name": payload.name,
                "fidelity": payload.fidelity,
                "seed": payload.seed,
                "logical_size_bytes": payload.logical_size_bytes,
                "physical_budget_bytes": payload.physical_budget_bytes,
                "clock_acceleration": payload.clock_acceleration,
                "retention_days": payload.retention_days,
                "quarantine_days": payload.quarantine_days,
                "configuration": payload.configuration,
                "fault_rules": payload.fault_rules,
                "template_id": payload.template_id,
            }
        )
    except SimulatorAdminError as error:
        status_code = error.status_code if error.status_code and 400 <= error.status_code < 500 else 502
        raise HTTPException(status_code=status_code, detail=error.detail) from error
    catalog = admin.materialize(
        scenario["id"],
        {
            "source_bucket": payload.source_bucket,
            "destination_bucket": payload.destination_bucket,
            "region": payload.region,
            "object_count": payload.object_count,
            "logical_size_bytes": payload.logical_size_bytes,
            "prefixes": prefixes,
            "storage_class": payload.storage_class,
        },
    )
    execution = admin.create_execution(scenario["id"])
    # A source gets its own clock, but it does not start consuming virtual
    # retention simply because it was created. The worker resumes it only
    # when durable discovery or migration work is queued for this source.
    admin.control_clock(execution["id"], "PAUSE")
    source = Source(
        name=payload.name,
        s3_bucket=payload.source_bucket,
        s3_prefix=prefixes[0],
        aws_region=payload.region,
        aws_bucket_region=payload.region,
        aws_connection_id=None,
        destination_bucket=payload.destination_bucket,
        backend_kind="SIMULATED",
        simulation_scenario_id=scenario["id"],
        simulation_execution_id=execution["id"],
        simulation_correlation_id=execution["correlation_id"],
        simulation_tenant_id=str(uuid.uuid4()),
        simulation_project_id=str(uuid.uuid4()),
        simulation_fidelity=scenario["fidelity"],
    )
    session.add(source)
    session.flush()
    session.add_all(SourcePrefix(source_id=source.id, prefix=prefix) for prefix in prefixes)
    record_event(
        session,
        "SIMULATED_SCENARIO_CREATED",
        f"SIMULATED {payload.fidelity} scenario '{payload.name}' created with seed {payload.seed}",
        source_id=source.id,
    )
    session.commit()
    return {
        "source_id": source.id,
        "scenario": scenario,
        "execution": execution,
        "catalog": catalog,
    }


def simulation_source_or_404(session: Session, source_id: int) -> Source:
    source = source_or_404(session, source_id)
    if source.backend_kind != "SIMULATED":
        raise HTTPException(status_code=409, detail="Source does not belong to Simulation mode")
    return source


@app.get("/api/simulation/sources")
def simulation_sources(session: Session = Depends(get_session)) -> list[dict]:
    require_simulation_mode()
    rows = list(session.scalars(select(Source).where(
        Source.backend_kind == "SIMULATED"
    ).order_by(Source.archived_at.is_not(None), Source.id.desc())))
    result = []
    admin = SimulatorAdminClient(runtime_context.simulator_base_url)
    scenarios = {item["id"]: item for item in admin.list_scenarios()}
    for source in rows:
        objects, size = session.execute(select(
            func.count(ObjectRecord.id), func.coalesce(func.sum(ObjectRecord.size_bytes), 0)
        ).where(ObjectRecord.source_id == source.id)).one()
        waves = int(session.scalar(select(func.count(Wave.id)).where(Wave.source_id == source.id)) or 0)
        pending = int(session.scalar(select(func.count(Task.id)).join(Wave).where(
            Wave.source_id == source.id, Task.state.in_([TaskState.READY, TaskState.RUNNING])
        )) or 0)
        clock = admin.clock_status(source.simulation_execution_id) if source.simulation_execution_id else None
        execution = admin.get_execution(source.simulation_execution_id) if source.simulation_execution_id else None
        scenario = scenarios.get(source.simulation_scenario_id, {})
        result.append({
            "id": source.id, "name": source.name, "status": source.status,
            "fidelity": source.simulation_fidelity, "source_bucket": source.s3_bucket,
            "destination_bucket": source.destination_bucket, "region": source.aws_region,
            "objects": int(objects), "bytes": int(size), "waves": waves,
            "pending_tasks": pending, "scenario_id": source.simulation_scenario_id,
            "execution_id": source.simulation_execution_id, "clock": clock,
            "execution_state": execution.get("state") if execution else None,
            "physical_budget_bytes": execution.get("physical_budget_bytes") if execution else 0,
            "physical_bytes_processed": execution.get("physical_bytes_processed") if execution else 0,
            "scenario_state": scenario.get("state"),
            "retention_days": scenario.get("retention_days"),
            "quarantine_days": scenario.get("quarantine_days"),
            "archived_at": source.archived_at,
        })
    return result


@app.post("/api/simulation/housekeeping")
def simulation_housekeeping() -> dict:
    require_simulation_mode()
    return SimulatorAdminClient(runtime_context.simulator_base_url).apply_housekeeping()


@app.post("/api/simulation/scenarios/{scenario_id}/restore")
def simulation_restore_scenario(scenario_id: str) -> dict:
    require_simulation_mode()
    return SimulatorAdminClient(runtime_context.simulator_base_url).restore_scenario(scenario_id)


@app.post("/api/simulation/sources/{source_id}/purge")
def simulation_purge_source(
    source_id: int,
    payload: SimulationScenarioPurge,
    session: Session = Depends(get_session),
) -> dict:
    """Cross-database guard before irreversible simulator catalog deletion."""
    require_simulation_mode()
    source = simulation_source_or_404(session, source_id)
    if payload.confirmation != "PURGE":
        raise HTTPException(status_code=422, detail="Type PURGE to confirm physical deletion")
    if source.archived_at is None:
        raise HTTPException(status_code=409, detail="Archive the simulated source before purging its catalog")
    active_tasks = int(session.scalar(select(func.count(Task.id)).join(Wave).where(
        Wave.source_id == source.id,
        Task.state.in_([TaskState.READY, TaskState.RUNNING]),
    )) or 0)
    active_discoveries = int(session.scalar(select(func.count(DiscoveryJob.id)).where(
        DiscoveryJob.source_id == source.id,
        DiscoveryJob.state.in_([TaskState.READY, TaskState.RUNNING]),
    )) or 0)
    if active_tasks or active_discoveries:
        raise HTTPException(
            status_code=409,
            detail=f"Purge blocked by {active_tasks} task(s) and {active_discoveries} discovery job(s)",
        )
    admin = SimulatorAdminClient(runtime_context.simulator_base_url)
    execution = admin.get_execution(source.simulation_execution_id)
    if execution.get("state") not in {"SUCCEEDED", "FAILED", "CANCELLED"}:
        raise HTTPException(status_code=409, detail="Simulation execution must be terminal before purge")
    result = admin.purge_scenario(source.simulation_scenario_id, payload.reason)
    record_event(
        session,
        "SIMULATION_SCENARIO_PURGED",
        f"SIMULATED catalog physically purged; tombstone {result['evidence_sha256']}",
        source_id=source.id,
    )
    session.commit()
    return result


@app.post("/api/simulation/sources/{source_id}/archive")
def simulation_archive_source(source_id: int, session: Session = Depends(get_session)) -> dict:
    require_simulation_mode()
    source = simulation_source_or_404(session, source_id)
    if source.archived_at is not None:
        return {"id": source.id, "status": source.status, "archived_at": source.archived_at}
    active_tasks = int(session.scalar(select(func.count(Task.id)).join(Wave).where(
        Wave.source_id == source.id,
        Task.state.in_([TaskState.READY, TaskState.RUNNING]),
    )) or 0)
    active_discoveries = int(session.scalar(select(func.count(DiscoveryJob.id)).where(
        DiscoveryJob.source_id == source.id,
        DiscoveryJob.state.in_([TaskState.READY, TaskState.RUNNING]),
    )) or 0)
    if active_tasks or active_discoveries:
        raise HTTPException(status_code=409, detail="Simulation source still has active work")
    execution = SimulatorAdminClient(runtime_context.simulator_base_url).get_execution(
        source.simulation_execution_id
    )
    if execution.get("state") not in {"SUCCEEDED", "FAILED", "CANCELLED"}:
        raise HTTPException(status_code=409, detail="Simulation execution must be terminal before archive")
    source.archived_at, source.status = utcnow(), "ARCHIVED"
    record_event(
        session,
        "SIMULATION_SOURCE_ARCHIVED",
        f"SIMULATED source '{source.name}' archived; simulator evidence retained",
        source_id=source.id,
    )
    session.commit()
    return {"id": source.id, "status": source.status, "archived_at": source.archived_at}


@app.post("/api/simulation/sources/{source_id}/clone", status_code=201)
def clone_simulation_source(
    source_id: int,
    payload: SimulationExecutionClone,
    session: Session = Depends(get_session),
) -> dict:
    require_simulation_mode()
    original = simulation_source_or_404(session, source_id)
    if session.scalar(select(Source.id).where(Source.name == payload.name)):
        raise HTTPException(status_code=409, detail="Simulation source name already exists")
    result = SimulatorAdminClient(runtime_context.simulator_base_url, timeout_seconds=600).clone_execution(
        original.simulation_execution_id,
        payload.name,
        payload.fidelity,
        payload.physical_budget_bytes,
    )
    scenario, execution, catalog = result["scenario"], result["execution"], result["catalog"]
    SimulatorAdminClient(runtime_context.simulator_base_url).control_clock(execution["id"], "PAUSE")
    prefixes = source_prefix_values(original)
    source = Source(
        name=payload.name,
        s3_bucket=catalog["source_bucket"],
        s3_prefix=prefixes[0],
        aws_region=original.aws_region,
        aws_bucket_region=original.aws_bucket_region,
        destination_bucket=catalog["destination_bucket"],
        backend_kind="SIMULATED",
        simulation_scenario_id=scenario["id"],
        simulation_execution_id=execution["id"],
        simulation_correlation_id=execution["correlation_id"],
        simulation_tenant_id=str(uuid.uuid4()),
        simulation_project_id=str(uuid.uuid4()),
        simulation_fidelity=scenario["fidelity"],
    )
    session.add(source)
    session.flush()
    session.add_all(SourcePrefix(source_id=source.id, prefix=prefix) for prefix in prefixes)
    record_event(
        session,
        "SIMULATED_EXECUTION_CLONED",
        f"SIMULATED execution cloned from source {original.id} with immutable snapshot and seed",
        source_id=source.id,
    )
    session.commit()
    return {"source_id": source.id, **result}


@app.post("/api/simulation/sources/{source_id}/discovery", status_code=202)
def simulation_discovery(source_id: int, session: Session = Depends(get_session)) -> dict:
    require_simulation_mode()
    source = simulation_source_or_404(session, source_id)
    job = queue_discovery(source, session)
    return {"source_id": source.id, "job_id": job.id, "status": source.status}


@app.get("/api/simulation/sources/{source_id}/waves")
def simulation_waves(source_id: int, session: Session = Depends(get_session)) -> list[dict]:
    require_simulation_mode()
    simulation_source_or_404(session, source_id)
    return list_waves(source_id, session)


@app.get("/api/simulation/sources/{source_id}/flight-board")
def simulation_flight_board(source_id: int, session: Session = Depends(get_session)) -> dict:
    """Expose persisted planned/observed phases and scheduler decisions."""
    require_simulation_mode()
    simulation_source_or_404(session, source_id)
    result = flight_board(source_id=source_id, run_id=None, session=session)
    result["scheduler_decisions"] = [
        {
            "at": item.created_at,
            "kind": item.kind,
            "wave_id": item.wave_id,
            "message": item.message,
        }
        for item in session.scalars(select(Event).where(
            Event.source_id == source_id,
            Event.kind.in_([
                "DYNAMIC_WAVE_REPLANNED",
                "DYNAMIC_PIPELINE_REPLANNED",
                "DYNAMIC_RESTORE_RELEASED",
                "DYNAMIC_RESTORE_SCHEDULED",
            ]),
        ).order_by(Event.created_at, Event.id))
    ]
    return result


@app.post("/api/simulation/sources/{source_id}/waves/dynamic", status_code=201)
def simulation_create_dynamic_waves(
    source_id: int, payload: DynamicWaveCreate, session: Session = Depends(get_session)
) -> dict:
    require_simulation_mode()
    simulation_source_or_404(session, source_id)
    return create_dynamic_waves(source_id, payload, session)


@app.post("/api/simulation/sources/{source_id}/waves/queue-all")
def simulation_queue_all(source_id: int, session: Session = Depends(get_session)) -> dict:
    require_simulation_mode()
    simulation_source_or_404(session, source_id)
    return queue_all_planned_waves(source_id, session)


@app.get("/api/simulation/executions/{execution_id}/clock")
def simulation_clock(execution_id: str) -> dict:
    require_simulation_mode()
    return SimulatorAdminClient(runtime_context.simulator_base_url).clock_status(execution_id)


@app.get("/api/simulation/executions/{execution_id}/report")
def simulation_execution_report(execution_id: str) -> dict:
    require_simulation_mode()
    return SimulatorAdminClient(runtime_context.simulator_base_url).execution_report(execution_id)


@app.post("/api/simulation/executions/{execution_id}/clock")
def simulation_control_clock(execution_id: str, payload: dict) -> dict:
    require_simulation_mode()
    action = str(payload.get("action") or "").upper()
    if action not in {"PAUSE", "RESUME", "ADVANCE"}:
        raise HTTPException(status_code=422, detail="Clock action must be PAUSE, RESUME, or ADVANCE")
    try:
        advance_seconds = float(payload.get("advance_seconds") or 0)
    except (TypeError, ValueError) as error:
        raise HTTPException(status_code=422, detail="advance_seconds must be numeric") from error
    return SimulatorAdminClient(runtime_context.simulator_base_url).control_clock(
        execution_id, action, advance_seconds
    )


@app.get("/api/platform/status")
def platform_status() -> dict:
    """Read host service state from an unprivileged, read-only status file."""
    try:
        with open(platform_status_file, encoding="utf-8") as status_file:
            return json.load(status_file)
    except (FileNotFoundError, json.JSONDecodeError, OSError) as error:
        return {
            "generated_at": None,
            "available": False,
            "message": f"Host status unavailable: {error}",
            "services": {},
            "last_postgres_backup": None,
        }


@app.get("/api/observability")
def observability(session: Session = Depends(get_session)) -> dict:
    """Local, non-sensitive operational signals for operators and monitors."""
    now = utcnow()
    stale_cutoff = now - timedelta(minutes=10)
    active_task = select(func.count(Task.id)).join(Wave, Task.wave_id == Wave.id).join(Source, Wave.source_id == Source.id).where(Source.archived_at.is_(None))
    failed_tasks = session.scalar(active_task.where(Task.state == TaskState.FAILED)) or 0
    retrying_tasks = session.scalar(active_task.where(Task.state == TaskState.READY, Task.attempts > 1)) or 0
    stale_leases = session.scalar(active_task.where(Task.state == TaskState.RUNNING, Task.lease_expires_at < now)) or 0
    recent_failures = session.scalar(select(func.count(Event.id)).where(Event.kind.like("%FAILED%"), Event.created_at >= now - timedelta(hours=24))) or 0
    active_object = select(func.count(ObjectRecord.id)).join(Source, ObjectRecord.source_id == Source.id).where(Source.archived_at.is_(None))
    active_multipart = session.scalar(active_object.where(ObjectRecord.multipart_upload_id.is_not(None))) or 0
    stalled_transfers = session.scalar(active_object.where(
        ObjectRecord.state == ObjectState.TRANSFERRING,
        ObjectRecord.transfer_progress_at.is_not(None), ObjectRecord.transfer_progress_at < stale_cutoff,
    )) or 0
    expiry_risks = []
    expiry_rows = session.execute(
        select(Wave, Source.name, func.min(ObjectRecord.restore_expires_at),
               func.count(ObjectRecord.id),
               func.coalesce(func.sum(case((ObjectRecord.state.in_([ObjectState.TRANSFERRED, ObjectState.VERIFIED]), 1), else_=0)), 0))
        .join(Source, Source.id == Wave.source_id).join(ObjectRecord, ObjectRecord.wave_id == Wave.id)
        .where(ObjectRecord.restore_expires_at.is_not(None), Source.archived_at.is_(None))
        .group_by(Wave.id, Source.name)
    )
    for wave, source_name, expiry, total, done in expiry_rows:
        if not expiry or int(done or 0) >= int(total or 0):
            continue
        remaining_ratio = max(0.0, 1 - (int(done or 0) / max(1, int(total or 0))))
        predicted_remaining = max(300, int(float(wave.predicted_transfer_seconds or 0) * remaining_ratio))
        safe_finish_at = now + timedelta(seconds=predicted_remaining + 3600)
        if expiry <= safe_finish_at:
            expiry_risks.append({"wave_id": wave.id, "wave_name": wave.name, "source_name": source_name,
                                 "expires_at": expiry, "predicted_remaining_seconds": predicted_remaining,
                                 "severity": "EXPIRED" if expiry <= now else "AT_RISK"})
    volume = shutil.disk_usage("/")
    return {
        "generated_at": now, "tasks": {"failed": int(failed_tasks), "retrying": int(retrying_tasks), "stale_leases": int(stale_leases)},
        "transfers": {"active_multipart_checkpoints": int(active_multipart), "stalled": int(stalled_transfers)},
        "events": {"failures_last_24h": int(recent_failures)},
        "restore_expiry": {"at_risk": len(expiry_risks), "waves": expiry_risks[:20]},
        "disk": {"free_bytes": volume.free, "used_percent": round(volume.used * 100 / volume.total, 2)},
    }


@app.get("/metrics", include_in_schema=False)
def prometheus_metrics(session: Session = Depends(get_session)) -> Response:
    """Prometheus text exposition, loopback-only with the rest of the API."""
    data = observability(session)
    lines = [
        "# HELP raijin_failed_tasks Number of failed durable tasks.",
        "# TYPE raijin_failed_tasks gauge",
        f"raijin_failed_tasks {data['tasks']['failed']}",
        "# HELP raijin_retrying_tasks Number of tasks waiting for a retry.",
        "# TYPE raijin_retrying_tasks gauge",
        f"raijin_retrying_tasks {data['tasks']['retrying']}",
        "# HELP raijin_stale_task_leases Number of running tasks whose lease expired.",
        "# TYPE raijin_stale_task_leases gauge",
        f"raijin_stale_task_leases {data['tasks']['stale_leases']}",
        "# HELP raijin_active_multipart_checkpoints Incomplete resumable OCI multipart uploads.",
        "# TYPE raijin_active_multipart_checkpoints gauge",
        f"raijin_active_multipart_checkpoints {data['transfers']['active_multipart_checkpoints']}",
        "# HELP raijin_stalled_transfers Transfers with no persisted progress for more than ten minutes.",
        "# TYPE raijin_stalled_transfers gauge",
        f"raijin_stalled_transfers {data['transfers']['stalled']}",
        "# HELP raijin_failures_last_24h Persisted migration failures during the previous 24 hours.",
        "# TYPE raijin_failures_last_24h gauge",
        f"raijin_failures_last_24h {data['events']['failures_last_24h']}",
        "# HELP raijin_restore_expiry_risk_waves Waves predicted not to finish before a temporary restore copy expires.",
        "# TYPE raijin_restore_expiry_risk_waves gauge",
        f"raijin_restore_expiry_risk_waves {data['restore_expiry']['at_risk']}",
        "# HELP raijin_disk_free_bytes Free bytes on the persistent VM volume.",
        "# TYPE raijin_disk_free_bytes gauge",
        f"raijin_disk_free_bytes {data['disk']['free_bytes']}",
    ]
    return Response("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")


def destination_provenance_matches(obj: ObjectRecord, headers: dict) -> bool:
    """Validate the immutable S3 provenance stored by the upload worker.

    This deliberately uses OCI headers only: it catches a destination object
    replaced under the same key and size without charging AWS requests or
    rereading object data.
    """
    if obj.etag and headers.get("opc-meta-s3-oci-source-etag", "") != obj.etag:
        return False
    if obj.last_modified and headers.get("opc-meta-s3-oci-source-last-modified", "") != obj.last_modified.isoformat():
        return False
    return True


@app.get("/api/readiness")
def oci_readiness(session: Session = Depends(get_session)) -> dict:
    """Explicit OCI pre-check. It returns only readiness states, never secret values."""
    checks: list[dict] = []
    try:
        runtime_config = read_oci_runtime_config()
        secret_ocids = runtime_config.get("secret_ocids", {})
        object_storage_namespace = runtime_config.get("object_storage_namespace", "").strip()
    except (FileNotFoundError, json.JSONDecodeError, OSError) as error:
        return {"ready": False, "checks": [{"name": "Configuração OCI", "status": "NOT_CONFIGURED", "detail": type(error).__name__}]}
    if not object_storage_namespace:
        checks.append({"name": "Namespace OCI Object Storage", "status": "NOT_CONFIGURED", "detail": "namespace ausente do runtime gerado pelo Terraform"})
    try:
        import oci
        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        secrets_client = oci.secrets.SecretsClient({}, signer=signer)
        object_storage_client = oci.object_storage.ObjectStorageClient({}, signer=signer)
    except Exception as error:  # SDK exposes several auth-specific exception types.
        return {"ready": False, "checks": [{"name": "Identidade dinâmica OCI", "status": "FAILED", "detail": type(error).__name__}]}

    for secret_name in ["postgres_password"]:
        secret_ocid = secret_ocids.get(secret_name)
        if not secret_ocid:
            checks.append({"name": f"Secret {secret_name}", "status": "NOT_CONFIGURED", "detail": "OCID ausente"})
            continue
        try:
            bundle = secrets_client.get_secret_bundle(secret_ocid).data
            encoded_content = bundle.secret_bundle_content.content
            content = base64.b64decode(encoded_content).decode("utf-8").strip()
            status = "PLACEHOLDER" if content.startswith("REPLACE_THIS_PLACEHOLDER") else "CONFIGURED"
            detail = "valor preenchido; a validação funcional é exibida nos cartões AWS" if status == "CONFIGURED" else "placeholder ainda não substituído"
            if secret_name == "postgres_password" and status == "CONFIGURED":
                probe_engine = create_engine(URL.create("postgresql+psycopg", username="migration", password=content, host="postgres", port=5432, database="migration"), pool_pre_ping=True)
                try:
                    with probe_engine.connect() as connection:
                        connection.execute(text("SELECT 1"))
                    status = "VALIDATED"
                    detail = "senha do Vault autenticada no PostgreSQL local"
                except Exception as error:
                    detail = f"valor preenchido, mas autenticação PostgreSQL falhou: {type(error).__name__}"
                finally:
                    probe_engine.dispose()
            checks.append({"name": f"Secret {secret_name}", "status": status, "detail": detail})
        except Exception as error:
            checks.append({"name": f"Secret {secret_name}", "status": "FAILED", "detail": type(error).__name__})

    connections = list(session.scalars(select(AwsConnection).where(AwsConnection.archived_at.is_(None)).order_by(AwsConnection.id)))
    if not connections:
        checks.append({"name": "AWS connections", "status": "NOT_CONFIGURED", "detail": "register an AWS connection before operating sources"})
    for connection in connections:
        try:
            payload = aws_secret_payload(connection.secret_ocid)
            import boto3
            sts = boto3.client("sts", region_name=payload["default_region"], aws_access_key_id=payload["bootstrap_access_key_id"], aws_secret_access_key=payload["bootstrap_secret_access_key"])
            assumed = sts.assume_role(RoleArn=payload["migration_role_arn"], RoleSessionName="raijin-readiness", DurationSeconds=900)["Credentials"]
            role = boto3.Session(aws_access_key_id=assumed["AccessKeyId"], aws_secret_access_key=assumed["SecretAccessKey"], aws_session_token=assumed["SessionToken"], region_name=payload["default_region"])
            account_id = role.client("sts").get_caller_identity()["Account"]
            if account_id != connection.aws_account_id:
                raise RuntimeError("Assumed account differs from registered connection")
            role.client("s3").head_bucket(Bucket=payload["control_bucket"])
            checks.append({"name": f"AWS connection {connection.label}", "status": "VALIDATED", "detail": f"account {account_id}; control bucket validated"})
        except Exception as error:
            checks.append({"name": f"AWS connection {connection.label}", "status": "FAILED", "detail": safe_aws_error_summary(error)})

    try:
        if not object_storage_namespace:
            raise RuntimeError("Object Storage namespace is not configured")
        checks.append({"name": "Namespace OCI Object Storage", "status": "READY", "detail": object_storage_namespace})
        for bucket_name in sorted({source.destination_bucket for source in session.scalars(select(Source))}):
            try:
                object_storage_client.list_objects(object_storage_namespace, bucket_name, limit=1)
                checks.append({"name": f"Bucket OCI {bucket_name}", "status": "READY", "detail": "leitura autorizada"})
            except Exception as error:
                checks.append({"name": f"Bucket OCI {bucket_name}", "status": "FAILED", "detail": type(error).__name__})
    except Exception as error:
        checks.append({"name": "OCI Object Storage", "status": "FAILED", "detail": type(error).__name__})
    ready = all(check["status"] in ["READY", "VALIDATED", "CONFIGURED"] for check in checks)
    return {"ready": ready, "checks": checks}


@app.get("/api/operations")
def operations_overview(session: Session = Depends(get_session)) -> dict:
    """Local operational status; deliberately does not contact AWS or OCI."""
    session.execute(select(1))
    settings = runtime_settings(session)
    source_count = session.scalar(select(func.count(Source.id))) or 0
    object_count, bytes_total = session.execute(
        select(func.count(ObjectRecord.id), func.coalesce(func.sum(ObjectRecord.size_bytes), 0))
    ).one()
    task_counts = dict(session.execute(
        select(Task.state, func.count(Task.id)).group_by(Task.state)
    ).all())
    # A durable failure remains in the audit trail forever.  The banner must
    # instead represent the latest task of each wave: once an operator starts
    # a newer task (for example, Reprocess), the old failure is historical and
    # no longer requires action unless the newest task fails as well.
    latest_task = aliased(Task)
    latest_task_id = (
        select(func.max(latest_task.id))
        .where(latest_task.wave_id == Wave.id)
        .correlate(Wave)
        .scalar_subquery()
    )
    actionable_failed_tasks = session.scalar(
        select(func.count(Task.id)).join(Wave).join(Source, Source.id == Wave.source_id).where(
            Task.state == TaskState.FAILED,
            Task.id == latest_task_id,
            Source.archived_at.is_(None),
        )
    ) or 0
    task_counts["ACTIONABLE_FAILED"] = int(actionable_failed_tasks)
    window_seconds = 300
    since = utcnow() - timedelta(seconds=window_seconds)
    transferred_bytes, transferred_files, first_transfer = session.execute(
        select(func.coalesce(func.sum(ObjectRecord.size_bytes), 0), func.count(ObjectRecord.id), func.min(ObjectRecord.transferred_at)).where(
            ObjectRecord.transferred_at >= since
        )
    ).one()
    restored_files, first_restore = session.execute(
        select(func.count(ObjectRecord.id), func.min(ObjectRecord.restored_at)).where(
            ObjectRecord.restored_at >= since,
            ObjectRecord.storage_class.in_(ARCHIVE_STORAGE_CLASSES),
        )
    ).one()
    restore_requested_total, restore_available_total = session.execute(
        select(
            func.count(ObjectRecord.id).filter(ObjectRecord.restore_requested_at.is_not(None)),
            func.count(ObjectRecord.id).filter(ObjectRecord.restore_requested_at.is_not(None), ObjectRecord.restored_at.is_not(None)),
        )
    ).one()
    transferred_bytes, transferred_files, restored_files = int(transferred_bytes or 0), int(transferred_files or 0), int(restored_files or 0)
    transfer_seconds = min(window_seconds, max(1, (utcnow() - first_transfer).total_seconds())) if first_transfer else window_seconds
    restore_seconds = min(window_seconds, max(1, (utcnow() - first_restore).total_seconds())) if first_restore else window_seconds
    live_transfer_mbps = float(session.scalar(select(func.coalesce(func.sum(ObjectRecord.transfer_rate_mbps), 0)).where(
        ObjectRecord.state == ObjectState.TRANSFERRING,
            ObjectRecord.wave_id.in_(select(TransferQueueItem.wave_id).where(TransferQueueItem.state == TransferQueueState.LEASED)),
    )) or 0)
    control_logical_recent = session.scalar(select(ObjectRecord.id).join(Source).where(
        ObjectRecord.transferred_at >= since,
        Source.backend_kind == "SIMULATED",
        Source.simulation_fidelity == "CONTROL",
    ).limit(1)) is not None
    control_logical_active = session.scalar(select(ObjectRecord.id).join(Wave).join(Source).where(
        ObjectRecord.state == ObjectState.TRANSFERRING,
        Source.backend_kind == "SIMULATED",
        Source.simulation_fidelity == "CONTROL",
    ).limit(1)) is not None
    # Tiny objects can finish before a two-second in-flight sample exists. Add
    # their completion throughput from a short fixed window so current activity
    # remains meaningful for workloads with many small files.
    live_window_seconds = 15
    live_since = utcnow() - timedelta(seconds=live_window_seconds)
    recently_completed_bytes = int(session.scalar(select(func.coalesce(func.sum(ObjectRecord.size_bytes), 0)).where(
        ObjectRecord.transferred_at >= live_since,
    )) or 0)
    if not (control_logical_active or control_logical_recent):
        live_transfer_mbps += (recently_completed_bytes * 8) / live_window_seconds / 1_000_000
    if control_logical_active or control_logical_recent:
        # CONTROL promotes catalog evidence immediately. Display the bounded
        # simulated link allocation, never an impossible wall-clock rate.
        live_transfer_mbps = min(float(settings.max_throughput_mbps), live_transfer_mbps)
    if control_logical_recent:
        logical_elapsed = float(session.scalar(select(func.coalesce(func.sum(ObjectRecord.transfer_elapsed_seconds), 0)).join(Source).where(
            ObjectRecord.transferred_at >= since,
            Source.backend_kind == "SIMULATED",
            Source.simulation_fidelity == "CONTROL",
        )) or 0)
        if logical_elapsed > 0:
            transfer_seconds = max(1, logical_elapsed / max(1, RAIJU_MIN_WORKERS))
    active_transfer_rows = session.execute(
        select(
            Wave.id, Wave.name, Source.name,
            func.count(ObjectRecord.id),
            func.coalesce(func.sum(ObjectRecord.size_bytes), 0),
            func.coalesce(func.sum(case((ObjectRecord.state.in_([ObjectState.TRANSFERRED, ObjectState.VERIFIED]), 1), else_=0)), 0),
            func.coalesce(func.sum(case((ObjectRecord.state.in_([ObjectState.TRANSFERRED, ObjectState.VERIFIED]), ObjectRecord.size_bytes), else_=0)), 0),
            func.coalesce(func.sum(case((ObjectRecord.state == ObjectState.TRANSFERRING, 1), else_=0)), 0),
            func.coalesce(func.sum(case((ObjectRecord.state == ObjectState.TRANSFERRING, ObjectRecord.transfer_progress_bytes), else_=0)), 0),
            func.coalesce(func.sum(case((ObjectRecord.state == ObjectState.TRANSFERRING, ObjectRecord.transfer_rate_mbps), else_=0)), 0),
        ).join(Source, Source.id == Wave.source_id).join(ObjectRecord, ObjectRecord.wave_id == Wave.id)
        .where(Wave.id.in_(select(TransferQueueItem.wave_id).where(TransferQueueItem.state == TransferQueueState.LEASED)))
        .group_by(Wave.id, Source.name).order_by(Wave.id)
    ).all()
    active_transfers = [
        {"wave_id": wave_id, "wave_name": wave_name, "source_name": source_name,
         "total_files": int(total_files), "total_bytes": int(total_bytes),
         "transferred_files": int(done_files), "transferred_bytes": int(done_bytes),
         "in_flight_files": int(in_flight_files), "in_flight_bytes": int(in_flight_bytes), "live_mbps": round(float(live_mbps), 2)}
        for wave_id, wave_name, source_name, total_files, total_bytes, done_files, done_bytes, in_flight_files, in_flight_bytes, live_mbps in active_transfer_rows
    ]
    # Raiju concurrency is selected dynamically by the transfer worker and
    # persisted on the running wave.  A Raikou process owns governance work
    # serially, so its busy state is represented by a live governance task or
    # discovery job.  Service liveness is combined with this local snapshot by
    # the browser, because only the host status collector can see containers.
    running_transfer_wave_ids = select(TransferQueueItem.wave_id).where(
        TransferQueueItem.state == TransferQueueState.LEASED
    )
    raiju_active = int(session.scalar(select(func.coalesce(func.sum(Wave.active_transfer_workers), 0)).where(
        Wave.id.in_(running_transfer_wave_ids)
    )) or 0)
    raiju_busy = int(session.scalar(select(func.count(ObjectRecord.id)).where(
        ObjectRecord.state == ObjectState.TRANSFERRING,
        ObjectRecord.wave_id.in_(running_transfer_wave_ids),
    )) or 0)
    raikou_task_kinds = ("SUBMIT_BATCH_RESTORE", "POLL_RESTORE", "VERIFY_WAVE")
    raikou_busy = int(session.scalar(select(func.count(Task.id)).where(
        Task.kind.in_(raikou_task_kinds), Task.state == TaskState.RUNNING
    )) or 0)
    raikou_busy += int(session.scalar(select(func.count(DiscoveryJob.id)).where(
        DiscoveryJob.state == TaskState.RUNNING
    )) or 0)
    transfer_mbps = (transferred_bytes * 8) / transfer_seconds / 1_000_000
    if control_logical_recent:
        # Older CONTROL evidence may predate the aggregate-cap contract. Do
        # not let it make the console claim an impossible link speed.
        transfer_mbps = min(float(settings.max_throughput_mbps), transfer_mbps)
    volume = shutil.disk_usage("/")
    return {
        "status": "ok",
        "time": utcnow(),
        "sources": source_count,
        "objects": object_count,
        "bytes": bytes_total,
        "tasks": task_counts,
        "activity": {
            "window_seconds": window_seconds,
            "transfer_bytes": transferred_bytes,
            "transfer_files": transferred_files,
            "transferred_per_minute": round(transferred_files / (transfer_seconds / 60), 2),
            "transferred_per_hour": round(transferred_files / (transfer_seconds / 3600), 2),
            "transfer_mbps": round(transfer_mbps, 2),
            "transfer_live_mbps": round(live_transfer_mbps, 2),
            "simulation_logical_metrics": bool(control_logical_recent or control_logical_active),
            "transfer_live_window_seconds": live_window_seconds,
            "restored_files": restored_files,
            "restore_requested_total": int(restore_requested_total or 0),
            "restore_available_total": int(restore_available_total or 0),
            "restored_per_minute": round(restored_files / (restore_seconds / 60), 2),
            "restored_per_hour": round(restored_files / (restore_seconds / 3600), 2),
            "active_transfers": active_transfers,
        },
        "workers": {
            "raiju": {"active": raiju_active, "busy": raiju_busy, "idle": max(0, raiju_active - raiju_busy)},
            "raikou": {"busy": min(1, raikou_busy)},
        },
        "disk": {"total": volume.total, "used": volume.used, "free": volume.free},
    }


def restore_timing(session: Session, wave_id: int) -> dict:
    """Summarize durable per-object S3 restore timestamps and availability."""
    requested_at, first_available_at, last_available_at, earliest_expiry_at, requested_objects, available_objects = session.execute(
        select(
            func.min(ObjectRecord.restore_requested_at),
            func.min(ObjectRecord.restored_at),
            func.max(ObjectRecord.restored_at),
            func.min(ObjectRecord.restore_expires_at),
            func.count(ObjectRecord.id),
            func.count(ObjectRecord.id).filter(ObjectRecord.restored_at.is_not(None)),
        ).where(ObjectRecord.wave_id == wave_id, ObjectRecord.restore_requested_at.is_not(None))
    ).one()
    requested_objects, available_objects = int(requested_objects or 0), int(available_objects or 0)
    availability_span_seconds = max(0, int((last_available_at - first_available_at).total_seconds())) if first_available_at and last_available_at else None
    return {
        "requested_at": requested_at,
        "first_available_at": first_available_at,
        "last_available_at": last_available_at,
        "earliest_expiry_at": earliest_expiry_at,
        "requested_objects": requested_objects,
        "available_objects": available_objects,
        "pending_objects": max(0, requested_objects - available_objects),
        "restore_elapsed_seconds": max(0, int((last_available_at - requested_at).total_seconds())) if requested_at and last_available_at else None,
        # This is the average interval between consecutive availability events,
        # not the Batch job duration. It becomes meaningful after at least two
        # objects are visible as restored.
        "average_availability_interval_seconds": round(availability_span_seconds / (available_objects - 1), 2)
        if availability_span_seconds is not None and available_objects > 1 else None,
    }


def restore_queue_details(wave: Wave, task: Task, now: datetime, session: Session | None = None) -> dict:
    """Safe, local-only restore diagnostics used by the dashboard queue API."""
    task_created_at = task.created_at
    if task_created_at and task_created_at.tzinfo is None:
        # SQLite test/local runtimes can return a naive value for a timezone
        # column. The durable timestamp is UTC, so normalize before deriving
        # the display-only wait duration.
        task_created_at = task_created_at.replace(tzinfo=timezone.utc)
    attempt = session.scalar(select(RestoreAttempt).where(
        RestoreAttempt.wave_id == wave.id
    ).order_by(RestoreAttempt.id.desc()).limit(1)) if session is not None else None
    timing = restore_timing(session, wave.id) if session is not None else {
        "requested_at": None, "first_available_at": None, "last_available_at": None,
        "earliest_expiry_at": None, "requested_objects": 0, "available_objects": 0,
        "pending_objects": 0, "restore_elapsed_seconds": None,
    }
    return {
        "batch_job_id": wave.batch_job_id,
        "batch_status": wave.batch_job_status,
        "last_poll_at": wave.last_poll_at,
        "poll_count": int(wave.poll_count or 0),
        "next_attempt_at": task.available_at if task.state == TaskState.READY else None,
        "waiting_seconds": max(0, int((now - task_created_at).total_seconds())) if task_created_at and task.kind in {"SUBMIT_BATCH_RESTORE", "POLL_RESTORE"} else 0,
        "last_error": task.error,
        "availability_poll_interval_seconds": restore_availability_poll_delay_seconds(
            attempt.completed_at if attempt else None, now, wave.restore_tier,
            partial_availability=bool(timing["available_objects"] and timing["pending_objects"]),
            transfer_strategy=wave.transfer_release_policy,
            pending_objects=int(timing["pending_objects"] or 0),
        ) if task.kind == "POLL_RESTORE" and attempt and attempt.completed_at else None,
        "availability_polling": {
            "method": "HeadObject (pending wave objects)",
            "head_requests": int(getattr(wave, "availability_head_requests", 0) or 0),
            "elapsed_seconds": round(float(getattr(wave, "availability_poll_elapsed_seconds", 0) or 0), 2),
            "throttle_retries": int(getattr(wave, "availability_throttle_retries", 0) or 0),
            "last_checked_objects": int(getattr(wave, "last_availability_poll_objects", 0) or 0),
            "last_elapsed_seconds": round(float(getattr(wave, "last_availability_poll_seconds", 0) or 0), 2),
        },
        "batch_evidence_polling": {
            "describe_requests": int(getattr(attempt, "batch_describe_requests", 0) or 0) if attempt else 0,
            "completion_report_list_requests": int(getattr(attempt, "completion_report_list_requests", 0) or 0) if attempt else 0,
            "completion_report_get_requests": int(getattr(attempt, "completion_report_get_requests", 0) or 0) if attempt else 0,
        },
        **timing,
    }


@app.get("/api/transfer-queue")
def transfer_queue(session: Session = Depends(get_session)) -> dict:
    """Queue view for the dashboard; it only reads the local control database."""
    now = utcnow()
    transfer_kinds = ("SUBMIT_BATCH_RESTORE", "POLL_RESTORE", "TRANSFER_CONTINUOUS")
    tasks = list(session.scalars(
        select(Task).join(Wave).join(Source).where(
            Task.kind.in_(transfer_kinds), Task.state.in_([TaskState.READY, TaskState.RUNNING]),
            Wave.status != "PAUSED",
            Source.archived_at.is_(None),
        ).order_by(Task.available_at, Task.id)
    ))
    # At most one current task represents each wave.  Prefer a running Raiju
    # transfer over an availability poll: an early-release wave can be both
    # RESTORING and actively TRANSFERRING at the same time.
    by_wave: dict[int, Task] = {}
    for task in tasks:
        previous = by_wave.get(task.wave_id)
        score = (2 if task.kind == "TRANSFER_CONTINUOUS" and task.state == TaskState.RUNNING else
                 1 if task.state == TaskState.RUNNING else 0)
        previous_score = (-1 if previous is None else
                          2 if previous.kind == "TRANSFER_CONTINUOUS" and previous.state == TaskState.RUNNING else
                          1 if previous.state == TaskState.RUNNING else 0)
        if previous is None or score > previous_score:
            by_wave[task.wave_id] = task

    # A continuous dispatcher is source-scoped, while queue ownership remains
    # object/wave-scoped.  Include every wave that has durable lane work even
    # when the current dispatcher happens to be anchored to another wave.
    lane_rows = list(session.execute(
        select(TransferQueueItem.wave_id, TransferQueueItem.state,
               func.count(TransferQueueItem.id), func.coalesce(func.sum(TransferQueueItem.size_bytes), 0),
               func.min(TransferQueueItem.available_at), func.min(TransferQueueItem.restore_expires_at))
        .join(Wave, Wave.id == TransferQueueItem.wave_id)
        .join(Source, Source.id == TransferQueueItem.source_id)
        .where(
            TransferQueueItem.state.in_([
                TransferQueueState.AVAILABLE, TransferQueueState.READY,
                TransferQueueState.LEASED, TransferQueueState.MULTIPART_RESUME,
                TransferQueueState.RETRY_WAIT,
            ]),
            Wave.status != "PAUSED", Source.archived_at.is_(None),
        )
        .group_by(TransferQueueItem.wave_id, TransferQueueItem.state)
    ))
    lane_by_wave: dict[int, dict] = {}
    for wave_id, state, count, size_bytes, available_at, expires_at in lane_rows:
        lane = lane_by_wave.setdefault(int(wave_id), {
            "states": {}, "items": 0, "bytes": 0, "available_at": available_at,
            "earliest_expiry_at": expires_at,
        })
        state_name = state.value if hasattr(state, "value") else str(state)
        lane["states"][state_name] = int(count or 0)
        lane["items"] += int(count or 0)
        lane["bytes"] += int(size_bytes or 0)
        if available_at and (lane["available_at"] is None or available_at < lane["available_at"]):
            lane["available_at"] = available_at
        if expires_at and (lane["earliest_expiry_at"] is None or expires_at < lane["earliest_expiry_at"]):
            lane["earliest_expiry_at"] = expires_at

    wave_ids = set(by_wave) | set(lane_by_wave)
    wave_models = {
        wave.id: wave for wave in session.scalars(select(Wave).where(Wave.id.in_(wave_ids)))
    } if wave_ids else {}
    # The queue dashboard is a read model. Do not load every object in every
    # active wave merely to render a handful of totals; a 10 TB wave may have
    # millions of rows. In-flight object detail is bounded by the Raiju pool.
    object_totals = {
        int(wave_id): {
            "total_files": int(total_files or 0),
            "transferred_files": int(transferred_files or 0),
            "total_bytes": int(total_bytes or 0),
            "transferred_bytes": int(transferred_bytes or 0),
            "elapsed_seconds": int(elapsed or 0),
        }
        for wave_id, total_files, transferred_files, total_bytes, transferred_bytes, elapsed in session.execute(
            select(
                ObjectRecord.wave_id,
                func.count(ObjectRecord.id),
                func.coalesce(func.sum(case((ObjectRecord.state.in_([
                    ObjectState.TRANSFERRED, ObjectState.VERIFIED,
                ]), 1), else_=0)), 0),
                func.coalesce(func.sum(ObjectRecord.size_bytes), 0),
                func.coalesce(func.sum(case((ObjectRecord.state.in_([
                    ObjectState.TRANSFERRED, ObjectState.VERIFIED,
                ]), ObjectRecord.size_bytes), else_=0)), 0),
                func.coalesce(func.sum(ObjectRecord.transfer_elapsed_seconds), 0),
            ).where(ObjectRecord.wave_id.in_(wave_ids)).group_by(ObjectRecord.wave_id)
        )
    } if wave_ids else {}
    in_flight_by_wave: dict[int, list[ObjectRecord]] = {}
    if wave_ids:
        for obj in session.scalars(
            select(ObjectRecord).where(
                ObjectRecord.wave_id.in_(wave_ids),
                ObjectRecord.state == ObjectState.TRANSFERRING,
            ).order_by(ObjectRecord.wave_id, ObjectRecord.id)
        ):
            in_flight_by_wave.setdefault(int(obj.wave_id), []).append(obj)
    waves = []
    for wave_id in wave_ids:
        task = by_wave.get(wave_id)
        wave = wave_models.get(wave_id) or (task.wave if task else None)
        if wave is None:
            continue
        lane = lane_by_wave.get(wave_id, {})
        totals = object_totals.get(wave.id, {})
        in_flight = in_flight_by_wave.get(wave.id, [])
        active = bool(lane.get("states", {}).get(TransferQueueState.LEASED))
        # Task state describes the worker lease (READY/RUNNING); it is not the
        # lifecycle state of the wave.  Keep both in the API so a queued poll
        # cannot visually downgrade a submitted restore from RESTORING to
        # READY in the operator console.
        if active:
            operational_state = "RESTORE + TRANSFER" if wave.status == "RESTORING" else "TRANSFERRING"
        elif wave.status in {"RESTORE_REQUESTED", "RESTORE_REQUEST_ACCEPTED", "RESTORING"}:
            operational_state = "RESTORING"
        elif wave.status == "RESTORE_SCHEDULED":
            operational_state = "RESTORE SCHEDULED"
        elif wave.status == "RESTORED":
            operational_state = "READY FOR TRANSFER"
        else:
            operational_state = wave.status
        workers = []
        capacity = RAIJU_MIN_WORKERS
        for slot in range(capacity):
            obj = in_flight[slot] if slot < len(in_flight) else None
            workers.append({
                "slot": slot + 1,
                "state": "TRANSFERRING" if obj else "IDLE",
                "object_key": obj.object_key if obj else None,
                "progress_bytes": int(obj.transfer_progress_bytes or 0) if obj else 0,
                "total_bytes": int(obj.size_bytes) if obj else 0,
                "rate_mbps": round(float(obj.transfer_rate_mbps or 0), 2) if obj else 0,
                "elapsed_seconds": int(float(obj.transfer_elapsed_seconds or 0)) if obj else 0,
            })
        waves.append({
            "wave_id": wave.id, "wave_name": wave.name, "source_id": wave.source_id, "source_name": wave.source.name,
            "status": wave.status,
            "operational_state": operational_state,
            "task_kind": "TRANSFER_CONTINUOUS" if lane and active else (task.kind if task else "TRANSFER_CONTINUOUS"),
            "task_state": (TaskState.RUNNING if active else (task.state if task else TaskState.READY)),
            "available_at": lane.get("available_at") or (task.available_at if task else wave.planned_transfer_start_at) or now,
            "active": active,
            # Batch data comes from durable local records only.  It makes the
            # wait explainable without turning dashboard refreshes into AWS API
            # calls (and therefore without adding request cost).
            "restore": restore_queue_details(wave, task, now, session=session) if task else {
                **restore_timing(session, wave.id),
                "batch_job_id": wave.batch_job_id, "batch_status": wave.batch_job_status,
                "last_poll_at": wave.last_poll_at, "poll_count": int(wave.poll_count or 0),
                "next_attempt_at": None, "waiting_seconds": 0, "last_error": None,
                "earliest_expiry_at": lane.get("earliest_expiry_at"),
            },
            "transferred_files": totals.get("transferred_files", 0), "total_files": totals.get("total_files", 0),
            "transferred_bytes": totals.get("transferred_bytes", 0),
            "total_bytes": totals.get("total_bytes", 0),
            "elapsed_seconds": totals.get("elapsed_seconds", 0), "workers": workers if active else [],
            "continuous_lane": lane,
        })
    waves.sort(key=lambda item: (not item["active"], item["available_at"], item["wave_id"]))
    # A dynamic horizon deliberately keeps one locally mutable look-ahead
    # wave without a durable AWS task. Show it explicitly so operators do not
    # mistake it for a missing queue entry.
    queued_ids = set(by_wave) | set(lane_by_wave)
    planned = []
    for wave, source in session.execute(
        select(Wave, Source).join(Source).where(
            Wave.planner_mode == "DYNAMIC",
            Wave.status == "RESTORE_SCHEDULED",
            Source.archived_at.is_(None),
        ).order_by(Wave.pipeline_run_id, Wave.id)
    ):
        if wave.id in queued_ids:
            continue
        planned.append({
            "wave_id": wave.id, "wave_name": wave.name, "source_id": source.id,
            "source_name": source.name, "planned_restore_at": wave.planned_restore_at,
            "planned_transfer_start_at": wave.planned_transfer_start_at,
        })
    lane_totals = {
        "items": 0,
        "bytes": 0,
        "available_items": 0,
        "leased_items": 0,
        "retry_items": 0,
        "earliest_expiry_at": None,
    }
    for lane in lane_by_wave.values():
        lane_totals["items"] += int(lane.get("items") or 0)
        lane_totals["bytes"] += int(lane.get("bytes") or 0)
        states = lane.get("states") or {}
        lane_totals["available_items"] += int(states.get(TransferQueueState.AVAILABLE.value, 0)) + int(states.get(TransferQueueState.READY.value, 0))
        lane_totals["leased_items"] += int(states.get(TransferQueueState.LEASED.value, 0))
        lane_totals["retry_items"] += int(states.get(TransferQueueState.RETRY_WAIT.value, 0)) + int(states.get(TransferQueueState.MULTIPART_RESUME.value, 0))
        expiry = lane.get("earliest_expiry_at")
        if expiry and (lane_totals["earliest_expiry_at"] is None or expiry < lane_totals["earliest_expiry_at"]):
            lane_totals["earliest_expiry_at"] = expiry
    priority_rows = list(session.execute(
        select(TransferQueueItem.priority_band, func.count(TransferQueueItem.id)).where(
            TransferQueueItem.state.in_([
                TransferQueueState.AVAILABLE, TransferQueueState.READY,
                TransferQueueState.LEASED, TransferQueueState.RETRY_WAIT,
                TransferQueueState.MULTIPART_RESUME,
            ])
        ).group_by(TransferQueueItem.priority_band)
    ))
    lane_totals["priority_bands"] = {str(band): int(count or 0) for band, count in priority_rows}
    # A handoff is a durable reservation, not merely a transient event. Show
    # it in the read model so operators can distinguish "critical work is
    # waiting" from "critical work already owns the next safe Raiju slot".
    lane_totals["handoff_reservations"] = int(session.scalar(
        select(func.count(TransferQueueItem.id)).where(
            TransferQueueItem.state == TransferQueueState.LEASED,
            TransferQueueItem.preemption_successor_item_id.is_not(None),
        )
    ) or 0)
    # The queue screen must explain what Raikou will do next without causing
    # another AWS call.  This is a read of the durable lane after the most
    # recent priority refresh performed by governance/dispatch.
    next_item = session.scalar(
        select(TransferQueueItem)
        .join(Wave, Wave.id == TransferQueueItem.wave_id)
        .join(Source, Source.id == TransferQueueItem.source_id)
        .where(
            TransferQueueItem.state.in_([
                TransferQueueState.READY,
                TransferQueueState.RETRY_WAIT,
                TransferQueueState.MULTIPART_RESUME,
            ]),
            or_(TransferQueueItem.retry_at.is_(None), TransferQueueItem.retry_at <= now),
            Wave.status != "PAUSED",
            Source.archived_at.is_(None),
        )
        .order_by(
            TransferQueueItem.priority_score.desc(),
            TransferQueueItem.restore_expires_at.is_(None),
            TransferQueueItem.restore_expires_at,
            TransferQueueItem.available_at,
            TransferQueueItem.id,
        )
        .limit(1)
    )
    if next_item:
        next_wave = session.get(Wave, next_item.wave_id)
        next_source = session.get(Source, next_item.source_id)
        lane_totals["next_decision"] = {
            "state": "READY_FOR_DISPATCH",
            "item_id": next_item.id,
            "source_id": next_item.source_id,
            "source_name": next_source.name if next_source else None,
            "wave_id": next_item.wave_id,
            "wave_name": next_wave.name if next_wave else None,
            "priority_score": int(next_item.priority_score or 0),
            "priority_band": next_item.priority_band,
            "reason": next_item.decision_reason,
            "size_bytes": int(next_item.size_bytes or 0),
            "restore_expires_at": next_item.restore_expires_at,
            "predicted_transfer_seconds": round(float(next_item.predicted_transfer_seconds or 0), 2),
        }
    else:
        lane_totals["next_decision"] = {"state": "NO_ELIGIBLE_ITEM"}
    oldest_available_at = session.scalar(select(func.min(TransferQueueItem.available_at)).where(
        TransferQueueItem.state.in_([
        TransferQueueState.AVAILABLE, TransferQueueState.READY,
        TransferQueueState.RETRY_WAIT, TransferQueueState.MULTIPART_RESUME,
    ])))
    if oldest_available_at and oldest_available_at.tzinfo is None:
        oldest_available_at = oldest_available_at.replace(tzinfo=timezone.utc)
    lane_totals["oldest_wait_seconds"] = max(0, int((now - oldest_available_at).total_seconds())) if oldest_available_at else 0
    restore_schedule = {
        "planned": len(planned),
        "submitted_or_polling": sum(1 for task in tasks if task.kind in {"SUBMIT_BATCH_RESTORE", "POLL_RESTORE"}),
        "restore_slots_active": sum(1 for wave in waves if wave["status"] in {"RESTORING", "RESTORE_DRAINING"}),
    }
    # A zero transfer rate is not enough context for an operator.  Keep the
    # diagnosis local and deterministic: dashboard reads must never trigger an
    # AWS request merely to explain an idle lane.
    if lane_totals["leased_items"]:
        idle_reason = {"state": "BUSY", "message": "Raiju is processing leased objects in the continuous lane."}
    elif lane_totals["available_items"]:
        idle_reason = {
            "state": "DISPATCH_PENDING",
            "message": "Restored objects are eligible; Raikou will dispatch the next durable batch.",
        }
    elif lane_totals["retry_items"]:
        idle_reason = {
            "state": "RETRY_WAIT",
            "message": "No immediately eligible object; transfer items are waiting for their retry time or multipart resume.",
        }
    elif restore_schedule["submitted_or_polling"]:
        idle_reason = {
            "state": "AWAITING_RESTORE",
            "message": "No restored object is currently eligible; active waves are still awaiting restore availability.",
        }
    elif restore_schedule["planned"]:
        idle_reason = {
            "state": "PLANNED_RESTORE",
            "message": "No transfer backlog yet; Raikou is retaining a planned restore horizon.",
        }
    else:
        idle_reason = {"state": "EMPTY", "message": "The continuous lane has no eligible object or planned restore work."}
    lane_totals["idle_diagnosis"] = idle_reason
    return {
        "waves": waves,
        "planned_lookahead": planned,
        "restore_schedule": restore_schedule,
        "continuous_lane": lane_totals,
        "generated_at": now,
    }


@app.get("/api/flight-board/availability")
def flight_board_availability(source_id: int | None = Query(default=None, ge=1),
                              session: Session = Depends(get_session)) -> dict:
    """Cheap indicator for the queued source's dynamic flight board only."""
    filters = [Wave.planner_mode == "DYNAMIC"]
    if source_id is not None:
        filters.append(Wave.source_id == source_id)
    dynamic_waves = session.scalar(select(func.count(Wave.id)).where(*filters)) or 0
    return {"available": bool(dynamic_waves), "waves": int(dynamic_waves), "source_id": source_id}


@app.get("/api/flight-board")
def flight_board(source_id: int | None = Query(default=None, ge=1),
                 run_id: int | None = Query(default=None, ge=1),
                 session: Session = Depends(get_session)) -> dict:
    """Return local-only phases for one source, or for one durable pipeline run."""
    now = utcnow()
    filters = [Wave.planner_mode == "DYNAMIC"]
    source_name = None
    if source_id is not None:
        source_name = source_or_404(session, source_id).name
        filters.append(Wave.source_id == source_id)
    if run_id is not None:
        filters.append(Wave.pipeline_run_id == run_id)
    rows = list(session.execute(
        select(Wave, Source).join(Source).where(*filters)
        .order_by(Wave.planned_transfer_start_at.nulls_last(), Wave.id).limit(500)
    ))
    wave_ids = [wave.id for wave, _source in rows]
    if not wave_ids:
        return {"waves": [], "generated_at": now, "truncated": False, "source_id": source_id,
                "source_name": source_name}
    run_ids = [wave.pipeline_run_id for wave, _ in rows if wave.pipeline_run_id is not None]
    runs = list(session.scalars(select(DynamicPipelineRun).where(DynamicPipelineRun.id.in_(run_ids)))) if run_ids else []
    for run in runs:
        refresh_dynamic_pipeline_run(session, run)
    if runs:
        session.commit()
    object_times = {
        wave_id: {"restore_requested_at": requested, "first_available_at": first_available,
                  "last_available_at": last_available, "transfer_started_at": transfer_started,
                  "transfer_completed_at": transfer_completed}
        for wave_id, requested, first_available, last_available, transfer_started, transfer_completed in session.execute(
            select(ObjectRecord.wave_id, func.min(ObjectRecord.restore_requested_at),
                   func.min(ObjectRecord.restored_at), func.max(ObjectRecord.restored_at),
                   func.min(ObjectRecord.transfer_started_at), func.max(ObjectRecord.transferred_at))
            .where(ObjectRecord.wave_id.in_(wave_ids)).group_by(ObjectRecord.wave_id)
        )
    }
    latest_tasks: dict[int, Task] = {}
    for task in session.scalars(select(Task).where(Task.wave_id.in_(wave_ids)).order_by(Task.wave_id, Task.id.desc())):
        latest_tasks.setdefault(task.wave_id, task)
    # A segment is the durable transfer-lane evidence.  CONTROL simulations
    # created before the lane recorded the logical elapsed duration can have a
    # zero-width segment; the linked object still retains its measured logical
    # duration, which lets this read model faithfully recover the interval.
    segments_by_wave: dict[int, list[tuple[TransferLaneSegment, float]]] = {}
    for segment, elapsed_seconds in session.execute(
        select(TransferLaneSegment, ObjectRecord.transfer_elapsed_seconds)
        .join(TransferQueueItem, TransferQueueItem.id == TransferLaneSegment.queue_item_id)
        .outerjoin(ObjectRecord, ObjectRecord.id == TransferQueueItem.object_id)
        .where(TransferLaneSegment.wave_id.in_(wave_ids))
        .order_by(TransferLaneSegment.started_at, TransferLaneSegment.id)
    ):
        segments_by_wave.setdefault(segment.wave_id, []).append((segment, float(elapsed_seconds or 0)))
    # The board is an observability endpoint and must remain available while a
    # large simulation is busy.  In particular, do not perform one clock HTTP
    # call per wave: ``dict.setdefault`` evaluates its value argument before
    # deciding whether the key already exists.  One stalled simulator request
    # must not turn an otherwise local, durable timeline into a 500 response.
    source_clock_now: dict[int, datetime] = {}
    for _wave, row_source in rows:
        if (not runtime_context.is_simulation or not row_source.simulation_execution_id
                or row_source.id in source_clock_now):
            continue
        persisted_virtual_points = [
            item
            for candidate_wave, candidate_source in rows
            if candidate_source.id == row_source.id
            for item in (
                candidate_wave.restore_requested_virtual_at,
                candidate_wave.first_restore_available_virtual_at,
                candidate_wave.last_restore_available_virtual_at,
                candidate_wave.transfer_started_virtual_at,
                candidate_wave.transfer_completed_virtual_at,
                candidate_wave.planned_restore_at,
                candidate_wave.planned_transfer_start_at,
            )
            if item is not None
        ]
        persisted_now = max(persisted_virtual_points) if persisted_virtual_points else now
        try:
            source_clock_now[row_source.id] = cloud_backend.clock(
                row_source.simulation_execution_id
            ).effective_now
        except (OSError, TimeoutError, ValueError):
            # The known virtual milestone is sufficient to render a stable,
            # truthful snapshot.  The next dashboard refresh will use the
            # live clock again once the simulator is responsive.
            source_clock_now[row_source.id] = persisted_now

    def board_timestamp(value: datetime | None) -> datetime | None:
        """SQLite test rows are naive; API timestamps are always UTC-aware."""
        if value is not None and value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    def phase(kind: str, start: datetime | None, end: datetime | None, *, planned: bool = False,
              expected_seconds: int | None = None, elapsed_seconds: int | None = None,
              bytes_transferred: int = 0, object_count: int = 0,
              entry_reason: str | None = None, exit_reason: str | None = None,
              nearest_expiry_at: datetime | None = None) -> dict | None:
        start, end = board_timestamp(start), board_timestamp(end)
        if not start or not end or end <= start:
            return None
        return {
            "kind": kind, "start_at": start, "end_at": end, "planned": planned,
            "expected_seconds": expected_seconds,
            "bytes_transferred": bytes_transferred, "object_count": object_count,
            "entry_reason": entry_reason, "exit_reason": exit_reason,
            "nearest_expiry_at": nearest_expiry_at,
            "elapsed_seconds": max(0, int(elapsed_seconds)) if elapsed_seconds is not None
            else (0 if planned else max(0, int((end - start).total_seconds()))),
        }

    board_waves = []
    for wave, row_source in rows:
        row_source_name = row_source.name
        times = object_times.get(wave.id, {})
        simulated = runtime_context.is_simulation and bool(row_source.simulation_execution_id)
        request_at = wave.restore_requested_virtual_at if simulated else times.get("restore_requested_at")
        first_available_at = wave.first_restore_available_virtual_at if simulated else times.get("first_available_at")
        available_at = wave.last_restore_available_virtual_at if simulated else times.get("last_available_at")
        transfer_started_at = wave.transfer_started_virtual_at if simulated else times.get("transfer_started_at")
        transfer_completed_at = wave.transfer_completed_virtual_at if simulated else times.get("transfer_completed_at")
        effective_now = source_clock_now.get(row_source.id, now)
        task = latest_tasks.get(wave.id)
        phases = []
        queue_end = request_at or wave.planned_restore_at
        # Simulation timestamps live in a source-owned virtual clock.  Do not
        # mix them with the database wall-clock creation timestamp, otherwise
        # a planned lane can appear to start days before its own scenario.
        queue_start = wave.planned_restore_at if simulated else wave.created_at
        if queue_start and queue_end and queue_start > queue_end:
            queue_start = queue_end
        queued = phase("QUEUE", queue_start, queue_end, planned=not bool(request_at))
        if queued:
            phases.append(queued)
        restore_start = request_at or wave.planned_restore_at
        expected_available_at = (
            restore_start + timedelta(seconds=restore_service_window_seconds(wave.restore_tier))
            if restore_start else None
        )
        if request_at:
            restore_end = available_at or transfer_started_at or effective_now
        else:
            restore_end = expected_available_at
        restore_progress_seconds = max(0, int((restore_end - restore_start).total_seconds())) if restore_start and restore_end else 0
        restore = phase("RESTORE", restore_start, restore_end, planned=not bool(request_at),
                        expected_seconds=restore_service_window_seconds(wave.restore_tier),
                        elapsed_seconds=restore_progress_seconds)
        if restore:
            phases.append(restore)
        # The orange forecast ends at the tier service window. Safety and
        # transfer-lane serialization are not restore time. If they create a
        # gap, expose it separately as readiness/waiting time.
        if request_at and not available_at and expected_available_at:
            forecast_restore = phase("RESTORE", restore_end, expected_available_at, planned=True,
                                     expected_seconds=restore_service_window_seconds(wave.restore_tier),
                                     elapsed_seconds=restore_progress_seconds)
            if forecast_restore:
                phases.append(forecast_restore)
        # The continuous lane is event driven.  A future transfer window is
        # not a meaningful forecast: files are admitted as soon as they are
        # restored and the scheduler can reprioritize them at every free Raiju
        # slot.  Therefore this board never synthesizes a blue segment from a
        # wave plan.  Only the durable TransferLaneSegment records created at
        # actual dispatch time are allowed to draw transfer activity.
        planned_transfer_at = wave.planned_transfer_start_at
        status = wave.status
        if transfer_started_at and not transfer_completed_at:
            status = "TRANSFERRING"
        elif request_at and not available_at:
            status = "RESTORING"
        elif available_at and not transfer_started_at:
            status = "RESTORED"
        board_waves.append({
            "wave_id": wave.id, "wave_name": wave.name, "source_name": row_source_name,
            "pipeline_run_id": wave.pipeline_run_id,
            "status": status, "task_state": task.state if task else None,
            "started_at": request_at or transfer_started_at or (wave.created_at if not simulated else None),
            "completed_at": transfer_completed_at if status in {"COMPLETED", "VERIFIED", "TRANSFERRED"} else None,
            "planned_restore_at": wave.planned_restore_at,
            "expected_restore_available_at": expected_available_at,
            # Retained only as a durable scheduler datum for APIs and reports;
            # it is deliberately not rendered as transfer work on this board.
            "planned_transfer_start_at": planned_transfer_at,
            "transfer_started_at": transfer_started_at,
            "transfer_completed_at": transfer_completed_at,
            "transfer_effective_start_at": transfer_started_at,
            "transfer_start_inferred": False,
            "predicted_transfer_seconds": int(wave.predicted_transfer_seconds or 0),
            "phases": phases,
        })
    # A continuous-transfer pipeline has one shared transfer lane.  Collapse
    # object records into the union of their durable intervals: rendering tens
    # of thousands of tiny object bars hides the period the lane was actually
    # busy and makes the browser unusable.  The union keeps exact persisted
    # boundaries, byte/object totals and all participating wave identities.
    wave_by_id = {wave.id: wave for wave, _source in rows}
    board_by_id = {wave["wave_id"]: wave for wave in board_waves}
    observed_intervals: list[dict] = []
    for wave_id, entries in segments_by_wave.items():
        board_wave = board_by_id[wave_id]
        terminal_at = board_timestamp(board_wave.get("transfer_completed_at"))
        effective_now = board_timestamp(source_clock_now.get(wave_by_id[wave_id].source_id, now))
        for segment, logical_elapsed in entries:
            start = board_timestamp(segment.started_at)
            completed = board_timestamp(segment.completed_at)
            if completed and completed > start:
                end = completed
            elif terminal_at is not None:
                # Older CONTROL runs recorded a zero-width segment for every
                # object while preserving the wave's virtual completion.
                end = max(start + timedelta(seconds=1), terminal_at)
            elif logical_elapsed > 0:
                end = start + timedelta(seconds=logical_elapsed)
            elif completed is None and effective_now > start:
                end = effective_now
            else:
                # Keep a durable instantaneous event visible without claiming
                # that it occupied an unrecorded interval.
                end = start + timedelta(seconds=1)
            observed_intervals.append({
                "start_at": start, "end_at": end, "wave_ids": {wave_id},
                "bytes_transferred": int(segment.bytes_transferred or 0),
                "object_count": int(segment.object_count or 1),
                "nearest_expiry_at": segment.nearest_expiry_at,
                "segment_count": 1,
            })

    observed_intervals.sort(key=lambda item: (item["start_at"], item["end_at"]))
    merged_observed: list[dict] = []
    for interval in observed_intervals:
        if merged_observed and interval["start_at"] <= merged_observed[-1]["end_at"]:
            current = merged_observed[-1]
            current["end_at"] = max(current["end_at"], interval["end_at"])
            current["wave_ids"].update(interval["wave_ids"])
            current["bytes_transferred"] += interval["bytes_transferred"]
            current["object_count"] += interval["object_count"]
            current["segment_count"] += interval["segment_count"]
            expiry = interval["nearest_expiry_at"]
            if expiry and (current["nearest_expiry_at"] is None or expiry < current["nearest_expiry_at"]):
                current["nearest_expiry_at"] = expiry
        else:
            merged_observed.append(interval)

    transfer_lane_phases: list[dict] = []
    for interval in merged_observed:
        involved = sorted(interval["wave_ids"])
        labels = [f"#{wave_id}: {board_by_id[wave_id]['wave_name']}" for wave_id in involved]
        transfer_lane_phases.append({
            "kind": "TRANSFER", "start_at": interval["start_at"], "end_at": interval["end_at"],
            "planned": False,
            "expected_seconds": max(1, int((interval["end_at"] - interval["start_at"]).total_seconds())),
            "elapsed_seconds": max(1, int((interval["end_at"] - interval["start_at"]).total_seconds())),
            "bytes_transferred": interval["bytes_transferred"], "object_count": interval["object_count"],
            "wave_id": involved[0] if len(involved) == 1 else None,
            "wave_name": board_by_id[involved[0]]["wave_name"] if len(involved) == 1 else None,
            "entry_reason": f"{interval['segment_count']} segmento(s) observados na lane; " + ", ".join(labels),
                "exit_reason": "janela observada consolidada",
            "nearest_expiry_at": interval["nearest_expiry_at"],
        })

    # Predictions on the lane are deliberately restricted to work already
    # available in the durable queue.  A restoring wave does not reserve a
    # blue window: only a ready object can form a queued-lane projection.
    eligible_states = [
        TransferQueueState.AVAILABLE, TransferQueueState.READY,
        TransferQueueState.MULTIPART_RESUME,
    ]
    queued_items = list(session.scalars(
        select(TransferQueueItem).where(
            TransferQueueItem.wave_id.in_(wave_ids),
            or_(
                TransferQueueItem.state.in_(eligible_states),
                and_(TransferQueueItem.state == TransferQueueState.RETRY_WAIT,
                     or_(TransferQueueItem.retry_at.is_(None), TransferQueueItem.retry_at <= now)),
            ),
        ).order_by(
            TransferQueueItem.priority_score.desc(),
            TransferQueueItem.restore_expires_at.is_(None),
            TransferQueueItem.restore_expires_at,
            TransferQueueItem.available_at, TransferQueueItem.id,
        )
    ))
    projection_by_wave: dict[int, dict] = {}
    settings = runtime_settings(session)
    if queued_items:
        source_nows = [board_timestamp(source_clock_now.get(wave_by_id[item.wave_id].source_id, now)) for item in queued_items]
        cursor = max(source_nows)
        rate_bps = max(1.0, float(settings.max_throughput_mbps) * 1_000_000 / 8)
        for item in queued_items:
            seconds = max(1, math.ceil(int(item.size_bytes or 0) / rate_bps))
            start, end = cursor, cursor + timedelta(seconds=seconds)
            cursor = end
            prior = projection_by_wave.get(item.wave_id)
            if prior is None:
                prior = projection_by_wave[item.wave_id] = {
                    "kind": "TRANSFER", "start_at": start, "end_at": end, "planned": True,
                    "expected_seconds": seconds, "elapsed_seconds": 0,
                    "bytes_transferred": int(item.size_bytes or 0), "object_count": 1,
                    "wave_id": item.wave_id, "wave_name": board_by_id[item.wave_id]["wave_name"],
                    "entry_reason": "projeção da fila pronta aguardando Raiju",
                    "exit_reason": None, "nearest_expiry_at": item.restore_expires_at,
                }
            else:
                prior["end_at"] = end
                prior["expected_seconds"] += seconds
                prior["bytes_transferred"] += int(item.size_bytes or 0)
                prior["object_count"] += 1
                if item.restore_expires_at and (prior["nearest_expiry_at"] is None or item.restore_expires_at < prior["nearest_expiry_at"]):
                    prior["nearest_expiry_at"] = item.restore_expires_at
        for wave_id, projection in projection_by_wave.items():
            board_by_id[wave_id]["transfer_queue_projected_start_at"] = projection["start_at"]
            board_by_id[wave_id]["transfer_queue_projected_end_at"] = projection["end_at"]
            transfer_lane_phases.append(projection)
    transfer_lane_phases.sort(key=lambda item: (item["start_at"], item["end_at"], bool(item["planned"])))
    timeline_points = [point for wave in board_waves for phase_item in wave["phases"] for point in (phase_item["start_at"], phase_item["end_at"])]
    # Once processing starts, the time origin is immutable: it is the first
    # observed AWS restore submission for this source/run.  It must not drift
    # with ``now`` while the modal refreshes.  Sources without a submission
    # yet continue to use their earliest persisted planning point.
    submitted_points = [
        wave["started_at"] for wave in board_waves
        if wave["started_at"] is not None
    ]
    timeline_start = min(submitted_points) if submitted_points else (min(timeline_points) if timeline_points else now)
    return {"waves": board_waves,
            "transfer_lane": {"enabled": True, "phases": transfer_lane_phases},
            "generated_at": now, "source_id": source_id,
            "source_name": source_name,
            "timeline_start_at": timeline_start,
            "timeline_end_at": max(timeline_points) if timeline_points else now + timedelta(hours=1),
            "truncated": len(rows) >= 500}


@app.get("/api/sources/{source_id}/pipeline-history")
def source_pipeline_history(source_id: int, session: Session = Depends(get_session)) -> list[dict]:
    """Durable dynamic-pipeline runs attached to a source, including completed ones."""
    source_or_404(session, source_id)
    runs = list(session.scalars(select(DynamicPipelineRun).where(
        DynamicPipelineRun.source_id == source_id
    ).order_by(DynamicPipelineRun.created_at.desc(), DynamicPipelineRun.id.desc())))
    if not runs:
        return []
    run_ids = [run.id for run in runs]
    counts = {
        run_id: (int(total), int(completed), started_at, completed_at)
        for run_id, total, completed, started_at, completed_at in session.execute(
            select(Wave.pipeline_run_id, func.count(func.distinct(Wave.id)),
                   func.count(func.distinct(case((Wave.status.in_(["COMPLETED", "VERIFIED", "TRANSFERRED"]), Wave.id), else_=None))),
                   func.min(ObjectRecord.restore_requested_at), func.max(ObjectRecord.transferred_at))
            .outerjoin(ObjectRecord, ObjectRecord.wave_id == Wave.id)
            .where(Wave.pipeline_run_id.in_(run_ids)).group_by(Wave.pipeline_run_id)
        )
    }
    for run in runs:
        refresh_dynamic_pipeline_run(session, run)
    session.commit()
    return [{"id": run.id, "status": run.status, "planner_version": run.planner_version,
             "created_at": run.created_at, "completed_at": run.completed_at,
             "target_max_bytes": run.target_max_bytes, "target_transfer_seconds": run.target_transfer_seconds,
             "max_objects": run.max_objects, "restore_safety_seconds": run.restore_safety_seconds,
             "restore_days": run.restore_days, "restore_tier": run.restore_tier,
             "transfer_strategy": run.transfer_strategy, "scheduled_restores": run.scheduled_restores,
             "historical_samples": run.historical_samples,
             "waves": counts.get(run.id, (0, 0, None, None))[0],
             "completed_waves": counts.get(run.id, (0, 0, None, None))[1],
             "started_at": counts.get(run.id, (0, 0, None, None))[2],
             "last_transfer_at": counts.get(run.id, (0, 0, None, None))[3]}
            for run in runs]


@app.get("/api/deep-audits")
def deep_audits(session: Session = Depends(get_session)) -> dict:
    """Progress of explicitly approved full OCI rereads."""
    tasks = list(session.scalars(
        select(Task).where(Task.kind == "VERIFY_WAVE", Task.state.in_([TaskState.READY, TaskState.RUNNING]))
        .order_by(Task.created_at, Task.id)
    ))
    audits = []
    for task in tasks:
        wave = task.wave
        objects = list(session.scalars(select(ObjectRecord).where(ObjectRecord.wave_id == wave.id)))
        total_bytes = sum(obj.size_bytes for obj in objects)
        checked = [obj for obj in objects if obj.integrity_verified_at or obj.integrity_error]
        checked_bytes = sum(min(obj.size_bytes, int(obj.audit_progress_bytes or 0)) for obj in objects)
        live_rate = sum(float(obj.audit_rate_mbps or 0) for obj in objects if obj.audit_started_at and not obj.integrity_verified_at and not obj.integrity_error)
        audits.append({
            "task_id": task.id, "task_state": task.state, "wave_id": wave.id,
            "wave_name": wave.name, "source_name": wave.source.name,
            "objects_checked": len(checked), "objects_total": len(objects),
            "bytes_checked": checked_bytes, "bytes_total": total_bytes,
            "failed": sum(1 for obj in objects if obj.integrity_error),
            "rate_mbps": round(live_rate, 2), "started_at": min((obj.audit_started_at for obj in objects if obj.audit_started_at), default=None),
        })
    return {"audits": audits, "generated_at": utcnow()}


@app.get("/api/settings")
def get_settings(session: Session = Depends(get_session)) -> dict:
    return settings_dict(runtime_settings(session))


@app.get("/api/activity-refresh-settings")
def get_activity_refresh_settings(session: Session = Depends(get_session)) -> dict:
    settings = runtime_settings(session)
    return {"enabled": settings.activity_auto_refresh_enabled, "seconds": settings.activity_refresh_seconds}


@app.put("/api/activity-refresh-settings")
def update_activity_refresh_settings(payload: ActivityRefreshSettingsUpdate, session: Session = Depends(get_session)) -> dict:
    settings = runtime_settings(session)
    settings.activity_auto_refresh_enabled, settings.activity_refresh_seconds = payload.enabled, payload.seconds
    record_event(session, "ACTIVITY_REFRESH_SETTINGS_UPDATED", f"Activity auto-refresh {'enabled' if payload.enabled else 'disabled'}; interval {payload.seconds}s")
    session.commit()
    return {"enabled": settings.activity_auto_refresh_enabled, "seconds": settings.activity_refresh_seconds}


@app.put("/api/settings")
def update_settings(payload: RuntimeSettingsUpdate, session: Session = Depends(get_session)) -> dict:
    if not (payload.continuous_transfer_min_buffer_seconds <=
            payload.continuous_transfer_target_buffer_seconds <=
            payload.continuous_transfer_max_buffer_seconds):
        raise HTTPException(
            status_code=422,
            detail="Continuous-lane buffers must satisfy minimum ≤ target ≤ maximum.",
        )
    settings = runtime_settings(session)
    for field, value in payload.model_dump().items():
        if value is not None:
            setattr(settings, field, value)
    record_event(session, "SETTINGS_UPDATED", "Operational transfer settings updated")
    session.commit()
    return settings_dict(settings)


@app.get("/api/oci/buckets")
def list_oci_bucket_cache(session: Session = Depends(get_session)) -> dict:
    buckets = list(session.scalars(select(OciBucketCache).order_by(OciBucketCache.name, OciBucketCache.compartment_id)))
    refreshed_at = max((bucket.refreshed_at for bucket in buckets), default=None)
    return {"buckets": [{"name": bucket.name, "ocid": bucket.bucket_ocid,
                          "compartment_id": bucket.compartment_id, "compartment_name": bucket.compartment_name,
                          "lifecycle_state": bucket.lifecycle_state,
                          "refreshed_at": bucket.refreshed_at} for bucket in buckets],
            "refreshed_at": refreshed_at}


@app.post("/api/oci/buckets/refresh")
def refresh_oci_bucket_cache(session: Session = Depends(get_session)) -> dict:
    """Manually refresh tenancy-wide bucket metadata through OCI Resource Search."""
    try:
        import oci
        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        client = oci.resource_search.ResourceSearchClient({}, signer=signer)
        query = "query bucket resources"
        response = client.search_resources(oci.resource_search.models.StructuredSearchDetails(query=query))
        items = list(response.data.items)
        while response.next_page:
            response = client.search_resources(oci.resource_search.models.StructuredSearchDetails(query=query), page=response.next_page)
            items.extend(response.data.items)
    except Exception as error:
        raise HTTPException(status_code=502, detail=f"OCI Resource Search failed: {type(error).__name__}") from error

    try:
        configured_compartment_names = read_oci_runtime_config().get("destination_compartment_names", {})
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        configured_compartment_names = {}

    now, found = utcnow(), set()
    for item in items:
        bucket_ocid = getattr(item, "identifier", None)
        name = getattr(item, "display_name", None)
        if not bucket_ocid or not name:
            continue
        found.add(bucket_ocid)
        cached = session.scalar(select(OciBucketCache).where(OciBucketCache.bucket_ocid == bucket_ocid))
        if not cached:
            cached = OciBucketCache(bucket_ocid=bucket_ocid, name=name)
            session.add(cached)
        cached.name = name
        cached.compartment_id = getattr(item, "compartment_id", None)
        cached.compartment_name = (
            getattr(item, "compartment_name", None)
            or configured_compartment_names.get(cached.compartment_id)
        )
        cached.lifecycle_state = getattr(item, "lifecycle_state", None)
        cached.refreshed_at = now
    if found:
        session.query(OciBucketCache).filter(OciBucketCache.bucket_ocid.not_in(found)).delete(synchronize_session=False)
    else:
        session.query(OciBucketCache).delete(synchronize_session=False)
    record_event(session, "OCI_BUCKET_CACHE_REFRESHED", f"OCI Resource Search cached {len(found)} bucket(s)")
    session.commit()
    return {"buckets": len(found), "refreshed_at": now}


@app.get("/api/aws-secrets")
def list_aws_secret_cache(session: Session = Depends(get_session)) -> dict:
    secrets = list(session.scalars(select(AwsSecretCache).order_by(AwsSecretCache.name, AwsSecretCache.secret_ocid)))
    return {"secrets": [{"ocid": item.secret_ocid, "name": item.name, "compartment_id": item.compartment_id,
                          "schema_version": item.schema_version, "connection_name": item.connection_name,
                          "aws_account_id": item.aws_account_id, "default_region": item.default_region,
                          "valid": item.valid, "validation_error": item.validation_error,
                          "refreshed_at": item.refreshed_at} for item in secrets],
            "compatible": sum(1 for item in secrets if item.valid),
            "refreshed_at": max((item.refreshed_at for item in secrets), default=None)}


@app.post("/api/aws-secrets/refresh")
def refresh_aws_secret_cache(session: Session = Depends(get_session)) -> dict:
    """Explicitly inspect readable Secrets and cache only compatibility metadata."""
    try:
        import oci
        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        search = oci.resource_search.ResourceSearchClient({}, signer=signer)
        secrets_client = oci.secrets.SecretsClient({}, signer=signer)
        query = OCI_VAULT_SECRET_SEARCH_QUERY
        response = search.search_resources(oci.resource_search.models.StructuredSearchDetails(query=query))
        listed = list(response.data.items)
        while response.next_page:
            response = search.search_resources(oci.resource_search.models.StructuredSearchDetails(query=query), page=response.next_page)
            listed.extend(response.data.items)
    except Exception as error:
        raise HTTPException(status_code=502, detail=f"OCI Secret discovery failed: {type(error).__name__}") from error

    now, seen, compatible = utcnow(), set(), 0
    for secret in listed:
        secret_ocid = getattr(secret, "identifier", None)
        if not secret_ocid:
            continue
        seen.add(secret_ocid)
        cached = session.scalar(select(AwsSecretCache).where(AwsSecretCache.secret_ocid == secret_ocid))
        if not cached:
            cached = AwsSecretCache(secret_ocid=secret_ocid, name=getattr(secret, "display_name", secret_ocid), compartment_id=getattr(secret, "compartment_id", ""))
            session.add(cached)
        cached.name, cached.compartment_id, cached.refreshed_at = getattr(secret, "display_name", secret_ocid), getattr(secret, "compartment_id", ""), now
        cached.valid, cached.validation_error = False, None
        cached.schema_version = cached.connection_name = cached.aws_account_id = cached.default_region = None
        try:
            bundle = secrets_client.get_secret_bundle(secret_ocid).data
            payload = parse_aws_connection_payload(base64.b64decode(bundle.secret_bundle_content.content).decode("utf-8").strip())
            cached.schema_version = payload["schema_version"]
            cached.connection_name, cached.aws_account_id, cached.default_region = payload["connection_name"], payload["aws_account_id"], payload["default_region"]
            cached.valid, compatible = True, compatible + 1
        except Exception as error:
            cached.validation_error = safe_oci_error_summary(error)
    if seen:
        session.query(AwsSecretCache).filter(AwsSecretCache.secret_ocid.not_in(seen)).delete(synchronize_session=False)
    else:
        session.query(AwsSecretCache).delete(synchronize_session=False)
    record_event(session, "AWS_SECRET_CACHE_REFRESHED", f"Checked {len(seen)} readable OCI Secret(s); {compatible} match AWS connection schema v{AWS_CONNECTION_SCHEMA_VERSION}")
    session.commit()
    return {"secrets": len(seen), "compatible": compatible, "refreshed_at": now}


@app.get("/api/aws-connections")
def list_aws_connections(session: Session = Depends(get_session)) -> list[dict]:
    return [connection_summary(session, item) for item in session.scalars(select(AwsConnection).order_by(AwsConnection.id))]


@app.post("/api/aws-connections", status_code=201)
def create_aws_connection(payload: AwsConnectionCreate, session: Session = Depends(get_session)) -> dict:
    cached = session.scalar(select(AwsSecretCache).where(AwsSecretCache.secret_ocid == payload.secret_ocid, AwsSecretCache.valid.is_(True)))
    if not cached:
        raise HTTPException(status_code=422, detail="Refresh OCI Secrets and choose a compatible AWS connection Secret")
    if session.scalar(select(AwsConnection.id).where(AwsConnection.secret_ocid == payload.secret_ocid)):
        raise HTTPException(status_code=409, detail="This Secret is already registered as an AWS connection")
    if session.scalar(select(AwsConnection.id).where(AwsConnection.label == payload.label.strip())):
        raise HTTPException(status_code=409, detail="AWS connection label already exists")
    try:
        secret = aws_secret_payload(payload.secret_ocid)
    except Exception as error:
        raise HTTPException(status_code=422, detail=f"AWS connection Secret is no longer compatible: {type(error).__name__}") from error
    duplicate_bucket = session.scalar(select(AwsConnection.id).where(
        AwsConnection.aws_account_id == secret["aws_account_id"], AwsConnection.control_bucket == secret["control_bucket"]
    ))
    if duplicate_bucket:
        raise HTTPException(status_code=409, detail="This AWS account/control bucket pair is already registered")
    connection = AwsConnection(label=payload.label.strip(), secret_ocid=payload.secret_ocid,
                               aws_account_id=secret["aws_account_id"], default_region=secret["default_region"], control_bucket=secret["control_bucket"])
    session.add(connection)
    session.flush()
    record_event(session, "AWS_CONNECTION_CREATED", f"AWS connection '{connection.label}' registered for account {connection.aws_account_id}")
    session.commit()
    return connection_summary(session, connection)


@app.post("/api/aws-connections/{connection_id}/archive")
def archive_aws_connection(connection_id: int, session: Session = Depends(get_session)) -> dict:
    connection = connection_or_404(session, connection_id)
    if not session.scalar(select(Source.id).where(Source.aws_connection_id == connection.id)):
        raise HTTPException(status_code=409, detail="An unused AWS connection should be deleted, not archived")
    connection.archived_at = utcnow()
    record_event(session, "AWS_CONNECTION_ARCHIVED", f"AWS connection '{connection.label}' archived; historical sources retained")
    session.commit()
    return connection_summary(session, connection)


@app.post("/api/aws-connections/{connection_id}/precheck")
def precheck_aws_connection(connection_id: int, session: Session = Depends(get_session)) -> dict:
    """Validate one connection with STS and its control bucket, never listing source data."""
    connection = connection_or_404(session, connection_id)
    try:
        payload = aws_secret_payload(connection.secret_ocid)
        import boto3
        from botocore.config import Config
        config = Config(connect_timeout=10, read_timeout=30, retries={"max_attempts": 3, "mode": "standard"})
        bootstrap = boto3.Session(aws_access_key_id=payload["bootstrap_access_key_id"], aws_secret_access_key=payload["bootstrap_secret_access_key"], region_name=payload["default_region"])
        assumed = bootstrap.client("sts", config=config).assume_role(RoleArn=payload["migration_role_arn"], RoleSessionName="raijin-connection-precheck", DurationSeconds=900)["Credentials"]
        role = boto3.Session(aws_access_key_id=assumed["AccessKeyId"], aws_secret_access_key=assumed["SecretAccessKey"], aws_session_token=assumed["SessionToken"], region_name=payload["default_region"])
        account_id = role.client("sts", config=config).get_caller_identity()["Account"]
        if account_id != connection.aws_account_id or account_id != payload["aws_account_id"]:
            raise RuntimeError("Assumed account does not match registered connection")
        role.client("s3", config=config).head_bucket(Bucket=payload["control_bucket"])
    except Exception as error:
        record_event(session, "AWS_CONNECTION_PRECHECK_FAILED", f"AWS connection '{connection.label}' pre-check failed: {safe_aws_error_summary(error)}")
        session.commit()
        raise HTTPException(status_code=422, detail=f"AWS connection pre-check failed: {safe_aws_error_summary(error)}") from error
    record_event(session, "AWS_CONNECTION_PRECHECK_VALIDATED", f"AWS connection '{connection.label}' validated for account {account_id}")
    session.commit()
    return {"id": connection.id, "status": "VALIDATED", "aws_account_id": account_id, "control_bucket": connection.control_bucket}


@app.get("/api/aws-connections/{connection_id}/configuration")
def get_aws_connection_configuration(connection_id: int, session: Session = Depends(get_session)) -> dict:
    connection = connection_or_404(session, connection_id)
    try:
        return aws_connection_configuration(connection, aws_secret_payload(connection.secret_ocid))
    except Exception as error:
        raise HTTPException(status_code=422, detail=f"AWS connection Secret is no longer compatible: {type(error).__name__}") from error


@app.get("/api/aws-connections/{connection_id}/cost-pricing")
def get_cost_pricing(connection_id: int, session: Session = Depends(get_session)) -> dict:
    connection_or_404(session, connection_id)
    pricing = pricing_or_create(session, connection_id)
    session.commit()
    return pricing_dict(pricing)


@app.put("/api/aws-connections/{connection_id}/cost-pricing")
def update_cost_pricing(connection_id: int, payload: CostPricingUpdate, session: Session = Depends(get_session)) -> dict:
    connection = connection_or_404(session, connection_id)
    pricing = pricing_or_create(session, connection_id)
    for field, value in payload.model_dump().items():
        if field in PRICING_RATE_FIELDS:
            value = internal_rate_value(field, value)
        setattr(pricing, field, value)
    record_event(session, "COST_PRICING_UPDATED", f"Cost pricing updated for AWS connection '{connection.label}'", source_id=None)
    session.commit()
    return pricing_dict(pricing)


@app.get("/api/aws-connections/{connection_id}/operational-limits")
def get_aws_connection_operational_limits(connection_id: int, session: Session = Depends(get_session)) -> dict:
    connection = connection_or_404(session, connection_id)
    return connection_summary(session, connection)["operational_limits"]


@app.put("/api/aws-connections/{connection_id}/operational-limits")
def update_aws_connection_operational_limits(connection_id: int, payload: AwsConnectionOperationalLimitsUpdate,
                                             session: Session = Depends(get_session)) -> dict:
    connection = connection_or_404(session, connection_id)
    for field, value in payload.model_dump().items():
        setattr(connection, field, value)
    record_event(session, "AWS_CONNECTION_LIMITS_UPDATED",
                 f"API limits updated for AWS connection '{connection.label}': discovery {connection.discovery_requests_per_second}/s, restore polling {connection.restore_poll_requests_per_second}/s with concurrency {connection.restore_poll_concurrency}")
    session.commit()
    return connection_summary(session, connection)["operational_limits"]


@app.get("/api/global-aws-pricing")
def get_global_aws_pricing(session: Session = Depends(get_session)) -> list[dict]:
    regions = active_pricing_regions(session)
    return [global_pricing_summary(session, region) for region in regions]


@app.put("/api/global-aws-pricing/outbound")
def update_global_aws_pricing_outbound(payload: GlobalOutboundCostUpdate,
                                       session: Session = Depends(get_session)) -> dict:
    settings = runtime_settings(session)
    settings.cost_include_aws_transfer_out = payload.include_aws_transfer_out
    record_event(
        session,
        "GLOBAL_AWS_OUTBOUND_COST_UPDATED",
        f"Global AWS outbound cost estimation {'enabled' if payload.include_aws_transfer_out else 'disabled'}",
    )
    session.commit()
    return {"include_aws_transfer_out": settings.cost_include_aws_transfer_out}


@app.post("/api/global-aws-pricing/refresh")
def refresh_global_aws_pricing_now(session: Session = Depends(get_session)) -> dict:
    regions = active_pricing_regions(session)
    if not regions:
        raise HTTPException(status_code=409, detail="No active source region is available for public pricing refresh")
    updated = []
    for region in regions:
        try:
            updated.append(refresh_global_aws_pricing(session, region))
            session.commit()
        except Exception as error:
            session.commit()
            raise HTTPException(status_code=502, detail=f"Public AWS pricing refresh failed for {region}: {type(error).__name__}") from error
    return {"regions": [row.aws_region for row in updated], "refreshed": len(updated)}


@app.post("/api/aws-connections/{connection_id}/sync")
def sync_aws_connection(connection_id: int, session: Session = Depends(get_session)) -> dict:
    """Synchronize non-sensitive connection metadata while preserving its immutable label and identity."""
    connection = connection_or_404(session, connection_id)
    try:
        secret = aws_secret_payload(connection.secret_ocid)
    except Exception as error:
        raise HTTPException(status_code=422, detail=f"AWS connection Secret is no longer compatible: {type(error).__name__}") from error
    if secret["aws_account_id"] != connection.aws_account_id:
        raise HTTPException(status_code=409, detail="Secret AWS account ID differs from the immutable registered connection")
    before = {"default_region": connection.default_region, "control_bucket": connection.control_bucket}
    connection.default_region, connection.control_bucket = secret["default_region"], secret["control_bucket"]
    cached = session.scalar(select(AwsSecretCache).where(AwsSecretCache.secret_ocid == connection.secret_ocid))
    if cached:
        cached.schema_version, cached.connection_name = secret["schema_version"], secret["connection_name"]
        cached.aws_account_id, cached.default_region, cached.valid, cached.validation_error = secret["aws_account_id"], secret["default_region"], True, None
        cached.refreshed_at = utcnow()
    changed = [field for field, old in before.items() if getattr(connection, field) != old]
    record_event(session, "AWS_CONNECTION_SYNCED", f"AWS connection '{connection.label}' synchronized from its current Secret version; changed: {', '.join(changed) or 'none'}")
    session.commit()
    return {"connection": connection_summary(session, connection), "changed": changed, "configuration": aws_connection_configuration(connection, secret)}


@app.delete("/api/aws-connections/{connection_id}")
def delete_aws_connection(connection_id: int, session: Session = Depends(get_session)) -> dict:
    connection = connection_or_404(session, connection_id)
    if session.scalar(select(Source.id).where(Source.aws_connection_id == connection.id)):
        raise HTTPException(status_code=409, detail="An AWS connection with sources must be archived, not deleted")
    session.delete(connection)
    record_event(session, "AWS_CONNECTION_DELETED", f"Unused AWS connection '{connection.label}' deleted")
    session.commit()
    return {"id": connection_id, "deleted": True}


@app.get("/api/sources")
def list_sources(session: Session = Depends(get_session)) -> list[dict]:
    sources = list(session.scalars(select(Source).where(Source.archived_at.is_(None)).order_by(Source.id)))
    source_ids = [s.id for s in sources]
    wave_statuses: dict[int, set[str]] = {}
    if source_ids:
        for source_id, status in session.execute(
            select(Wave.source_id, Wave.status).where(Wave.source_id.in_(source_ids))
        ):
            wave_statuses.setdefault(source_id, set()).add(str(status))
    total_rows = session.execute(
        select(ObjectRecord.source_id, func.count(ObjectRecord.id),
               func.coalesce(func.sum(case((ObjectRecord.state == ObjectState.VERIFIED, 1), else_=0)), 0),
               func.coalesce(func.sum(case((ObjectRecord.state.in_([ObjectState.TRANSFERRED, ObjectState.VERIFIED]), 1), else_=0)), 0),
               func.coalesce(func.sum(case((ObjectRecord.delivery_integrity_status == "OCI_ACCEPTED", 1), else_=0)), 0))
        .where(ObjectRecord.source_id.in_([s.id for s in sources])).group_by(ObjectRecord.source_id)
    ).all() if sources else []
    totals = {source_id: (int(total), int(verified), int(transferred), int(delivery_verified)) for source_id, total, verified, transferred, delivery_verified in total_rows}
    def migration_status(source: Source) -> str:
        total, verified, transferred, delivery_verified = totals.get(source.id, (0, 0, 0, 0))
        if source.destination_validation_status == "DIFFERENT":
            return "DESTINATION_DIVERGENT"
        if source.status == "DISCOVERED" and total and (verified == total or (transferred == total and delivery_verified == total)):
            return "COMPLETED"
        if source.status == "DISCOVERED" and total and transferred == total:
            return "AWAITING_INTEGRITY_VERIFICATION"
        return "IN_PROGRESS" if total else "NOT_STARTED"

    def operational_status(source: Source) -> str:
        """Return one stable, operator-facing state for the source selector."""
        completed = migration_status(source) == "COMPLETED"
        statuses = wave_statuses.get(source.id, set())
        if completed:
            return "COMPLETED"
        if "TRANSFERRING" in statuses:
            return "TRANSFERRING"
        if statuses.intersection({"RESTORING", "RESTORE_REQUESTED", "RESTORE_REQUEST_ACCEPTED"}):
            return "RESTORING"
        if statuses.intersection({"RESTORED", "TRANSFERRED", "VERIFICATION_QUEUED", "VERIFICATION_FAILED"}):
            return "READY_TO_TRANSFER"
        if statuses:
            return "QUEUED"
        if source.status == "DISCOVERING":
            return "DISCOVERING"
        if source.status == "DISCOVERED":
            return "DISCOVERED"
        return "CONFIGURED"
    return [{"id": s.id, "name": s.name, "s3_bucket": s.s3_bucket, "s3_prefix": s.s3_prefix,
             "s3_prefixes": source_prefix_values(s),
             "aws_region": s.aws_region, "destination_bucket": s.destination_bucket, "status": s.status,
             "aws_bucket_region": s.aws_bucket_region,
             "aws_connection_id": s.aws_connection_id,
             "business_priority": int(s.business_priority or 999),
             "aws_connection_label": (s.aws_connection.label if s.aws_connection else
                                      ("Simulated backend" if runtime_context.is_simulation else "Unassigned AWS connection")),
             "discovery_requested_at": s.discovery_requested_at, "discovery_completed_at": s.discovery_completed_at,
             "discovery_error": s.discovery_error, "discovery_pages_completed": s.discovery_pages_completed,
             "discovery_objects_inserted": s.discovery_objects_inserted,
             "last_discovery_mode": s.last_discovery_mode, "discovery_generation": s.discovery_generation,
             "discovery_can_resume": bool(s.discovery_continuation_token), "archived_at": s.archived_at,
             "destination_validation": {"status": s.destination_validation_status, "at": s.destination_validation_at,
                                        "missing": s.destination_missing_count, "size_mismatches": s.destination_size_mismatch_count},
             "migration_status": migration_status(s), "operational_status": operational_status(s),
             "can_delete": not source_has_executed_wave(session, s.id),
             "scope_conflicts": ([] if runtime_context.is_simulation else
                                 active_source_scope_conflicts(session, s.s3_bucket, source_prefix_values(s), s.id))}
            for s in sources]


@app.post("/api/sources", status_code=201)
def create_source(payload: SourceCreate, session: Session = Depends(get_session)) -> dict:
    if session.scalar(select(Source).where(Source.name == payload.name)):
        raise HTTPException(status_code=409, detail="Source name already exists")
    if payload.aws_connection_id is None:
        raise HTTPException(status_code=422, detail="Select an AWS connection before creating a source")
    if not session.scalar(select(OciBucketCache.id).where(OciBucketCache.name == payload.destination_bucket)):
        raise HTTPException(status_code=422, detail="Choose a destination bucket from the OCI cache; refresh it in Settings first")
    if payload.aws_connection_id is not None:
        connection = connection_or_404(session, payload.aws_connection_id)
        if connection.archived_at:
            raise HTTPException(status_code=409, detail="Archived AWS connections cannot be used by new sources")
        if payload.aws_region != connection.default_region:
            raise HTTPException(status_code=422, detail="Source AWS region is defined by the selected AWS connection; create another connection for a different region")
    prefixes = normalize_source_prefixes(payload.s3_prefixes, payload.s3_prefix)
    conflicts = active_source_scope_conflicts(session, payload.s3_bucket, prefixes)
    if conflicts:
        names = ", ".join(sorted({item["source_name"] for item in conflicts}))
        raise HTTPException(status_code=409, detail=(
            f"S3 prefix scope overlaps active source(s): {names}. Archive the conflicting source or remove the overlapping prefix."
        ))
    source_data = payload.model_dump(exclude={"s3_prefixes"})
    source_data["s3_prefix"] = prefixes[0]
    source = Source(**source_data)
    session.add(source)
    session.flush()
    session.add_all(SourcePrefix(source_id=source.id, prefix=prefix) for prefix in prefixes)
    record_event(session, "SOURCE_CREATED", f"Source '{source.name}' configured", source_id=source.id)
    session.commit()
    return {"id": source.id, "name": source.name, "status": source.status}


@app.put("/api/sources/{source_id}")
def update_source(source_id: int, payload: SourceUpdate, session: Session = Depends(get_session)) -> dict:
    source = active_source_or_409(session, source_id)
    objects = session.scalar(select(func.count(ObjectRecord.id)).where(ObjectRecord.source_id == source_id)) or 0
    waves = session.scalar(select(func.count(Wave.id)).where(Wave.source_id == source_id)) or 0
    if objects or waves:
        raise HTTPException(status_code=409, detail="A source with inventory or waves is immutable to preserve auditability")
    duplicate = session.scalar(select(Source.id).where(Source.name == payload.name, Source.id != source_id))
    if duplicate:
        raise HTTPException(status_code=409, detail="Source name already exists")
    if not session.scalar(select(OciBucketCache.id).where(OciBucketCache.name == payload.destination_bucket)):
        raise HTTPException(status_code=422, detail="Choose a destination bucket from the OCI cache; refresh it in Settings first")
    if payload.aws_connection_id is not None:
        connection = connection_or_404(session, payload.aws_connection_id)
        if connection.archived_at:
            raise HTTPException(status_code=409, detail="Archived AWS connections cannot be used by sources")
        if payload.aws_region != connection.default_region:
            raise HTTPException(status_code=422, detail="Source AWS region is defined by the selected AWS connection; create another connection for a different region")
    prefixes = normalize_source_prefixes(payload.s3_prefixes, payload.s3_prefix)
    conflicts = active_source_scope_conflicts(session, payload.s3_bucket, prefixes, source_id)
    if conflicts:
        names = ", ".join(sorted({item["source_name"] for item in conflicts}))
        raise HTTPException(status_code=409, detail=(
            f"S3 prefix scope overlaps active source(s): {names}. Archive the conflicting source or remove the overlapping prefix."
        ))
    for field, value in payload.model_dump(exclude={"s3_prefixes"}).items():
        setattr(source, field, value)
    source.s3_prefix = prefixes[0]
    session.query(SourcePrefix).filter(SourcePrefix.source_id == source.id).delete(synchronize_session=False)
    session.add_all(SourcePrefix(source_id=source.id, prefix=prefix) for prefix in prefixes)
    record_event(session, "SOURCE_UPDATED", f"Source '{source.name}' configuration updated", source_id=source.id)
    session.commit()
    return {"id": source.id, "name": source.name, "status": source.status}


@app.post("/api/sources/{source_id}/migrate-aws-connection")
def migrate_legacy_source_connection(source_id: int, payload: LegacySourceConnectionMigration, session: Session = Depends(get_session)) -> dict:
    """Adopt a legacy source into an equivalent immutable AWS connection."""
    source = active_source_or_409(session, source_id)
    if source.aws_connection_id is not None:
        raise HTTPException(status_code=409, detail="This source already uses an AWS connection")
    running = session.scalar(
        select(func.count(Task.id)).join(Wave).where(Wave.source_id == source.id, Task.state == TaskState.RUNNING)
    ) or 0
    if running:
        raise HTTPException(status_code=409, detail="Pause or wait for running tasks before migrating this source")
    connection = connection_or_404(session, payload.aws_connection_id)
    if connection.archived_at:
        raise HTTPException(status_code=409, detail="Archived AWS connections cannot be used by sources")
    if source.aws_region != connection.default_region:
        raise HTTPException(status_code=422, detail="Source AWS region must match the AWS connection")
    settings = runtime_settings(session)
    try:
        values = aws_secret_payload(connection.secret_ocid)
    except Exception as error:
        raise HTTPException(status_code=422, detail=f"AWS connection Secret is no longer compatible: {type(error).__name__}") from error
    if not (
        values["migration_role_arn"] == settings.aws_migration_role_arn.strip()
        and values["batch_operations_role_arn"] == settings.aws_batch_role_arn.strip()
        and values["control_bucket"] == settings.aws_control_bucket.strip()
    ):
        raise HTTPException(status_code=422, detail="AWS connection must match the legacy roles and control bucket for audited source migration")
    source.aws_connection_id = connection.id
    record_event(session, "SOURCE_AWS_CONNECTION_MIGRATED", f"Source '{source.name}' adopted AWS connection '{connection.label}' without changing inventory or waves", source_id=source.id)
    # Archived test/history sources never execute again and must not keep a
    # retired credential path alive for active operations.
    remaining_legacy = session.scalar(select(func.count(Source.id)).where(
        Source.id != source.id, Source.aws_connection_id.is_(None), Source.archived_at.is_(None)
    )) or 0
    legacy_cleared = False
    if not remaining_legacy:
        settings.aws_migration_role_arn = ""
        settings.aws_batch_role_arn = ""
        settings.aws_control_bucket = ""
        settings.aws_control_prefix = ""
        legacy_cleared = True
        record_event(session, "LEGACY_AWS_CONFIGURATION_CLEARED", "All sources use immutable AWS connections; legacy runtime AWS configuration cleared")
    session.commit()
    return {"id": source.id, "name": source.name, "aws_connection_id": connection.id, "aws_connection_label": connection.label, "legacy_configuration_cleared": legacy_cleared}


@app.post("/api/legacy-aws/retire")
def retire_legacy_aws_configuration(session: Session = Depends(get_session)) -> dict:
    """Erase retired global AWS settings after every active source is connected."""
    legacy_sources = session.scalar(select(func.count(Source.id)).where(
        Source.aws_connection_id.is_(None), Source.archived_at.is_(None)
    )) or 0
    if legacy_sources:
        raise HTTPException(status_code=409, detail="Migrate or archive every active legacy source before retiring global AWS configuration")
    running = session.scalar(select(func.count(Task.id)).where(Task.state == TaskState.RUNNING)) or 0
    if running:
        raise HTTPException(status_code=409, detail="Pause or wait for running tasks before retiring global AWS configuration")
    settings = runtime_settings(session)
    settings.aws_migration_role_arn = ""
    settings.aws_batch_role_arn = ""
    settings.aws_control_bucket = ""
    settings.aws_control_prefix = ""
    record_event(session, "LEGACY_AWS_CONFIGURATION_CLEARED", "Global AWS roles, control bucket and prefix cleared after connection migration")
    session.commit()
    return {"retired": True}


def source_has_executed_wave(session: Session, source_id: int) -> bool:
    """A queued or drafted wave is removable; a claimed wave is audit data."""
    task_was_claimed = session.scalar(
        select(func.count(Task.id)).join(Wave).where(Wave.source_id == source_id, Task.attempts > 0)
    ) or 0
    progressed_object = session.scalar(select(func.count(ObjectRecord.id)).where(
        ObjectRecord.source_id == source_id,
        ObjectRecord.state.not_in([ObjectState.DISCOVERED, ObjectState.WAVE_ASSIGNED]),
    )) or 0
    submitted_batch = session.scalar(select(func.count(Wave.id)).where(
        Wave.source_id == source_id, Wave.batch_job_id.is_not(None)
    )) or 0
    return bool(task_was_claimed or progressed_object or submitted_batch)


def active_source_or_409(session: Session, source_id: int) -> Source:
    source = source_or_404(session, source_id)
    if source.archived_at:
        raise HTTPException(status_code=409, detail="Archived sources are read-only")
    return source


def delete_unexecuted_source_data(session: Session, source_id: int) -> dict[str, int]:
    """Delete an unexecuted source in foreign-key-safe dependency order.

    A source that has never executed a wave is a preview/plan, not audit
    evidence.  It is therefore removable together with its durable discovery
    queue entries, planned waves and local observations.  The explicit order
    also makes this safe for PostgreSQL installations that enforce every FK.
    """
    wave_ids = list(session.scalars(select(Wave.id).where(Wave.source_id == source_id)))
    attempt_ids = list(session.scalars(select(RestoreAttempt.id).where(RestoreAttempt.wave_id.in_(wave_ids)))) if wave_ids else []
    deleted: dict[str, int] = {}
    deleted["events"] = session.query(Event).filter(
        (Event.source_id == source_id) | (Event.wave_id.in_(wave_ids) if wave_ids else False)
    ).delete(synchronize_session=False)
    if attempt_ids:
        deleted["restore_results"] = session.query(RestoreObjectResult).filter(RestoreObjectResult.attempt_id.in_(attempt_ids)).delete(synchronize_session=False)
        deleted["restore_attempts"] = session.query(RestoreAttempt).filter(RestoreAttempt.id.in_(attempt_ids)).delete(synchronize_session=False)
    else:
        deleted["restore_results"], deleted["restore_attempts"] = 0, 0
    deleted["tasks"] = session.query(Task).filter(Task.wave_id.in_(wave_ids)).delete(synchronize_session=False) if wave_ids else 0
    # Object-level transfer work is durable as well.  It must be removed
    # before inventory objects/waves, otherwise PostgreSQL correctly blocks
    # deletion through the queue-item foreign keys.
    deleted["transfer_queue_items"] = session.query(TransferQueueItem).filter(
        TransferQueueItem.source_id == source_id
    ).delete(synchronize_session=False)
    # Changes may point at a successor object revision, so they must be
    # removed before the inventory objects themselves.
    deleted["discovery_changes"] = session.query(DiscoveryChange).filter(DiscoveryChange.source_id == source_id).delete(synchronize_session=False)
    deleted["objects"] = session.query(ObjectRecord).filter(ObjectRecord.source_id == source_id).delete(synchronize_session=False)
    deleted["waves"] = session.query(Wave).filter(Wave.source_id == source_id).delete(synchronize_session=False)
    deleted["pipeline_runs"] = session.query(DynamicPipelineRun).filter(DynamicPipelineRun.source_id == source_id).delete(synchronize_session=False)
    deleted["discovery_jobs"] = session.query(DiscoveryJob).filter(DiscoveryJob.source_id == source_id).delete(synchronize_session=False)
    return deleted


def cancel_active_wave_tasks(session: Session, wave: Wave, reason: str) -> int:
    """Durably remove pending work for an operator-paused wave.

    A wave state alone is not enough: queued tasks survive restarts and used
    to remain visible after a source had been archived.  Cancellation keeps
    the immutable task record while making it ineligible for every worker.
    """
    tasks = list(session.scalars(select(Task).where(
        Task.wave_id == wave.id,
        Task.state.in_([TaskState.READY, TaskState.RUNNING]),
    )))
    for task in tasks:
        task.state = TaskState.CANCELLED
        task.worker_id = None
        task.lease_expires_at = None
        task.error = f"Cancelled: {reason}"
    # The continuous lane is object-scoped.  Ready entries must be cancelled
    # with the wave or a later source dispatcher could still claim them after
    # the wave disappeared from the operator's queue.  A LEASED object is
    # allowed to reach its safe object boundary; the worker sees PAUSED before
    # claiming the next batch.
    items = list(session.scalars(select(TransferQueueItem).where(
        TransferQueueItem.wave_id == wave.id,
        TransferQueueItem.state.in_([
            TransferQueueState.AVAILABLE, TransferQueueState.READY,
            TransferQueueState.RETRY_WAIT, TransferQueueState.MULTIPART_RESUME,
        ]),
    )))
    for item in items:
        item.state = TransferQueueState.CANCELLED
        item.lease_token = item.lease_owner = None
        item.lease_expires_at = None
        item.decision_reason = f"Cancelled: {reason}"
    return len(tasks) + len(items)


def deactivate_archived_source_work(session: Session, source: Source, reason: str) -> int:
    """Stop all executable control-plane work owned by an archived source."""
    cancelled = 0
    terminal = {"COMPLETED", "VERIFIED", "TRANSFERRED", "TRANSFERRED_WITH_ERRORS", "VERIFICATION_FAILED"}
    for wave in session.scalars(select(Wave).where(Wave.source_id == source.id)):
        cancelled += cancel_active_wave_tasks(session, wave, reason)
        if wave.status not in terminal:
            wave.status = "PAUSED"
    for job in session.scalars(select(DiscoveryJob).where(
        DiscoveryJob.source_id == source.id,
        DiscoveryJob.state.in_([TaskState.READY, TaskState.RUNNING]),
    )):
        job.state, job.worker_id, job.lease_expires_at = TaskState.CANCELLED, None, None
        job.error = f"Cancelled: {reason}"
        cancelled += 1
    for run in session.scalars(select(DynamicPipelineRun).where(
        DynamicPipelineRun.source_id == source.id,
        DynamicPipelineRun.status.not_in(["COMPLETED", "HISTORICAL"]),
    )):
        run.status = "HISTORICAL"
        run.scheduled_restores = False
        run.completed_at = run.completed_at or utcnow()
    return cancelled


def reconcile_archived_source_work(session: Session) -> int:
    """Backstop for sources archived by an earlier Raijin release."""
    cancelled = 0
    for source in session.scalars(select(Source).where(Source.archived_at.is_not(None))):
        cancelled += deactivate_archived_source_work(session, source, "source is archived")
    return cancelled


@app.delete("/api/sources/{source_id}")
def delete_source(source_id: int, session: Session = Depends(get_session)) -> dict:
    source = source_or_404(session, source_id)
    if source_has_executed_wave(session, source_id):
        raise HTTPException(status_code=409, detail="This source has an executed wave and must be archived, not deleted")
    try:
        deleted = delete_unexecuted_source_data(session, source_id)
        session.delete(source)
        session.commit()
    except IntegrityError as error:
        session.rollback()
        # Do not expose a raw database exception to the browser.  The source
        # remains untouched because the transaction was rolled back.
        raise HTTPException(status_code=409, detail="The source could not be deleted because it still has protected operational records. Refresh the source and archive it if a wave was executed.") from error
    return {"id": source_id, "deleted": True, "removed": deleted}


@app.post("/api/sources/{source_id}/archive")
def archive_source(source_id: int, session: Session = Depends(get_session)) -> dict:
    source = source_or_404(session, source_id)
    if not source_has_executed_wave(session, source_id):
        raise HTTPException(status_code=409, detail="A source without an executed wave must be deleted, not archived")
    cancelled = deactivate_archived_source_work(session, source, "source archived by operator")
    source.archived_at, source.status = utcnow(), "ARCHIVED"
    record_event(session, "SOURCE_ARCHIVED", f"Source '{source.name}' archived; {cancelled} pending task(s) cancelled and historical data retained", source_id=source.id)
    session.commit()
    return {"id": source.id, "status": source.status, "archived_at": source.archived_at}


@app.post("/api/sources/{source_id}/inventory/import", status_code=201)
def import_inventory(source_id: int, payload: InventoryImport, session: Session = Depends(get_session)) -> dict:
    source = active_source_or_409(session, source_id)
    require_non_overlapping_source_scope(session, source)
    inserted = skipped_out_of_scope = 0
    for item in payload.items:
        if not source_key_in_scope(source, item.object_key):
            skipped_out_of_scope += 1
            continue
        duplicate = session.scalar(select(ObjectRecord.id).where(
            ObjectRecord.source_id == source_id,
            ObjectRecord.object_key == item.object_key,
            ObjectRecord.version_id == item.version_id,
        ))
        if duplicate:
            continue
        session.add(ObjectRecord(source_id=source_id, **item.model_dump()))
        inserted += 1
    duplicates = len(payload.items) - inserted - skipped_out_of_scope
    record_event(session, "INVENTORY_IMPORTED", f"Imported {inserted} inventory record(s); skipped {duplicates} duplicate(s) and {skipped_out_of_scope} out-of-scope record(s)", source_id=source_id)
    session.commit()
    return {"inserted": inserted, "skipped_duplicates": duplicates, "skipped_out_of_scope": skipped_out_of_scope}


def inventory_file_value(row: dict[str, str], *names: str) -> str | None:
    """Read an inventory column case-insensitively, including S3 CSV names."""
    normalized = {str(key).strip().lower().replace("_", ""): value for key, value in row.items() if key}
    for name in names:
        value = normalized.get(name.lower().replace("_", ""))
        if value is not None and str(value).strip() != "":
            return str(value).strip()
    return None


@app.post("/api/sources/{source_id}/inventory/upload", status_code=201)
def upload_inventory_file(source_id: int, inventory_file: UploadFile = File(...), rediscovery: bool = Query(False),
                          justification: str | None = Form(None), session: Session = Depends(get_session)) -> dict:
    """Import a scalable CSV inventory without calling AWS.

    The source must still be pristine.  This avoids a potentially ambiguous
    merge between a supplied inventory and an AWS discovery, and lets a large
    file be parsed in bounded batches directly on the VM.
    """
    source = active_source_or_409(session, source_id)
    require_non_overlapping_source_scope(session, source)
    if not rediscovery and session.scalar(select(Wave.id).where(Wave.source_id == source_id)):
        raise HTTPException(status_code=409, detail="Inventory file import is immutable after waves are created")
    if not rediscovery and session.scalar(select(ObjectRecord.id).where(ObjectRecord.source_id == source_id).limit(1)):
        raise HTTPException(status_code=409, detail="This source already has inventory records; create a new source or delete the unexecuted discovery first")
    if rediscovery and len((justification or "").strip()) <= 10:
        raise HTTPException(status_code=422, detail="Rediscovery justification must contain more than 10 characters")
    filename = inventory_file.filename or "inventory.csv"
    if not filename.lower().endswith((".csv", ".csv.gz", ".gz")):
        raise HTTPException(status_code=422, detail="Inventory file must be CSV UTF-8 or CSV.GZ")
    started = utcnow()
    raw = inventory_file.file
    binary = gzip.GzipFile(fileobj=raw, mode="rb") if filename.lower().endswith((".csv.gz", ".gz")) else raw
    try:
        reader = csv.DictReader(io.TextIOWrapper(binary, encoding="utf-8-sig", newline=""))
        if not reader.fieldnames:
            raise HTTPException(status_code=422, detail="Inventory CSV must contain a header row")
        pending: list[dict] = []
        inserted = 0
        for line_number, row in enumerate(reader, start=2):
            key = inventory_file_value(row, "object_key", "key")
            size = inventory_file_value(row, "size_bytes", "size")
            if not key or size is None:
                raise HTTPException(status_code=422, detail=f"Inventory row {line_number} requires object_key/Key and size_bytes/Size")
            if not source_key_in_scope(source, key):
                continue
            try:
                size_bytes = int(size)
            except ValueError as error:
                raise HTTPException(status_code=422, detail=f"Inventory row {line_number} has invalid size") from error
            if size_bytes < 0:
                raise HTTPException(status_code=422, detail=f"Inventory row {line_number} has negative size")
            last_modified = inventory_file_value(row, "last_modified", "lastmodifieddate")
            try:
                parsed_last_modified = datetime.fromisoformat(last_modified.replace("Z", "+00:00")) if last_modified else None
            except ValueError as error:
                raise HTTPException(status_code=422, detail=f"Inventory row {line_number} has invalid last_modified") from error
            pending.append({
                "source_id": source_id, "object_key": key, "size_bytes": size_bytes,
                "version_id": inventory_file_value(row, "version_id", "versionid"),
                "etag": inventory_file_value(row, "etag"),
                "storage_class": inventory_file_value(row, "storage_class", "storageclass"),
                "last_modified": parsed_last_modified,
                "metadata_json": inventory_file_value(row, "metadata_json") or "{}",
                "tags_json": inventory_file_value(row, "tags_json") or "{}",
                "source_checksum": inventory_file_value(row, "source_checksum", "checksum"),
                "checksum_algorithm": inventory_file_value(row, "checksum_algorithm") or None,
                "state": ObjectState.DISCOVERED,
            })
            if len(pending) >= 5000:
                if rediscovery:
                    added, _updated, _changed = merge_discovery_rows(session, source, pending)
                    inserted += added
                else:
                    session.bulk_insert_mappings(ObjectRecord, pending)
                    inserted += len(pending)
                pending.clear()
        if pending:
            if rediscovery:
                added, _updated, _changed = merge_discovery_rows(session, source, pending)
                inserted += added
            else:
                session.bulk_insert_mappings(ObjectRecord, pending)
                inserted += len(pending)
        if not inserted and not rediscovery:
            raise HTTPException(status_code=422, detail="Inventory CSV has no object records")
        source.status = "DISCOVERED"
        source.discovery_requested_at = started
        source.discovery_started_at = None
        source.discovery_completed_at = utcnow()
        source.discovery_elapsed_seconds = max(0, (source.discovery_completed_at - started).total_seconds())
        source.discovery_error = None
        source.discovery_continuation_token = None
        source.discovery_pages_completed = 0
        source.discovery_objects_inserted = inserted
        source.last_discovery_mode = "INVENTORY_FILE"
        source.discovery_generation = int(source.discovery_generation or 0) + 1
        operation = "Rediscovery merged" if rediscovery else "Imported"
        record_event(session, "INVENTORY_FILE_REDISCOVERED" if rediscovery else "INVENTORY_FILE_IMPORTED", f"{operation} {inserted} new record(s) from inventory file '{filename}' without AWS discovery" + (f"; justification: {(justification or '').strip()}" if rediscovery else ""), source_id=source_id)
        session.commit()
        return {"source_id": source.id, "status": source.status, "inserted": inserted, "filename": filename, "rediscovery": rediscovery}
    except HTTPException:
        session.rollback()
        raise
    except (OSError, UnicodeDecodeError, csv.Error) as error:
        session.rollback()
        raise HTTPException(status_code=422, detail=f"Could not read inventory CSV: {type(error).__name__}") from error
    finally:
        inventory_file.file.close()


def queue_discovery(source: Source, session: Session, *, mode: str = "REMOTE_LIST", manifest_uri: str | None = None,
                    rediscovery: bool = False, justification: str | None = None) -> DiscoveryJob:
    require_non_overlapping_source_scope(session, source)
    if source.status == "DISCOVERING":
        raise HTTPException(status_code=409, detail="Discovery already running for this source")
    if not rediscovery and session.scalar(select(func.count(Wave.id)).where(Wave.source_id == source.id)):
        raise HTTPException(status_code=409, detail="Discovery is immutable after waves are created")
    if rediscovery and len((justification or "").strip()) <= 10:
        raise HTTPException(status_code=422, detail="Rediscovery justification must contain more than 10 characters")
    # A multi-prefix scan can have completed a prefix cleanly before a later
    # prefix fails; in that case the S3 token is empty but the durable prefix
    # cursor still identifies an exact safe resume point.
    resume = (not rediscovery and mode == "REMOTE_LIST" and source.status == "DISCOVERY_FAILED"
              and bool(source.discovery_continuation_token or source.discovery_prefix_index))
    # A remote discovery has one exact S3 continuation cursor.  Mixing it with
    # a previous inventory-file import (or silently starting over on completed
    # rows) makes the origin non-auditable and forces expensive duplicate
    # checks.  A failed remote discovery is the only valid resume case.
    if not rediscovery and not resume and session.scalar(select(ObjectRecord.id).where(ObjectRecord.source_id == source.id).limit(1)):
        raise HTTPException(status_code=409, detail="Remote discovery cannot replace or merge an existing inventory; create a new source or resume the failed remote discovery")
    existing_job = session.scalar(select(DiscoveryJob).where(
        DiscoveryJob.source_id == source.id,
        DiscoveryJob.state.in_([TaskState.READY, TaskState.RUNNING]),
    ).order_by(DiscoveryJob.id.desc()).limit(1))
    if existing_job:
        raise HTTPException(status_code=409, detail=f"Discovery job {existing_job.id} is already queued or running for this source")
    source.status = "DISCOVERY_QUEUED"
    source.discovery_requested_at = utcnow()
    source.discovery_completed_at = None
    source.discovery_error = None
    if not resume:
        source.discovery_continuation_token = None
        source.discovery_prefix_index = 0
        source.discovery_pages_completed = 0
        source.discovery_objects_inserted = 0
        source.discovery_started_at = None
        source.discovery_elapsed_seconds = 0
    job = DiscoveryJob(source_id=source.id, mode=mode, inventory_manifest_uri=manifest_uri,
                       is_rediscovery=rediscovery, justification=(justification or "").strip() or None)
    session.add(job)
    label = "Rediscovery" if rediscovery else "Discovery"
    record_event(session, "REDISCOVERY_QUEUED" if rediscovery else "DISCOVERY_QUEUED",
                 f"{label} job queued ({mode}) {'from its durable checkpoint' if resume else ''} for source '{source.name}'" + (f"; justification: {job.justification}" if rediscovery else ""), source_id=source.id)
    session.commit()
    return job


@app.post("/api/sources/{source_id}/discovery")
def request_discovery(source_id: int, session: Session = Depends(get_session)) -> dict:
    source = active_source_or_409(session, source_id)
    job = queue_discovery(source, session)
    return {"source_id": source.id, "status": source.status, "job_id": job.id, "mode": job.mode}


@app.post("/api/sources/{source_id}/rediscovery")
def request_rediscovery(source_id: int, payload: RediscoveryRequest, session: Session = Depends(get_session)) -> dict:
    source = active_source_or_409(session, source_id)
    job = queue_discovery(source, session, rediscovery=True, justification=payload.justification)
    return {"source_id": source.id, "status": source.status, "job_id": job.id, "mode": job.mode, "rediscovery": True}


@app.post("/api/sources/{source_id}/inventory/manifest")
def request_inventory_manifest_import(source_id: int, payload: InventoryManifestImport,
                                      session: Session = Depends(get_session)) -> dict:
    """Queue direct import of a S3 Inventory manifest and all of its shards."""
    source = active_source_or_409(session, source_id)
    parsed = urlparse(payload.manifest_uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
        raise HTTPException(status_code=422, detail="Manifest URI must use s3://bucket/key")
    job = queue_discovery(source, session, mode="S3_INVENTORY_MANIFEST", manifest_uri=payload.manifest_uri)
    return {"source_id": source.id, "status": source.status, "job_id": job.id, "mode": job.mode}


@app.post("/api/sources/{source_id}/rediscovery/inventory/manifest")
def request_inventory_manifest_rediscovery(source_id: int, payload: RediscoveryManifestImport,
                                            session: Session = Depends(get_session)) -> dict:
    source = active_source_or_409(session, source_id)
    parsed = urlparse(payload.manifest_uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
        raise HTTPException(status_code=422, detail="Manifest URI must use s3://bucket/key")
    job = queue_discovery(source, session, mode="S3_INVENTORY_MANIFEST", manifest_uri=payload.manifest_uri,
                          rediscovery=True, justification=payload.justification)
    return {"source_id": source.id, "status": source.status, "job_id": job.id, "mode": job.mode, "rediscovery": True}


@app.get("/api/sources/{source_id}/summary")
def source_summary(source_id: int, session: Session = Depends(get_session)) -> dict:
    source_or_404(session, source_id)
    current = ObjectRecord.is_current_revision.is_(True)
    count, bytes_total = session.execute(select(func.count(ObjectRecord.id), func.coalesce(func.sum(ObjectRecord.size_bytes), 0)).where(ObjectRecord.source_id == source_id, current)).one()
    states = dict(session.execute(
        select(ObjectRecord.state, func.count(ObjectRecord.id))
        .where(ObjectRecord.source_id == source_id, current)
        .group_by(ObjectRecord.state)
    ).all())
    source = source_or_404(session, source_id)
    change_counts = dict(session.execute(
        select(DiscoveryChange.change_type, func.count(DiscoveryChange.id))
        .where(DiscoveryChange.source_id == source_id).group_by(DiscoveryChange.change_type)
    ).all())
    newest_pending_change = session.scalar(select(func.max(DiscoveryChange.detected_at)).where(
        DiscoveryChange.source_id == source_id, DiscoveryChange.change_type == "MODIFIED",
        DiscoveryChange.reprocessed_object_id.is_(None)
    ))
    pending_modified_changes = session.scalar(select(func.count(DiscoveryChange.id)).where(
        DiscoveryChange.source_id == source_id, DiscoveryChange.change_type == "MODIFIED",
        DiscoveryChange.reprocessed_object_id.is_(None)
    )) or 0
    # This deliberately excludes time spent waiting in the durable queue.  If a
    # discovery is currently being processed, include only its active slice.
    discovery_duration_seconds = float(source.discovery_elapsed_seconds or 0)
    if source.status == "DISCOVERING" and source.discovery_started_at:
        discovery_duration_seconds += max(0, (utcnow() - source.discovery_started_at).total_seconds())
    delivery_verified = session.scalar(select(func.count(ObjectRecord.id)).where(ObjectRecord.source_id == source_id, ObjectRecord.delivery_integrity_status == "OCI_ACCEPTED")) or 0
    all_transferred = (states.get(ObjectState.TRANSFERRED, 0) + states.get(ObjectState.VERIFIED, 0)) == count
    migration_status = "DESTINATION_DIVERGENT" if source.destination_validation_status == "DIFFERENT" else "COMPLETED" if source.status == "DISCOVERED" and count and (states.get(ObjectState.VERIFIED, 0) == count or (all_transferred and delivery_verified == count)) else "AWAITING_INTEGRITY_VERIFICATION" if source.status == "DISCOVERED" and count and all_transferred else "IN_PROGRESS" if count else "NOT_STARTED"
    return {"source_id": source_id, "objects": count, "bytes": bytes_total, "object_states": states, "migration_status": migration_status,
            "destination_validation": {"status": source.destination_validation_status, "at": source.destination_validation_at,
                                       "missing": source.destination_missing_count, "size_mismatches": source.destination_size_mismatch_count,
                                       "metadata_mismatches": source.destination_metadata_mismatch_count, "extras": source.destination_extra_count,
                                       "current_for_modified_changes": bool(newest_pending_change and source.destination_validation_at and source.destination_validation_at >= newest_pending_change)},
            "discovery": {"status": source.status, "requested_at": source.discovery_requested_at,
                          "completed_at": source.discovery_completed_at, "error": source.discovery_error,
                          "duration_seconds": int(discovery_duration_seconds),
                          "pages_completed": source.discovery_pages_completed,
                          "objects_inserted": source.discovery_objects_inserted,
                          "mode": source.last_discovery_mode, "generation": source.discovery_generation,
                          "changes": change_counts, "pending_modified_changes": int(pending_modified_changes),
                          "objects_per_second": round(count / discovery_duration_seconds, 2) if discovery_duration_seconds else 0,
                          "pages_per_minute": round(source.discovery_pages_completed * 60 / discovery_duration_seconds, 2) if discovery_duration_seconds else 0,
                          "can_resume": bool(source.discovery_continuation_token)}}


@app.get("/api/sources/{source_id}/discovery-changes")
def source_discovery_changes(source_id: int, limit: int = 100, session: Session = Depends(get_session)) -> list[dict]:
    """Evidence of source updates detected without rewriting wave history."""
    source_or_404(session, source_id)
    rows = session.scalars(select(DiscoveryChange).where(DiscoveryChange.source_id == source_id)
                           .order_by(DiscoveryChange.detected_at.desc(), DiscoveryChange.id.desc()).limit(min(max(limit, 1), 1000)))
    return [{"id": row.id, "object_key": row.object_key, "type": row.change_type,
             "previous_size_bytes": row.previous_size_bytes, "current_size_bytes": row.current_size_bytes,
             "previous_etag": row.previous_etag, "current_etag": row.current_etag,
             "previous_version_id": row.previous_version_id, "current_version_id": row.current_version_id,
             "previous_last_modified": row.previous_last_modified, "current_last_modified": row.current_last_modified,
             "detected_at": row.detected_at, "discovery_job_id": row.discovery_job_id,
             "reprocessed_object_id": row.reprocessed_object_id, "reprocessed_at": row.reprocessed_at} for row in rows]


@app.post("/api/sources/{source_id}/discovery-changes/reprocess-modified")
def reprocess_modified_discovery_objects(source_id: int, session: Session = Depends(get_session)) -> dict:
    """Create eligible successor records without mutating migrated evidence.

    The operator must first reconcile OCI after the latest rediscovery.  A
    clean reconciliation proves the original version remains auditable before
    the new source revision is scheduled to overwrite that destination key.
    """
    source = active_source_or_409(session, source_id)
    changes = list(session.scalars(select(DiscoveryChange).where(
        DiscoveryChange.source_id == source.id, DiscoveryChange.change_type == "MODIFIED",
        DiscoveryChange.reprocessed_object_id.is_(None)
    ).order_by(DiscoveryChange.detected_at, DiscoveryChange.id)))
    if not changes:
        raise HTTPException(status_code=409, detail="No modified source objects are awaiting reprocessing")
    latest_change = max(change.detected_at for change in changes)
    if source.destination_validation_status != "VALID" or not source.destination_validation_at or source.destination_validation_at < latest_change:
        raise HTTPException(status_code=409, detail="Run Validate OCI destination after the latest rediscovery and resolve any divergence before reprocessing modified objects")
    created = 0
    for change in changes:
        prior = session.scalar(select(ObjectRecord).where(
            ObjectRecord.source_id == source.id, ObjectRecord.object_key == change.object_key,
            ObjectRecord.is_current_revision.is_(True)
        ).order_by(ObjectRecord.id.desc()).limit(1))
        if not prior:
            continue
        # The predecessor is immutable historical evidence.  Its linked wave,
        # integrity data and timestamps are deliberately retained.
        prior.is_current_revision, prior.superseded_at = False, utcnow()
        successor = ObjectRecord(
            source_id=source.id, previous_object_id=prior.id, object_key=prior.object_key,
            version_id=change.current_version_id, size_bytes=change.current_size_bytes,
            etag=change.current_etag, storage_class=prior.storage_class,
            last_modified=change.current_last_modified, metadata_json=prior.metadata_json,
            tags_json=prior.tags_json, state=ObjectState.DISCOVERED,
        )
        session.add(successor)
        session.flush()
        change.reprocessed_object_id, change.reprocessed_at = successor.id, utcnow()
        created += 1
    if not created:
        raise HTTPException(status_code=409, detail="No active source revision could be prepared for reprocessing")
    record_event(session, "MODIFIED_SOURCE_REPROCESS_QUEUED",
                 f"Prepared {created} changed source object revision(s) for a new wave after successful OCI destination validation",
                 source_id=source.id)
    session.commit()
    return {"source_id": source.id, "created": created, "status": "DISCOVERED"}


@app.get("/api/sources/{source_id}/report")
def source_report(source_id: int, session: Session = Depends(get_session)) -> dict:
    """Consolidated local report; never contacts AWS or OCI."""
    source = source_or_404(session, source_id)
    waves = list(session.scalars(select(Wave).where(Wave.source_id == source.id).order_by(Wave.id)))
    items = []
    for wave in waves:
        total, bytes_total, transferred, transferred_bytes, failed = session.execute(select(
            func.count(ObjectRecord.id), func.coalesce(func.sum(ObjectRecord.size_bytes), 0),
            func.coalesce(func.sum(case((ObjectRecord.state.in_([ObjectState.TRANSFERRED, ObjectState.VERIFIED]), 1), else_=0)), 0),
            func.coalesce(func.sum(case((ObjectRecord.state.in_([ObjectState.TRANSFERRED, ObjectState.VERIFIED]), ObjectRecord.size_bytes), else_=0)), 0),
            func.coalesce(func.sum(case((ObjectRecord.state == ObjectState.FAILED, 1), else_=0)), 0),
        ).where(ObjectRecord.wave_id == wave.id)).one()
        timing = restore_timing(session, wave.id)
        items.append({"wave_id": wave.id, "wave_name": wave.name, "status": wave.status,
                      "objects": int(total), "bytes": int(bytes_total), "transferred_objects": int(transferred),
                      "transferred_bytes": int(transferred_bytes), "failed_objects": int(failed),
                      "restore": timing, "predicted_transfer_seconds": int(wave.predicted_transfer_seconds or 0),
                      "planned_restore_at": wave.planned_restore_at, "planned_transfer_start_at": wave.planned_transfer_start_at})
    return {"source": {"id": source.id, "name": source.name, "s3_bucket": source.s3_bucket,
                        "s3_prefix": source.s3_prefix, "s3_prefixes": source_prefix_values(source), "aws_region": source.aws_region,
                        "destination_bucket": source.destination_bucket},
            "generated_at": utcnow(), "waves": items}


@app.get("/api/sources/{source_id}/report.csv")
def source_report_csv(source_id: int, session: Session = Depends(get_session)) -> StreamingResponse:
    report = source_report(source_id, session)
    content = io.StringIO()
    writer = csv.writer(content)
    writer.writerow(["source", "wave_id", "wave_name", "status", "objects", "bytes", "transferred_objects", "transferred_bytes", "failed_objects", "restore_requested_at", "first_available_at", "all_available_at", "restore_elapsed_seconds", "predicted_transfer_seconds"])
    for wave in report["waves"]:
        timing = wave["restore"]
        writer.writerow([report["source"]["name"], wave["wave_id"], wave["wave_name"], wave["status"], wave["objects"], wave["bytes"], wave["transferred_objects"], wave["transferred_bytes"], wave["failed_objects"], timing["requested_at"], timing["first_available_at"], timing["last_available_at"], timing["restore_elapsed_seconds"], wave["predicted_transfer_seconds"]])
    return StreamingResponse(iter([content.getvalue()]), media_type="text/csv", headers={"Content-Disposition": f'attachment; filename="source-{source_id}-report.csv"'})


@app.get("/api/discovery-queue")
def discovery_queue(limit: int = 100, session: Session = Depends(get_session)) -> list[dict]:
    """Local-only observability for durable remote-discovery work."""
    limit = min(max(limit, 1), 500)
    rows = session.execute(
        select(DiscoveryJob, Source)
        .join(Source, DiscoveryJob.source_id == Source.id)
        .order_by(
            case((DiscoveryJob.state == TaskState.RUNNING, 0), (DiscoveryJob.state == TaskState.READY, 1), else_=2),
            DiscoveryJob.available_at, DiscoveryJob.id,
        ).limit(limit)
    )
    now = utcnow()
    return [{
        "id": job.id, "source_id": source.id, "source_name": source.name, "mode": job.mode,
        "state": job.state, "attempts": job.attempts, "available_at": job.available_at,
        "lease_expires_at": job.lease_expires_at, "worker_id": job.worker_id,
        "error": job.error, "created_at": job.created_at, "completed_at": job.completed_at,
        "pages_completed": source.discovery_pages_completed,
        "objects_inserted": source.discovery_objects_inserted,
        "elapsed_seconds": int(float(source.discovery_elapsed_seconds or 0) + (max(0, (now - source.discovery_started_at).total_seconds()) if source.status == "DISCOVERING" and source.discovery_started_at else 0)),
    } for job, source in rows]


@app.post("/api/sources/{source_id}/validate-destination")
def validate_destination(source_id: int, session: Session = Depends(get_session)) -> dict:
    """Explicit OCI-only final reconciliation against the durable discovery."""
    source = active_source_or_409(session, source_id)
    expected = {obj.object_key: obj for obj in session.scalars(
        select(ObjectRecord).where(ObjectRecord.source_id == source.id, ObjectRecord.is_current_revision.is_(True))
    )}
    if not expected:
        raise HTTPException(status_code=409, detail="Run discovery before validating the destination")
    try:
        import oci
        namespace = read_oci_runtime_config().get("object_storage_namespace", "").strip()
        if not namespace:
            raise RuntimeError("OCI namespace is not configured")
        client = oci.object_storage.ObjectStorageClient({}, signer=oci.auth.signers.InstancePrincipalsSecurityTokenSigner())
        found: dict[str, int] = {}
        for prefix in source_prefix_values(source):
            start = None
            while True:
                arguments = {"prefix": prefix, "limit": 1000, "fields": "name,size"}
                if start:
                    arguments["start"] = start
                response = client.list_objects(namespace, source.destination_bucket, **arguments).data
                for item in response.objects:
                    found[item.name] = int(item.size)
                start = response.next_start_with
                if not start:
                    break
    except Exception as error:
        source.destination_validation_at, source.destination_validation_status = utcnow(), "FAILED"
        record_event(session, "DESTINATION_VALIDATION_FAILED", f"OCI destination validation failed: {type(error).__name__}", source_id=source.id)
        session.commit()
        raise HTTPException(status_code=502, detail=f"OCI destination validation failed: {type(error).__name__}") from error
    missing = sorted(key for key in expected if key not in found)
    mismatched = sorted(key for key, obj in expected.items() if key in found and found[key] != obj.size_bytes)
    # The listing establishes coverage cheaply.  HeadObject is performed only
    # for matching objects and verifies source provenance metadata that the
    # worker persisted at upload time; no S3 call and no object-body read occur.
    metadata_mismatched: list[str] = []
    for key, obj in expected.items():
        if key not in found or found[key] != obj.size_bytes or not obj.etag:
            continue
        try:
            headers = client.head_object(namespace, source.destination_bucket, key).headers
            if not destination_provenance_matches(obj, headers):
                metadata_mismatched.append(key)
        except Exception:
            metadata_mismatched.append(key)
    extras = sorted(key for key in found if key not in expected)
    source.destination_validation_at = utcnow()
    source.destination_missing_count, source.destination_size_mismatch_count = len(missing), len(mismatched)
    source.destination_metadata_mismatch_count, source.destination_extra_count = len(metadata_mismatched), len(extras)
    source.destination_validation_status = "VALID" if not missing and not mismatched and not metadata_mismatched else "DIFFERENT"
    affected_wave_counts: dict[int, int] = {}
    divergent_keys = sorted(set(missing + mismatched + metadata_mismatched))
    for offset in range(0, len(divergent_keys), 1000):
        objects = list(session.scalars(select(ObjectRecord).where(
            ObjectRecord.source_id == source.id, ObjectRecord.is_current_revision.is_(True),
            ObjectRecord.object_key.in_(divergent_keys[offset:offset + 1000])
        )))
        for obj in objects:
            obj.integrity_verified_at, obj.destination_checksum, obj.transferred_at = None, None, None
            obj.delivery_integrity_algorithm, obj.delivery_integrity_checksum = None, None
            obj.delivery_integrity_status, obj.delivery_integrity_verified_at = None, None
            obj.integrity_error = "OCI destination reconciliation found object missing, size-mismatched, or with mismatched provenance metadata"
            if obj.wave_id:
                obj.state = ObjectState.WAVE_ASSIGNED
                affected_wave_counts[obj.wave_id] = affected_wave_counts.get(obj.wave_id, 0) + 1
            else:
                obj.state = ObjectState.DISCOVERED
    for wave_id, affected_objects in affected_wave_counts.items():
        wave = session.get(Wave, wave_id)
        wave.status, wave.batch_job_id, wave.batch_job_status, wave.manifest_key, wave.manifest_etag = "READY_FOR_RESTORE", None, None, None, None
        wave.last_poll_at, wave.poll_count = None, 0
        wave.availability_head_requests, wave.availability_poll_elapsed_seconds = 0, 0
        wave.availability_throttle_retries = 0
        wave.last_availability_poll_objects, wave.last_availability_poll_seconds = 0, 0
        record_event(session, "DESTINATION_DIVERGENCE_REOPENED_WAVE", f"Wave reopened after OCI destination validation; {affected_objects} object(s) require reprocessing", source_id=source.id, wave_id=wave.id)
    record_event(session, "DESTINATION_VALIDATED", f"OCI destination reconciliation: {len(missing)} missing, {len(mismatched)} size mismatch(es), {len(metadata_mismatched)} metadata mismatch(es), {len(extras)} extra object(s)", source_id=source.id)
    session.commit()
    return {"source_id": source.id, "status": source.destination_validation_status, "expected": len(expected), "found": len(found), "missing": len(missing), "size_mismatches": len(mismatched), "metadata_mismatches": len(metadata_mismatched), "extras": len(extras), "missing_examples": missing[:50], "size_mismatch_examples": mismatched[:50], "metadata_mismatch_examples": metadata_mismatched[:50], "extra_examples": extras[:50], "validated_at": source.destination_validation_at}


@app.get("/api/sources/{source_id}/inventory")
def list_inventory(source_id: int, limit: int = 10, offset: int = 0,
                   after_key: str = Query(default="", max_length=2048),
                   after_id: int = Query(default=0, ge=0),
                   search: str = Query(default="", max_length=512),
                   session: Session = Depends(get_session)) -> dict:
    """Read inventory with keyset pagination when a cursor is supplied.

    ``offset`` remains for old clients only. Raijin's UI uses the key/id
    cursor, so opening page 50,000 does not force PostgreSQL to walk through
    the preceding 499,990 rows.
    """
    source_or_404(session, source_id)
    limit = min(max(limit, 1), 1000)
    filters = [ObjectRecord.source_id == source_id, ObjectRecord.is_current_revision.is_(True)]
    if search.strip():
        filters.append(ObjectRecord.object_key.ilike(f"%{search.strip()}%"))
    use_cursor = bool(after_key)
    if use_cursor:
        filters.append(or_(
            ObjectRecord.object_key > after_key,
            and_(ObjectRecord.object_key == after_key, ObjectRecord.id > after_id),
        ))
    query = select(ObjectRecord).where(*filters).order_by(ObjectRecord.object_key, ObjectRecord.id)
    if not use_cursor:
        query = query.offset(max(offset, 0))
    rows = list(session.scalars(query.limit(limit + 1)))
    has_more = len(rows) > limit
    rows = rows[:limit]
    next_cursor = {"object_key": rows[-1].object_key, "id": rows[-1].id} if rows and has_more else None
    # Counting every row on each navigation becomes an avoidable expensive
    # aggregate for a 100-million-object source. Total is intentionally
    # omitted for cursor calls; source summary already exposes its durable
    # inventory total.
    total = None if use_cursor else session.scalar(select(func.count(ObjectRecord.id)).where(*filters)) or 0
    return {"items": [{"id": obj.id, "key": obj.object_key, "version_id": obj.version_id,
                       "size_bytes": obj.size_bytes, "storage_class": obj.storage_class, "state": obj.state,
                       "last_modified": obj.last_modified, "etag": obj.etag, "wave_id": obj.wave_id} for obj in rows],
            "limit": limit, "offset": offset, "total": total, "next_cursor": next_cursor,
            "search": search.strip()}


@app.get("/api/objects/{object_id}")
def object_detail(object_id: int, session: Session = Depends(get_session)) -> dict:
    obj = session.get(ObjectRecord, object_id)
    if not obj:
        raise HTTPException(status_code=404, detail="Object not found")
    return {"id": obj.id, "source_id": obj.source_id, "wave_id": obj.wave_id, "key": obj.object_key,
            "version_id": obj.version_id, "size_bytes": obj.size_bytes, "etag": obj.etag,
            "storage_class": obj.storage_class, "last_modified": obj.last_modified, "state": obj.state,
            "metadata": json.loads(obj.metadata_json), "tags": json.loads(obj.tags_json),
            "integrity": {"source_checksum": obj.source_checksum, "destination_checksum": obj.destination_checksum,
                          "algorithm": obj.checksum_algorithm, "verified_at": obj.integrity_verified_at,
                          "error": obj.integrity_error,
                          "delivery_algorithm": obj.delivery_integrity_algorithm,
                          "delivery_checksum": obj.delivery_integrity_checksum,
                          "delivery_status": obj.delivery_integrity_status,
                          "delivery_verified_at": obj.delivery_integrity_verified_at}, "restored_at": obj.restored_at,
            "transferred_at": obj.transferred_at}


@app.put("/api/objects/{object_id}/integrity")
def record_integrity(object_id: int, payload: IntegrityEvidence, session: Session = Depends(get_session)) -> dict:
    """Persist verification evidence. Production transfer worker is its only intended caller."""
    obj = session.get(ObjectRecord, object_id)
    if not obj:
        raise HTTPException(status_code=404, detail="Object not found")
    if payload.verified and (not payload.source_checksum or not payload.destination_checksum):
        raise HTTPException(status_code=422, detail="Verified evidence requires both source and destination checksums")
    obj.source_checksum = payload.source_checksum
    obj.destination_checksum = payload.destination_checksum
    obj.checksum_algorithm = payload.checksum_algorithm
    obj.integrity_verified_at = utcnow() if payload.verified else None
    obj.integrity_error = payload.error
    if payload.verified:
        obj.state = ObjectState.VERIFIED
    elif payload.error:
        obj.state = ObjectState.FAILED
    record_event(session, "INTEGRITY_RECORDED", f"Integrity evidence recorded for object {obj.id}: {'verified' if payload.verified else 'failed'}", source_id=obj.source_id, wave_id=obj.wave_id)
    session.commit()
    return {"id": obj.id, "state": obj.state, "verified_at": obj.integrity_verified_at}


@app.get("/api/sources/{source_id}/inventory.csv")
def export_inventory(source_id: int, session: Session = Depends(get_session)) -> StreamingResponse:
    source_or_404(session, source_id)
    content = io.StringIO()
    writer = csv.writer(content, lineterminator="\n")
    writer.writerow(["object_key", "version_id", "size_bytes", "etag", "storage_class", "last_modified", "state", "wave_id", "metadata_json", "tags_json"])
    for obj in session.scalars(select(ObjectRecord).where(ObjectRecord.source_id == source_id, ObjectRecord.is_current_revision.is_(True)).order_by(ObjectRecord.object_key)):
        writer.writerow([obj.object_key, obj.version_id or "", obj.size_bytes, obj.etag or "", obj.storage_class or "",
                         obj.last_modified.isoformat() if obj.last_modified else "", obj.state, obj.wave_id or "", obj.metadata_json, obj.tags_json])
    return StreamingResponse(iter([content.getvalue()]), media_type="text/csv", headers={"Content-Disposition": f'attachment; filename="source-{source_id}-inventory.csv"'})


def discovered_object_filters(source_id: int, prefix: str = "") -> list:
    filters = [ObjectRecord.source_id == source_id, ObjectRecord.is_current_revision.is_(True), ObjectRecord.state == ObjectState.DISCOVERED]
    if prefix.strip():
        filters.append(ObjectRecord.object_key.startswith(prefix.strip()))
    return filters


def assign_wave(session: Session, source_id: int, name: str, max_bytes: int, restore_days: int,
                restore_tier: str, objects: list[ObjectRecord], oversized: bool = False,
                *, planner_mode: str = "MANUAL", predicted_transfer_seconds: float = 0,
                prediction_samples: int = 0, planned_restore_at: datetime | None = None,
                planned_transfer_start_at: datetime | None = None,
                pipeline_run_id: int | None = None,
                transfer_release_policy: str = "AFTER_ALL_RESTORED") -> Wave:
    assigned_bytes = sum(obj.size_bytes for obj in objects)
    wave = Wave(source_id=source_id, name=name, max_bytes=max_bytes, restore_days=restore_days,
                restore_tier=restore_tier, status="PLANNED", planner_mode=planner_mode,
                predicted_transfer_seconds=predicted_transfer_seconds, prediction_samples=prediction_samples,
                planned_restore_at=planned_restore_at, planned_transfer_start_at=planned_transfer_start_at,
                pipeline_run_id=pipeline_run_id, transfer_release_policy=transfer_release_policy)
    session.add(wave)
    session.flush()
    for obj in objects:
        obj.wave_id = wave.id
        obj.state = ObjectState.WAVE_ASSIGNED
    suffix = " (contains an object larger than the configured target)" if oversized else ""
    prediction = f"; predicted transfer {int(predicted_transfer_seconds)}s from {prediction_samples} historical sample(s)" if planner_mode == "DYNAMIC" else ""
    record_event(session, "WAVE_CREATED", f"Wave '{wave.name}' planned with {len(objects)} object(s) and {assigned_bytes} byte(s){suffix}{prediction}; no task was queued", source_id=source_id, wave_id=wave.id)
    return wave


def prediction_bucket(size_bytes: int, multipart_part_size_mib: int) -> str:
    """Stable, explainable object classes used by the v1 historical model."""
    mib = 1024 ** 2
    if size_bytes >= multipart_part_size_mib * mib:
        return "multipart"
    if size_bytes <= mib:
        return "up_to_1_mib"
    if size_bytes <= 16 * mib:
        return "up_to_16_mib"
    if size_bytes <= 256 * mib:
        return "up_to_256_mib"
    return "large_single_part"


def percentile_75(values: list[float]) -> float:
    ordered = sorted(value for value in values if value > 0)
    if not ordered:
        return 0.0
    return ordered[min(len(ordered) - 1, math.ceil(len(ordered) * .75) - 1)]


def transfer_history_profiles(session: Session, source_id: int, multipart_part_size_mib: int) -> dict[str, dict]:
    """Return durable P75 per-object elapsed observations for one source.

    Only completed objects with a measured interval participate.  Failed and
    legacy rows with a zero duration intentionally do not pollute forecasts.
    """
    rows = session.execute(
        select(ObjectRecord.size_bytes, ObjectRecord.transfer_elapsed_seconds)
        .where(ObjectRecord.source_id == source_id,
               ObjectRecord.transferred_at.is_not(None),
               ObjectRecord.transfer_elapsed_seconds > 0,
               ObjectRecord.state.in_([ObjectState.TRANSFERRED, ObjectState.VERIFIED]))
    )
    grouped: dict[str, list[float]] = {}
    for size_bytes, elapsed in rows:
        grouped.setdefault(prediction_bucket(size_bytes, multipart_part_size_mib), []).append(float(elapsed))
    return {bucket: {"samples": len(values), "p75_seconds": percentile_75(values)}
            for bucket, values in grouped.items()}


def predict_object_transfer_seconds(obj: ObjectRecord, settings: RuntimeSettings, profiles: dict[str, dict]) -> tuple[float, int]:
    """Predict one object conservatively; history is used only with 5 samples."""
    return predict_size_transfer_seconds(obj.size_bytes, settings, profiles)


def predict_size_transfer_seconds(size_bytes: int, settings: RuntimeSettings, profiles: dict[str, dict]) -> tuple[float, int]:
    """Predict an object from scalar inventory data without retaining an ORM row."""
    bucket = prediction_bucket(size_bytes, settings.multipart_part_size_mib)
    profile = profiles.get(bucket, {})
    if profile.get("samples", 0) >= 5 and profile.get("p75_seconds", 0) > 0:
        return float(profile["p75_seconds"]), int(profile["samples"])
    # Link-model fallback: service/object overhead + the object's fair share
    # of configured aggregate bandwidth.  Multipart has a modest setup term.
    throughput_per_worker = max(1.0, settings.max_throughput_mbps / RAIJU_MIN_WORKERS)
    # Small-object operations are dominated by request, TLS and OCI write
    # overhead.  The prior 0.25-second cold estimate underpredicted the
    # Deep Archive validation workload by about 4x.  Use a conservative
    # baseline until this source has its own durable P75 history.
    seconds = 1.25 + (size_bytes * 8 / (throughput_per_worker * 1_000_000))
    if size_bytes >= settings.multipart_part_size_mib * 1024 ** 2:
        parts = math.ceil(size_bytes / (settings.multipart_part_size_mib * 1024 ** 2))
        seconds += 1.0 + parts * .08
    return max(.25, seconds), 0


def automatic_dynamic_duration_limit(restore_days: int) -> tuple[int, int]:
    """Derive a safe copy window from the requested restored-copy retention.

    S3 Batch restore retention is specified in whole days.  We plan against
    its guaranteed lower bound and reserve the larger of eight hours or 25%
    for throughput variance, retries and a recoverable VM interruption.  The
    operator never supplies this value: retention is the operational contract.
    """
    retention_seconds = restore_days * 24 * 3600
    reserve_seconds = max(8 * 3600, math.ceil(retention_seconds * .25))
    return max(300, retention_seconds - reserve_seconds), reserve_seconds


def dynamic_wave_plan(session: Session, source_id: int, max_bytes: int, target_transfer_seconds: int,
                      max_objects: int, prefix: str = "") -> dict:
    """Build, but do not persist, deterministic groups for the dynamic planner."""
    settings = runtime_settings(session)
    profiles = transfer_history_profiles(session, source_id, settings.multipart_part_size_mib)
    waves: list[dict] = []
    current_count = 0
    bytes_total = predicted_sum = sample_count = 0

    def flush(exclusive: bool = False) -> None:
        nonlocal current_count, bytes_total, predicted_sum, sample_count
        if not current_count:
            return
        # Individual estimates represent worker time. Wall time is bounded by
        # aggregate throughput and by worker parallelism, plus a small setup.
        link_seconds = bytes_total * 8 / max(1, settings.max_throughput_mbps * 1_000_000)
        wall_seconds = max(link_seconds, predicted_sum / RAIJU_MIN_WORKERS) + 30
        waves.append({"bytes": bytes_total, "object_count": current_count,
                      "predicted_transfer_seconds": math.ceil(wall_seconds), "prediction_samples": sample_count,
                      "exclusive": exclusive})
        current_count, bytes_total, predicted_sum, sample_count = 0, 0, 0.0, 0

    rows = session.execute(select(ObjectRecord.size_bytes).where(
        *discovered_object_filters(source_id, prefix)
    ).order_by(ObjectRecord.object_key, ObjectRecord.id)).yield_per(5000)
    for (size_bytes,) in rows:
        predicted, samples = predict_size_transfer_seconds(size_bytes, settings, profiles)
        projected_bytes = bytes_total + size_bytes
        projected_count = current_count + 1
        projected_sum = predicted_sum + predicted
        projected_wall = max(projected_bytes * 8 / max(1, settings.max_throughput_mbps * 1_000_000),
                             projected_sum / RAIJU_MIN_WORKERS) + 30
        exceeds = projected_bytes > max_bytes or projected_count > max_objects or projected_wall > target_transfer_seconds
        if current_count and exceeds:
            flush()
        if not current_count:
            # An object beyond any hard/soft target forms an exclusive wave;
            # it is never silently skipped.
            current_count = 1
            bytes_total, predicted_sum, sample_count = size_bytes, predicted, samples
            one_wall = max(bytes_total * 8 / max(1, settings.max_throughput_mbps * 1_000_000),
                           predicted_sum / RAIJU_MIN_WORKERS) + 30
            if bytes_total > max_bytes or one_wall > target_transfer_seconds:
                flush(exclusive=True)
            continue
        current_count += 1
        # A wave may mix size classes. Keep the strongest historical sample
        # count as a confidence indicator; do not inflate it per object.
        bytes_total, predicted_sum, sample_count = projected_bytes, projected_sum, max(sample_count, samples)
    flush()
    return {"waves": waves, "profiles": profiles, "settings": settings}


def next_dynamic_wave_plan(session: Session, source_id: int, max_bytes: int,
                           target_transfer_seconds: int, max_objects: int,
                           prefix: str = "") -> tuple[dict | None, dict[str, dict], RuntimeSettings]:
    """Pack only the next unassigned dynamic wave.

    This is deliberately different from :func:`dynamic_wave_plan`, which is a
    read-only preview.  A live adaptive pipeline must not freeze boundaries for
    every remaining object before it has measurements from the first waves.
    """
    settings = runtime_settings(session)
    profiles = transfer_history_profiles(session, source_id, settings.multipart_part_size_mib)
    object_count = bytes_total = prediction_samples = 0
    predicted_sum = 0.0
    rows = session.execute(select(ObjectRecord.size_bytes).where(
        *discovered_object_filters(source_id, prefix)
    ).order_by(ObjectRecord.object_key, ObjectRecord.id)).yield_per(5000)
    for (size_bytes,) in rows:
        predicted, samples = predict_size_transfer_seconds(size_bytes, settings, profiles)
        projected_bytes = bytes_total + size_bytes
        projected_count = object_count + 1
        projected_sum = predicted_sum + predicted
        projected_wall = max(
            projected_bytes * 8 / max(1, settings.max_throughput_mbps * 1_000_000),
            projected_sum / RAIJU_MIN_WORKERS,
        ) + 30
        if object_count and (
            projected_bytes > max_bytes or projected_count > max_objects or projected_wall > target_transfer_seconds
        ):
            break
        object_count = projected_count
        bytes_total = projected_bytes
        predicted_sum = projected_sum
        prediction_samples = max(prediction_samples, samples)
        if object_count == 1 and (bytes_total > max_bytes or projected_wall > target_transfer_seconds):
            return ({"bytes": bytes_total, "object_count": object_count,
                     "predicted_transfer_seconds": math.ceil(projected_wall),
                     "prediction_samples": prediction_samples, "exclusive": True}, profiles, settings)
    if not object_count:
        return None, profiles, settings
    wall_seconds = max(
        bytes_total * 8 / max(1, settings.max_throughput_mbps * 1_000_000),
        predicted_sum / RAIJU_MIN_WORKERS,
    ) + 30
    return ({"bytes": bytes_total, "object_count": object_count,
             "predicted_transfer_seconds": math.ceil(wall_seconds),
             "prediction_samples": prediction_samples, "exclusive": False}, profiles, settings)


def dynamic_wave_schedule(run: DynamicPipelineRun, prior_waves: list[Wave],
                          scheduler_now: datetime) -> tuple[datetime, datetime]:
    """Schedule one newly materialized wave after the current transfer lane."""
    service_window = restore_service_window_seconds(run.restore_tier)
    if prior_waves:
        predecessor = prior_waves[-1]
        predecessor_start = predecessor.planned_transfer_start_at or scheduler_now
        predecessor_end = predecessor_start + timedelta(seconds=max(1, int(predecessor.predicted_transfer_seconds or 1)))
        transfer_start = max(scheduler_now + timedelta(seconds=service_window), predecessor_end)
    else:
        transfer_start = scheduler_now + timedelta(seconds=service_window)
    restore_lead = service_window + int(run.restore_safety_seconds or 0)
    return max(scheduler_now, transfer_start - timedelta(seconds=restore_lead)), transfer_start


def assign_dynamic_wave_objects(session: Session, wave: Wave, source_id: int, prefix: str,
                                plan: dict, settings: RuntimeSettings,
                                profiles: dict[str, dict]) -> int:
    """Assign the exact next deterministic slice without retaining payload rows."""
    assigned = 0
    mappings: list[dict] = []
    rows = session.execute(select(ObjectRecord.id, ObjectRecord.size_bytes).where(
        *discovered_object_filters(source_id, prefix)
    ).order_by(ObjectRecord.object_key, ObjectRecord.id).limit(plan["object_count"])).yield_per(5000)
    for object_id, size_bytes in rows:
        prediction, _samples = predict_size_transfer_seconds(size_bytes, settings, profiles)
        mappings.append({"id": object_id, "wave_id": wave.id,
                         "state": ObjectState.WAVE_ASSIGNED,
                         "planned_transfer_seconds": prediction})
        if len(mappings) >= 5000:
            session.bulk_update_mappings(ObjectRecord, mappings)
            assigned += len(mappings)
            mappings.clear()
    if mappings:
        session.bulk_update_mappings(ObjectRecord, mappings)
        assigned += len(mappings)
    if assigned != plan["object_count"]:
        raise RuntimeError(f"Dynamic wave assignment produced {assigned} object(s); expected {plan['object_count']}")
    return assigned


def adaptive_restore_slot_limit(session: Session, run: DynamicPipelineRun,
                                settings: RuntimeSettings) -> int:
    """Return the durable restore-lane capacity for one dynamic pipeline.

    Two concurrent restores are the safe cold-start baseline.  Only after at
    least three completed waves provide usable timing evidence can Raikou add
    slots, and then only when the observed/planned restore lead would otherwise
    leave the single Raiju transfer lane idle.  A deliberately small ceiling
    prevents a temporary noisy estimate from creating an expensive restore
    fan-out.
    """
    baseline = 2
    completed = list(session.scalars(select(Wave).where(
        Wave.pipeline_run_id == run.id,
        Wave.status.in_(["COMPLETED", "VERIFIED", "TRANSFERRED"]),
        Wave.predicted_transfer_seconds > 0,
    ).order_by(Wave.id.desc()).limit(12)))
    if len(completed) < 3:
        return baseline
    predicted = sorted(float(w.predicted_transfer_seconds) for w in completed)
    transfer_seconds = predicted[len(predicted) // 2]
    if transfer_seconds <= 0:
        return baseline
    # Prefer observed end-to-end restore timings once the source has enough
    # completed waves.  The service-window fallback remains intentionally
    # conservative for a cold source and for sparse/partial evidence.
    observed_restore_seconds: list[float] = []
    completed_by_id = {wave.id: wave for wave in completed}
    for wave_id, requested_at, available_at in session.execute(select(
        ObjectRecord.wave_id,
        func.min(ObjectRecord.restore_requested_at),
        func.max(ObjectRecord.restored_at),
    ).where(
        ObjectRecord.wave_id.in_(tuple(completed_by_id)),
        ObjectRecord.restore_requested_at.is_not(None),
        ObjectRecord.restored_at.is_not(None),
    ).group_by(ObjectRecord.wave_id)):
        wave = completed_by_id[wave_id]
        if wave.source.backend_kind == "SIMULATED":
            requested_at = wave.restore_requested_virtual_at or requested_at
            available_at = wave.last_restore_available_virtual_at or available_at
        if requested_at and available_at and available_at >= requested_at:
            observed_restore_seconds.append((available_at - requested_at).total_seconds())
    restore_seconds = (
        percentile_75(observed_restore_seconds)
        if len(observed_restore_seconds) >= 3
        else restore_service_window_seconds(run.restore_tier)
    )
    needed_for_continuity = math.ceil(restore_seconds / transfer_seconds)
    retention_seconds = max(1, int(run.restore_days)) * 24 * 3600
    safe_by_retention = max(baseline, int(retention_seconds // transfer_seconds))
    configured_ceiling = max(baseline, min(4, int(settings.dynamic_restore_max_slots or 4)))
    return max(baseline, min(configured_ceiling, safe_by_retention, needed_for_continuity))


def materialize_dynamic_pipeline_horizon(session: Session, settings: RuntimeSettings,
                                         run: DynamicPipelineRun,
                                         now: datetime | None = None) -> list[Wave]:
    """Keep only a small, adaptive set of waves materialized ahead of the lane."""
    refresh_dynamic_pipeline_run(session, run)
    if run.status in {"COMPLETED", "HISTORICAL", "NEEDS_ATTENTION"}:
        return []
    source = session.get(Source, run.source_id) or source_or_404(session, run.source_id)
    scheduler_now = now or source_scheduler_clock(source).effective_now
    slots = adaptive_restore_slot_limit(session, run, settings)
    # One future wave remains unsubmitted and can be repacked after fresh
    # observations.  The initial 3-wave horizon therefore means two restore
    # slots plus one mutable planning slice.
    configured_horizon = int(run.restore_horizon_waves or settings.dynamic_restore_horizon_waves or 1)
    # Respect an explicit smaller horizon used by controlled tests/operators.
    # The default remains three (two restore slots plus one mutable slice).
    horizon = configured_horizon if slots == 2 else max(configured_horizon, slots + 1)
    terminal = {"COMPLETED", "VERIFIED", "TRANSFERRED", "FAILED", "TRANSFERRED_WITH_ERRORS", "RESTORE_REQUEST_FAILED", "VERIFICATION_FAILED"}
    existing = list(session.scalars(select(Wave).where(Wave.pipeline_run_id == run.id).order_by(
        Wave.planned_transfer_start_at, Wave.id
    )))
    active = [wave for wave in existing if wave.status not in terminal]
    created: list[Wave] = []
    while len(active) < horizon:
        plan, profiles, planner_settings = next_dynamic_wave_plan(
            session, source.id, run.target_max_bytes, run.target_transfer_seconds,
            run.max_objects, run.selection_prefix,
        )
        if not plan:
            break
        restore_at, transfer_at = dynamic_wave_schedule(run, existing, scheduler_now)
        name = automatic_wave_name(session, source, run.selection_prefix, run.next_sequence)
        run.next_sequence += 1
        wave = Wave(
            source_id=source.id, name=name, max_bytes=run.target_max_bytes,
            restore_days=run.restore_days, restore_tier=run.restore_tier,
            transfer_release_policy="AS_OBJECTS_AVAILABLE",
            status="RESTORE_SCHEDULED", planner_mode="DYNAMIC",
            predicted_transfer_seconds=plan["predicted_transfer_seconds"],
            prediction_samples=plan["prediction_samples"], planned_restore_at=restore_at,
            planned_transfer_start_at=transfer_at, pipeline_run_id=run.id,
        )
        session.add(wave)
        session.flush()
        assigned = assign_dynamic_wave_objects(session, wave, source.id, run.selection_prefix,
                                                plan, planner_settings, profiles)
        record_event(
            session, "DYNAMIC_WAVE_MATERIALIZED",
            f"Dynamic pipeline run {run.id} materialized wave '{wave.name}' with {assigned} object(s) and {plan['bytes']} byte(s); predicted transfer {plan['predicted_transfer_seconds']}s from {plan['prediction_samples']} historical sample(s)",
            source_id=source.id, wave_id=wave.id,
        )
        record_event(
            session, "DYNAMIC_RESTORE_SCHEDULED",
            f"Wave '{wave.name}' restore scheduled for {restore_at.isoformat()} before predicted transfer window {transfer_at.isoformat()}",
            source_id=source.id, wave_id=wave.id,
        )
        existing.append(wave)
        active.append(wave)
        created.append(wave)
    refresh_dynamic_pipeline_run(session, run)
    return created


def next_wave_objects(session: Session, source_id: int, max_bytes: int, prefix: str = "") -> tuple[list[ObjectRecord], bool]:
    """Select the next deterministic group without loading the full inventory."""
    selected: list[ObjectRecord] = []
    remaining = max_bytes
    last_key: str | None = None
    last_id: int | None = None
    filters = discovered_object_filters(source_id, prefix)
    while True:
        keyset = []
        if last_key is not None and last_id is not None:
            keyset.append(or_(ObjectRecord.object_key > last_key, and_(ObjectRecord.object_key == last_key, ObjectRecord.id > last_id)))
        rows = list(session.scalars(
            select(ObjectRecord).where(*filters, *keyset).order_by(ObjectRecord.object_key, ObjectRecord.id).limit(1000).with_for_update(skip_locked=True)
        ))
        if not rows:
            return selected, False
        for obj in rows:
            last_key, last_id = obj.object_key, obj.id
            if obj.size_bytes > max_bytes:
                if selected:
                    return selected, False
                return [obj], True
            if selected and obj.size_bytes > remaining:
                return selected, False
            selected.append(obj)
            remaining -= obj.size_bytes
            if remaining == 0:
                return selected, False


def automatic_wave_name(session: Session, source: Source, prefix: str, sequence: int) -> str:
    prefix_part = re.sub(r"[^A-Za-z0-9_-]+", "-", prefix.strip("/")) if prefix.strip("/") else ""
    stem = f"{source.name}-{prefix_part + '-' if prefix_part else ''}wave"
    stem = stem[:120].rstrip("-_") or "wave"
    candidate = f"{stem}-{sequence:03d}"
    while session.scalar(select(Wave.id).where(Wave.source_id == source.id, Wave.name == candidate)):
        sequence += 1
        candidate = f"{stem}-{sequence:03d}"
    return candidate


@app.get("/api/sources/{source_id}/waves/preview")
def preview_automatic_waves(source_id: int, max_bytes: int = Query(gt=0, le=10 * 1024**4),
                            prefix: str = Query(default="", max_length=1024),
                            session: Session = Depends(get_session)) -> dict:
    active_source_or_409(session, source_id)
    filters = discovered_object_filters(source_id, prefix)
    objects, total_bytes = session.execute(
        select(func.count(ObjectRecord.id), func.coalesce(func.sum(ObjectRecord.size_bytes), 0)).where(*filters)
    ).one()
    oversized = session.scalar(select(func.count(ObjectRecord.id)).where(*filters, ObjectRecord.size_bytes > max_bytes)) or 0
    estimate = (total_bytes + max_bytes - 1) // max_bytes if total_bytes else 0
    # This limit keeps a mistaken MB-sized target from turning a single action
    # into millions of durable tasks on the VM.
    return {"objects": objects, "bytes": total_bytes, "estimated_waves": estimate,
            "oversized_objects": oversized, "prefix": prefix.strip(), "max_automatic_waves": 10000}


@app.post("/api/sources/{source_id}/waves", status_code=201)
def create_wave(source_id: int, payload: WaveCreate, session: Session = Depends(get_session)) -> dict:
    source = active_source_or_409(session, source_id)
    require_non_overlapping_source_scope(session, source)
    if session.scalar(select(Wave).where(Wave.source_id == source_id, Wave.name == payload.name)):
        raise HTTPException(status_code=409, detail="Wave name already exists for this source")
    objects, oversized = next_wave_objects(session, source_id, payload.max_bytes)
    if not objects:
        raise HTTPException(status_code=409, detail="No discovered objects are available for this wave")
    wave = assign_wave(session, source.id, payload.name, payload.max_bytes, payload.restore_days,
                       payload.restore_tier, objects, oversized,
                       transfer_release_policy=payload.transfer_release_policy)
    session.commit()
    return {"id": wave.id, "name": wave.name, "objects": len(objects), "bytes": sum(obj.size_bytes for obj in objects),
            "status": wave.status, "oversized": oversized}


@app.post("/api/sources/{source_id}/waves/automatic", status_code=201)
def create_automatic_waves(source_id: int, payload: AutomaticWaveCreate, session: Session = Depends(get_session)) -> dict:
    source = active_source_or_409(session, source_id)
    require_non_overlapping_source_scope(session, source)
    preview = preview_automatic_waves(source_id, payload.max_bytes, payload.prefix, session)
    if not preview["objects"]:
        raise HTTPException(status_code=409, detail="No discovered objects match this automatic-wave selection")
    if preview["estimated_waves"] > preview["max_automatic_waves"]:
        raise HTTPException(status_code=422, detail=f"Estimated {preview['estimated_waves']} waves exceeds the safety limit of {preview['max_automatic_waves']}. Increase the target size.")
    created: list[Wave] = []
    sequence = 1
    total_objects = total_bytes = oversized_waves = 0
    while True:
        objects, oversized = next_wave_objects(session, source_id, payload.max_bytes, payload.prefix)
        if not objects:
            break
        name = automatic_wave_name(session, source, payload.prefix, sequence)
        wave = assign_wave(session, source.id, name, payload.max_bytes, payload.restore_days,
                           payload.restore_tier, objects, oversized,
                           transfer_release_policy=payload.transfer_release_policy)
        created.append(wave)
        total_objects += len(objects)
        total_bytes += sum(obj.size_bytes for obj in objects)
        oversized_waves += int(oversized)
        sequence += 1
    session.commit()
    return {"waves": len(created), "objects": total_objects, "bytes": total_bytes, "oversized_waves": oversized_waves,
            "names": [wave.name for wave in created]}


def dynamic_schedule_times(now: datetime, plans: list[dict], safety_seconds: int) -> list[tuple[datetime, datetime]]:
    """Forecast a single transfer lane with restore requests issued in advance."""
    # Safety is an advance-notice allowance for later restore submissions. It
    # is never added to the AWS service window and must not postpone the first
    # transfer. Objects observed as available may be copied even earlier.
    first_tier = plans[0].get("restore_tier") if plans else "BULK"
    transfer_start = now + timedelta(seconds=restore_service_window_seconds(first_tier))
    times: list[tuple[datetime, datetime]] = []
    for plan in plans:
        # BULK maximum latency is 48h; Standard is planned conservatively at
        # 12h. The additional configured safety protects the handoff window.
        restore_lead = restore_service_window_seconds(plan.get("restore_tier")) + safety_seconds
        restore_at = max(now, transfer_start - timedelta(seconds=restore_lead))
        times.append((restore_at, transfer_start))
        transfer_start += timedelta(seconds=plan["predicted_transfer_seconds"])
    return times


def restore_service_window_seconds(tier: str | None) -> int:
    """Return the conservative service window used by the local planner."""
    return (48 if tier == "BULK" else 12) * 3600


def release_dynamic_restore_horizon(session: Session, settings: RuntimeSettings, now: datetime | None = None) -> int:
    """Release only due restores inside each dynamic run's durable horizon.

    Only the adaptive horizon is materialized. Charged S3 Batch jobs are then
    emitted gradually within that horizon. A restore slot is occupied from
    submission until all of that wave's objects are available. The transfer
    lane remains independent and serial, which avoids restoring an unbounded
    set of temporary copies while preserving a warm transfer pipeline.
    """
    queue_now = utcnow()
    released = 0
    # Restore and transfer are separate lanes. A wave frees an expensive
    # restore slot when all of its objects are available, even while the
    # single global transfer lane is still copying it.
    active_statuses = {"RESTORE_REQUESTED", "RESTORE_REQUEST_ACCEPTED", "RESTORING"}
    runs = list(session.scalars(select(DynamicPipelineRun).where(
        DynamicPipelineRun.scheduled_restores.is_(True),
        DynamicPipelineRun.status.not_in(["COMPLETED", "HISTORICAL"]),
    )))
    for run in runs:
        # A failed wave is a durable operator decision point.  Do not consume
        # additional restore windows or costs by marching later waves forward
        # after the transfer lane has already stopped.
        refresh_dynamic_pipeline_run(session, run)
        if run.status == "NEEDS_ATTENTION":
            continue
        scheduler_now = now or source_scheduler_clock(run.source).effective_now
        if runtime_context.is_simulation and run.source.backend_kind == "SIMULATED" and now is None:
            # Simulator time is intentionally paused between durable
            # decisions. When this source has no claimed/ready work, jump only
            # to the next already-planned restore submission; never let a
            # free-running accelerated clock burn a temporary restore window.
            active_work = session.scalar(select(Task.id).join(Wave).where(
                Wave.source_id == run.source_id,
                Task.state.in_([TaskState.READY, TaskState.RUNNING]),
            ).limit(1))
            if active_work is None:
                next_wave = session.scalar(select(Wave).where(
                    Wave.pipeline_run_id == run.id,
                    Wave.status == "RESTORE_SCHEDULED",
                    Wave.planned_restore_at.is_not(None),
                ).order_by(Wave.planned_restore_at, Wave.id).limit(1))
                if next_wave and next_wave.planned_restore_at and next_wave.planned_restore_at > scheduler_now:
                    advance_seconds = (next_wave.planned_restore_at - scheduler_now).total_seconds()
                    SimulatorAdminClient(runtime_context.simulator_base_url).control_clock(
                        run.source.simulation_execution_id, "ADVANCE", advance_seconds
                    )
                    scheduler_now = source_scheduler_clock(run.source).effective_now
                    record_event(
                        session,
                        "SIMULATION_CLOCK_ADVANCED",
                        f"Virtual clock advanced {int(advance_seconds)}s to the next scheduled restore",
                        source_id=run.source_id,
                        wave_id=next_wave.id,
                    )
        release_capacity = adaptive_restore_slot_limit(session, run, settings)
        available_backlog_seconds = continuous_lane_backlog_seconds(session, run.source_id)
        # Restore and transfer are now decoupled.  Do not wait for a calendar
        # slot when Raiju has less than its minimum healthy stock; use the
        # bounded restore slots to warm the lane.  A target-sized backlog is
        # still preferred, but the minimum is the hard anti-idleness signal.
        minimum_buffer = float(settings.continuous_transfer_min_buffer_seconds or 0)
        target_buffer = max(minimum_buffer, float(settings.continuous_transfer_target_buffer_seconds or 0))
        maximum_buffer = max(target_buffer, float(settings.continuous_transfer_max_buffer_seconds or 0))
        # The target is the stock Raikou tries to maintain.  The minimum is a
        # hard anti-idleness threshold, while the ceiling prevents restores
        # from being submitted so far ahead that temporary copies expire.
        replenish_lane = available_backlog_seconds < target_buffer
        within_minimum = available_backlog_seconds < minimum_buffer
        projected_backlog_seconds = available_backlog_seconds
        configured_horizon = int(run.restore_horizon_waves or settings.dynamic_restore_horizon_waves or 1)
        horizon = configured_horizon if release_capacity == 2 else max(configured_horizon, release_capacity + 1)
        # Keep a future slice mutable.  The baseline is always two concurrent
        # restores; Raikou may safely add slots only after sufficient evidence.
        waves = list(session.scalars(select(Wave).where(Wave.pipeline_run_id == run.id).order_by(
            Wave.planned_restore_at, Wave.id
        )))
        occupied = sum(1 for wave in waves if wave.status in active_statuses or (
            wave.status == "RESTORE_SCHEDULED" and session.scalar(select(Task.id).where(
                Task.wave_id == wave.id, Task.kind == "SUBMIT_BATCH_RESTORE"
            ).limit(1)) is not None
        ))
        for wave in waves:
            if occupied >= release_capacity:
                break
            if projected_backlog_seconds >= maximum_buffer:
                break
            if wave.status != "RESTORE_SCHEDULED":
                continue
            estimated_wave_seconds = max(1.0, float(wave.predicted_transfer_seconds or 0))
            is_future_restore = bool(wave.planned_restore_at and wave.planned_restore_at > scheduler_now)
            # A future restore may be pulled forward only while the projected
            # post-release lane remains below its target.  Already-due work
            # is still released, unless the safety ceiling is occupied.
            if is_future_restore and (not replenish_lane or projected_backlog_seconds >= target_buffer):
                continue
            exists = session.scalar(select(Task.id).where(
                Task.wave_id == wave.id, Task.kind == "SUBMIT_BATCH_RESTORE"
            ).limit(1))
            if exists is not None:
                continue
            # Task leases always use wall time. A virtual timestamp here would
            # make the real durable queue wait until that future date.
            session.add(Task(wave_id=wave.id, kind="SUBMIT_BATCH_RESTORE", available_at=queue_now))
            if release_capacity > 2:
                record_event(
                    session,
                    "DYNAMIC_RESTORE_CAPACITY_SCALED",
                    f"Raikou released restore capacity {release_capacity}/"
                    f"{settings.dynamic_restore_max_slots}: observed history indicates "
                    "additional restore overlap is needed to protect transfer continuity.",
                    source_id=wave.source_id,
                    wave_id=wave.id,
                )
            release_reason = (
                f"continuous lane below minimum buffer ({available_backlog_seconds:.0f}s < {int(minimum_buffer)}s)"
                if within_minimum else (
                    f"continuous lane below target buffer ({available_backlog_seconds:.0f}s < {int(target_buffer)}s)"
                    if replenish_lane else "planned restore time reached"
                )
            )
            record_event(session, "DYNAMIC_RESTORE_RELEASED",
                         f"Dynamic pipeline run {run.id} released restore for wave '{wave.name}' within release capacity {release_capacity} of materialized horizon {horizon}; {release_reason}",
                         source_id=wave.source_id, wave_id=wave.id)
            occupied += 1
            projected_backlog_seconds += estimated_wave_seconds
            released += 1
        refresh_dynamic_pipeline_run(session, run)
    return released


def repackage_unsubmitted_dynamic_waves(session: Session, settings: RuntimeSettings,
                                        run: DynamicPipelineRun, scheduler_now: datetime) -> int:
    """Repack only future waves after the planner receives new observations.

    A materialized wave is still editable until a Batch restore task exists.
    At that point AWS has not been called and every object is only in the
    local ``WAVE_ASSIGNED`` state, so returning the future slices to
    ``DISCOVERED`` and packing them again is safe, deterministic and fully
    auditable.  Anything submitted to AWS is deliberately excluded.
    """
    source = session.get(Source, run.source_id)
    if source is None or source.archived_at:
        return 0
    waves = list(session.scalars(select(Wave).where(Wave.pipeline_run_id == run.id).order_by(
        Wave.planned_transfer_start_at, Wave.id
    )))
    mutable: list[Wave] = []
    for wave in waves:
        if wave.status != "RESTORE_SCHEDULED":
            continue
        submitted = session.scalar(select(Task.id).where(
            Task.wave_id == wave.id, Task.kind == "SUBMIT_BATCH_RESTORE"
        ).limit(1))
        progressed = session.scalar(select(ObjectRecord.id).where(
            ObjectRecord.wave_id == wave.id,
            ObjectRecord.state != ObjectState.WAVE_ASSIGNED,
        ).limit(1))
        if submitted is None and progressed is None:
            mutable.append(wave)
    if not mutable:
        return 0

    mutable_ids = [wave.id for wave in mutable]
    previous = {
        wave.id: (wave.predicted_transfer_seconds, wave.prediction_samples,
                  tuple(session.scalars(select(ObjectRecord.id).where(
                      ObjectRecord.wave_id == wave.id
                  ).order_by(ObjectRecord.object_key, ObjectRecord.id))))
        for wave in mutable
    }
    # The objects are made eligible as one local transaction before selecting
    # the new deterministic slices. No external call is performed here.
    session.execute(update(ObjectRecord).where(ObjectRecord.wave_id.in_(mutable_ids)).values(
        wave_id=None, state=ObjectState.DISCOVERED, planned_transfer_seconds=0
    ))
    session.flush()
    profiles = transfer_history_profiles(session, source.id, settings.multipart_part_size_mib)
    preceding = [wave for wave in waves if wave.id not in set(mutable_ids)]
    changed = 0
    for wave in mutable:
        plan, latest_profiles, planner_settings = next_dynamic_wave_plan(
            session, source.id, run.target_max_bytes, run.target_transfer_seconds,
            run.max_objects, run.selection_prefix,
        )
        if plan is None:
            # A concurrent discovery/operation should not silently delete a
            # planned wave. Keep it empty and let the next governance cycle
            # resolve it with the durable inventory state.
            continue
        restore_at, transfer_at = dynamic_wave_schedule(run, preceding, scheduler_now)
        wave.max_bytes = run.target_max_bytes
        wave.predicted_transfer_seconds = plan["predicted_transfer_seconds"]
        wave.prediction_samples = plan["prediction_samples"]
        wave.planned_restore_at, wave.planned_transfer_start_at = restore_at, transfer_at
        assign_dynamic_wave_objects(session, wave, source.id, run.selection_prefix,
                                    plan, planner_settings, latest_profiles or profiles)
        current_ids = tuple(session.scalars(select(ObjectRecord.id).where(
            ObjectRecord.wave_id == wave.id
        ).order_by(ObjectRecord.object_key, ObjectRecord.id)))
        prior_duration, prior_samples, prior_ids = previous[wave.id]
        if (prior_ids != current_ids or prior_duration != wave.predicted_transfer_seconds
                or prior_samples != wave.prediction_samples):
            changed += 1
            record_event(
                session, "DYNAMIC_WAVE_REPACKED",
                f"Wave '{wave.name}' repacked before AWS submission using updated transfer observations; "
                f"{len(prior_ids)} -> {len(current_ids)} object(s), predicted transfer "
                f"{prior_duration}s -> {wave.predicted_transfer_seconds}s",
                source_id=source.id, wave_id=wave.id,
            )
        preceding.append(wave)
    return changed


def observed_transfer_lane_window(session: Session, wave: Wave) -> tuple[datetime | None, datetime | None]:
    """Return the measured shared-lane window for one wave.

    The lane is the operational clock for a continuous pipeline.  Summing
    per-object durations and dividing by a fixed worker floor is not a lane
    measurement: it can turn one saturated link hour into many days whenever
    Raikou scales above the minimum worker count.  Older CONTROL records with
    zero-width object segments are recovered from the durable virtual wave
    completion or the logical object duration.
    """
    rows = list(session.execute(
        select(TransferLaneSegment.started_at, TransferLaneSegment.completed_at,
               ObjectRecord.transfer_elapsed_seconds)
        .join(TransferQueueItem, TransferQueueItem.id == TransferLaneSegment.queue_item_id)
        .outerjoin(ObjectRecord, ObjectRecord.id == TransferQueueItem.object_id)
        .where(TransferLaneSegment.wave_id == wave.id)
    ))
    if not rows:
        return None, None
    starts: list[datetime] = []
    ends: list[datetime] = []
    for started_at, completed_at, logical_elapsed in rows:
        starts.append(started_at)
        if completed_at and completed_at > started_at:
            ends.append(completed_at)
        elif wave.transfer_completed_virtual_at is not None:
            ends.append(max(started_at, wave.transfer_completed_virtual_at))
        elif float(logical_elapsed or 0) > 0:
            ends.append(started_at + timedelta(seconds=float(logical_elapsed)))
        else:
            ends.append(started_at)
    return min(starts), max(ends)


def replan_dynamic_pipeline(session: Session, settings: RuntimeSettings, now: datetime | None = None) -> int:
    """Adapt only unsubmitted dynamic waves to durable observed timings.

    Restore requests, active waves and their audit record are immutable. The
    transfer *forecast* remains mutable: moving it never sends an AWS request
    and is required to keep the serial transfer lane truthful after an
    observed delay.  Simulation planning uses source-owned virtual milestones,
    never wall-clock timestamps produced by a fast CONTROL transfer.
    """
    changed = 0
    runs = list(session.scalars(select(DynamicPipelineRun).where(
        DynamicPipelineRun.scheduled_restores.is_(True),
        DynamicPipelineRun.status.not_in(["COMPLETED", "HISTORICAL"]),
    )))
    for run in runs:
        refresh_dynamic_pipeline_run(session, run)
        if run.status == "NEEDS_ATTENTION":
            continue
        scheduler_now = now or source_scheduler_clock(run.source).effective_now
        run_changed = False
        latest_profiles = transfer_history_profiles(session, run.source_id, settings.multipart_part_size_mib)
        latest_samples = sum(value["samples"] for value in latest_profiles.values())
        # A new completed transfer gives the packing model new evidence. Only
        # then recompose unsubmitted waves; routine scheduler cycles merely
        # adjust their times and do not churn object assignments.
        if latest_samples != int(run.historical_samples or 0):
            repacked = repackage_unsubmitted_dynamic_waves(session, settings, run, scheduler_now)
            run.historical_samples = latest_samples
            changed += repacked
            run_changed = bool(repacked)
        # Pipeline sequence is the materialization order. Ordering by an old
        # forecast can put a corrupted/past transfer slot before its own
        # restore request and recursively amplify the error.
        waves = list(session.scalars(select(Wave).where(Wave.pipeline_run_id == run.id).order_by(Wave.id)))
        cursor: datetime | None = None
        cursor_basis = "initial forecast"
        for wave in waves:
            actual_start, actual_end, elapsed_sum, elapsed_max = session.execute(select(
                func.min(ObjectRecord.transfer_started_at), func.max(ObjectRecord.transferred_at),
                func.sum(ObjectRecord.transfer_elapsed_seconds), func.max(ObjectRecord.transfer_elapsed_seconds),
            ).where(ObjectRecord.wave_id == wave.id)).one()
            planned_start = wave.planned_transfer_start_at or scheduler_now
            # The planner is also exercised against an isolated simulation
            # database in tests and recovery tools. The source itself is the
            # durable authority for whether its timing is virtual.
            simulated = wave.source.backend_kind == "SIMULATED"
            observed_virtual_start = wave.transfer_started_virtual_at if simulated else None
            observed_virtual_end = wave.transfer_completed_virtual_at if simulated else None
            lane_observed_start, lane_observed_end = observed_transfer_lane_window(session, wave)
            duration_seconds = max(1, int(wave.predicted_transfer_seconds or 1))
            observed_basis = "prediction"
            if lane_observed_start and lane_observed_end and lane_observed_end >= lane_observed_start:
                duration_seconds = max(1, int((lane_observed_end - lane_observed_start).total_seconds()))
                observed_virtual_start, observed_virtual_end = lane_observed_start, lane_observed_end
                observed_basis = "observed continuous lane segments"
            # Compatibility only: older CONTROL evidence may predate the
            # transfer-lane table entirely.  Once a lane segment exists the
            # branch above is mandatory, because summing object work is not a
            # measurement of a saturated, autoscaled shared lane.
            elif simulated and wave.source.simulation_fidelity == "CONTROL" and elapsed_sum:
                duration_seconds = max(
                    1,
                    math.ceil(max(float(elapsed_max or 0), float(elapsed_sum) / RAIJU_MIN_WORKERS)),
                )
                observed_basis = "legacy simulated logical transfer duration"
            elif observed_virtual_start and observed_virtual_end and observed_virtual_end >= observed_virtual_start:
                duration_seconds = max(1, int((observed_virtual_end - observed_virtual_start).total_seconds()))
                observed_basis = "observed simulated virtual milestones"
            elif actual_start and actual_end and actual_end >= actual_start:
                duration_seconds = max(1, int((actual_end - actual_start).total_seconds()))
                observed_basis = "observed transfer timestamps"
            has_batch_task = session.scalar(select(Task.id).where(
                Task.wave_id == wave.id, Task.kind == "SUBMIT_BATCH_RESTORE"
            ).limit(1)) is not None
            # Once a simulated wave begins, its virtual timestamp is the
            # authoritative lane position.  For a submitted restore that has
            # not produced an object yet, the only defensible transfer floor
            # is its real request plus the tier service window.  A stale plan
            # must never push that work out by days.
            if observed_virtual_start:
                lane_start = observed_virtual_start
            elif wave.first_restore_available_virtual_at:
                lane_start = wave.first_restore_available_virtual_at
            elif wave.restore_requested_virtual_at:
                lane_start = wave.restore_requested_virtual_at + timedelta(
                    seconds=restore_service_window_seconds(wave.restore_tier)
                )
            elif wave.status == "RESTORE_SCHEDULED" and not has_batch_task:
                # An unsubmitted horizon entry is intentionally mutable. Its
                # former calendar slot is an output of this planner, never an
                # input that can keep the rest of the lane stranded.
                lane_start = scheduler_now
            else:
                lane_start = planned_start
            # A submitted restore is immutable evidence.  A merely planned
            # restore is not: preserving it as a floor recursively pins every
            # later wave in the old calendar (the root cause of distant 48h
            # forecasts such as wave #68).
            restore_floor = wave.restore_requested_virtual_at
            if restore_floor is None and has_batch_task:
                restore_floor = wave.planned_restore_at
            if restore_floor:
                lane_start = max(lane_start, restore_floor)
            constrained_by_prior_wave = bool(cursor and cursor > lane_start)
            decision_basis = cursor_basis if constrained_by_prior_wave else observed_basis
            start = max(lane_start, cursor) if cursor else lane_start
            if observed_virtual_end:
                cursor = max(start, observed_virtual_end)
            elif observed_basis == "legacy simulated logical transfer duration" and actual_end:
                cursor = start + timedelta(seconds=duration_seconds)
            elif actual_end:
                cursor = max(start, actual_end)
            else:
                cursor = start + timedelta(seconds=duration_seconds)
            cursor_basis = observed_basis
            # A transfer forecast can safely be fixed even after its restore
            # was submitted. The restore timestamp itself is immutable once a
            # Batch task exists because changing it would falsify evidence.
            restore_lead = restore_service_window_seconds(wave.restore_tier) + int(run.restore_safety_seconds or settings.dynamic_restore_safety_seconds)
            new_restore_at = max(scheduler_now, start - timedelta(seconds=restore_lead))
            shifted = abs((wave.planned_transfer_start_at - start).total_seconds()) if wave.planned_transfer_start_at else float("inf")
            if shifted >= 60:
                prior_transfer_at = wave.planned_transfer_start_at
                prior_restore_at = wave.planned_restore_at
                wave.planned_transfer_start_at = start
                if not has_batch_task and wave.status == "RESTORE_SCHEDULED":
                    wave.planned_restore_at = new_restore_at
                changed += 1
                run_changed = True
                record_event(
                    session,
                    "DYNAMIC_WAVE_REPLANNED",
                    f"Wave '{wave.name}' moved from transfer {prior_transfer_at.isoformat() if prior_transfer_at else 'unset'} / restore {prior_restore_at.isoformat() if prior_restore_at else 'unset'} to transfer {start.isoformat()} / restore {new_restore_at.isoformat()}; basis: {decision_basis}, current duration {duration_seconds}s",
                    source_id=wave.source_id,
                    wave_id=wave.id,
                )
        if run_changed:
            run.planner_version = "v2-adaptive"
    if changed:
        record_event(session, "DYNAMIC_PIPELINE_REPLANNED",
                     f"Adapted {changed} unsubmitted dynamic wave schedule(s) from observed transfer timings")
    return changed


@app.get("/api/sources/{source_id}/waves/dynamic-preview")
def preview_dynamic_waves(source_id: int, prefix: str = Query(default="", max_length=1024),
                          restore_days: int = Query(ge=1, le=30),
                          restore_tier: str = Query(default="BULK", pattern="^(BULK|STANDARD)$"),
                          session: Session = Depends(get_session)) -> dict:
    source = active_source_or_409(session, source_id)
    target_transfer_seconds, reserve_seconds = automatic_dynamic_duration_limit(restore_days)
    result = dynamic_wave_plan(session, source_id, DYNAMIC_PLATFORM_MAX_BYTES, target_transfer_seconds,
                               DYNAMIC_PLATFORM_MAX_OBJECTS, prefix)
    plans = result["waves"]
    for plan in plans:
        plan["restore_tier"] = restore_tier
    scheduler_now = source_scheduler_clock(source).effective_now
    times = dynamic_schedule_times(scheduler_now, plans, result["settings"].dynamic_restore_safety_seconds)
    total_predicted_seconds = sum(int(plan["predicted_transfer_seconds"]) for plan in plans)
    historical_waves = sum(1 for plan in plans if plan["prediction_samples"] > 0)
    return {
        "objects": sum(plan["object_count"] for plan in plans), "bytes": sum(plan["bytes"] for plan in plans),
        "estimated_waves": len(plans), "target_transfer_seconds": target_transfer_seconds,
        "automatic_duration_reserve_seconds": reserve_seconds,
        "restore_retention_seconds": restore_days * 24 * 3600,
        "platform_max_bytes": DYNAMIC_PLATFORM_MAX_BYTES,
        "platform_max_objects": DYNAMIC_PLATFORM_MAX_OBJECTS, "profiles": result["profiles"],
        "historical_samples": sum(value["samples"] for value in result["profiles"].values()),
        "forecast": {"pipeline_completion_at": (times[-1][1] + timedelta(seconds=plans[-1]["predicted_transfer_seconds"])) if times else scheduler_now,
                     "total_predicted_transfer_seconds": total_predicted_seconds,
                     "restore_horizon_waves": result["settings"].dynamic_restore_horizon_waves,
                     "historical_waves": historical_waves, "cold_start_waves": len(plans) - historical_waves},
        "waves": [{"objects": plan["object_count"], "bytes": plan["bytes"],
                    "predicted_transfer_seconds": plan["predicted_transfer_seconds"],
                    "prediction_samples": plan["prediction_samples"], "exclusive": plan["exclusive"],
                    "planned_restore_at": times[index][0], "planned_transfer_start_at": times[index][1]}
                  for index, plan in enumerate(plans[:100])],
        "truncated": len(plans) > 100,
    }


@app.post("/api/sources/{source_id}/waves/dynamic", status_code=201)
def create_dynamic_waves(source_id: int, payload: DynamicWaveCreate, session: Session = Depends(get_session)) -> dict:
    source = active_source_or_409(session, source_id)
    require_non_overlapping_source_scope(session, source)
    settings = runtime_settings(session)
    target_transfer_seconds, reserve_seconds = automatic_dynamic_duration_limit(payload.restore_days)
    matching_objects, matching_bytes = session.execute(select(
        func.count(ObjectRecord.id), func.coalesce(func.sum(ObjectRecord.size_bytes), 0)
    ).where(*discovered_object_filters(source_id, payload.prefix))).one()
    if not matching_objects:
        raise HTTPException(status_code=409, detail="No discovered objects match this dynamic-wave selection")
    run = DynamicPipelineRun(
        source_id=source.id, planner_version="v4-adaptive-horizon",
        status="SCHEDULED",
        target_max_bytes=DYNAMIC_PLATFORM_MAX_BYTES, target_transfer_seconds=target_transfer_seconds,
        max_objects=DYNAMIC_PLATFORM_MAX_OBJECTS, restore_safety_seconds=settings.dynamic_restore_safety_seconds,
        restore_days=payload.restore_days, restore_tier=payload.restore_tier,
        transfer_strategy="AS_OBJECTS_AVAILABLE", scheduled_restores=True,
        selection_prefix=payload.prefix.strip(), next_sequence=1,
        restore_horizon_waves=settings.dynamic_restore_horizon_waves,
        historical_samples=sum(value["samples"] for value in transfer_history_profiles(
            session, source.id, settings.multipart_part_size_mib
        ).values()),
    )
    session.add(run); session.flush()
    created = materialize_dynamic_pipeline_horizon(
        session, settings, run, now=source_scheduler_clock(source).effective_now
    )
    release_dynamic_restore_horizon(session, settings)
    record_event(session, "DYNAMIC_PIPELINE_STARTED",
                 f"Dynamic pipeline run {run.id} started with horizon {run.restore_horizon_waves}; {len(created)} wave(s) materialized from {matching_objects} object(s) / {matching_bytes} byte(s). Future waves will be packed after observed transfer results.",
                 source_id=source.id)
    session.commit()
    return {"waves": len(created), "objects": matching_objects,
            "bytes": matching_bytes, "scheduled_restores": True,
            "historical_samples": run.historical_samples, "pipeline_run_id": run.id,
            "names": [wave.name for wave in created]}


@app.get("/api/sources/{source_id}/waves")
def list_waves(source_id: int, session: Session = Depends(get_session)) -> list[dict]:
    source_or_404(session, source_id)
    executed_wave_ids = set(session.scalars(
        select(Task.wave_id).join(Wave).where(Wave.source_id == source_id, Task.attempts > 0)
    ))
    progressed_wave_ids = set(session.scalars(
        select(ObjectRecord.wave_id).where(ObjectRecord.source_id == source_id, ObjectRecord.wave_id.is_not(None),
                                            ObjectRecord.state != ObjectState.WAVE_ASSIGNED)
    ))
    # A continuous dispatcher is source-scoped; queue ownership identifies
    # the wave whose bytes are actually being transferred.
    transferring_wave_ids = set(session.scalars(
        select(TransferQueueItem.wave_id).where(
            TransferQueueItem.source_id == source_id,
            TransferQueueItem.state == TransferQueueState.LEASED,
        )
    ))
    ready_transfer_wave_ids = set(session.scalars(
        select(TransferQueueItem.wave_id).where(
            TransferQueueItem.source_id == source_id,
            TransferQueueItem.state.in_([
                TransferQueueState.AVAILABLE, TransferQueueState.READY,
                TransferQueueState.MULTIPART_RESUME, TransferQueueState.RETRY_WAIT,
            ]),
        )
    ))
    rows = list(session.execute(
        select(Wave, func.count(ObjectRecord.id), func.coalesce(func.sum(ObjectRecord.size_bytes), 0),
               func.min(ObjectRecord.transfer_started_at), func.max(ObjectRecord.transferred_at))
        .outerjoin(ObjectRecord, ObjectRecord.wave_id == Wave.id)
        .where(Wave.source_id == source_id)
        .group_by(Wave.id)
        .order_by(Wave.id)
    ))
    wave_ids = [wave.id for wave, *_ in rows]
    timing_by_wave = {
        wave_id: {
            "requested_at": requested_at,
            "first_available_at": first_available_at,
            "last_available_at": last_available_at,
            "earliest_expiry_at": earliest_expiry_at,
            "restore_elapsed_seconds": max(0, int((last_available_at - requested_at).total_seconds()))
            if requested_at and last_available_at else None,
        }
        for wave_id, requested_at, first_available_at, last_available_at, earliest_expiry_at in session.execute(
            select(
                ObjectRecord.wave_id,
                func.min(ObjectRecord.restore_requested_at),
                func.min(ObjectRecord.restored_at),
                func.max(ObjectRecord.restored_at),
                func.min(ObjectRecord.restore_expires_at),
            )
            .where(ObjectRecord.wave_id.in_(wave_ids), ObjectRecord.restore_requested_at.is_not(None))
            .group_by(ObjectRecord.wave_id)
        )
    } if wave_ids else {}
    def displayed_status(wave: Wave) -> str:
        # A task lease is the source of truth while a worker owns the wave.
        # This also heals UI state left behind by a controlled worker restart:
        # a re-queued transfer must be RESTORED, never READY_FOR_RESTORE.
        # Partial release is deliberately dual-state: RESTORING remains the
        # primary lifecycle state while ``is_transferring`` renders the live
        # Raiju lane as a secondary tag in the interface.
        if wave.id in transferring_wave_ids and wave.status != "RESTORING":
            return "TRANSFERRING"
        if wave.id in ready_transfer_wave_ids and wave.status in {"READY_FOR_RESTORE", "RESTORED", "TRANSFERRING"}:
            return "RESTORED"
        return wave.status

    return [{"id": wave.id, "name": wave.name,
             "status": displayed_status(wave),
             "restore_tier": wave.restore_tier,
             "restore_days": wave.restore_days, "objects": count, "bytes": size, "batch_job_id": wave.batch_job_id,
             "planner_mode": wave.planner_mode, "predicted_transfer_seconds": wave.predicted_transfer_seconds,
             "prediction_samples": wave.prediction_samples, "planned_restore_at": wave.planned_restore_at,
             "planned_transfer_start_at": wave.planned_transfer_start_at,
             "restore_timing": timing_by_wave.get(wave.id, {}),
             "transfer_duration_seconds": int((finished - started).total_seconds()) if started and finished and displayed_status(wave) in {"COMPLETED", "TRANSFERRED", "VERIFIED"} else None,
             "last_poll_at": wave.last_poll_at,
             "can_delete": wave.id not in executed_wave_ids and wave.id not in progressed_wave_ids,
             "is_transferring": wave.id in transferring_wave_ids}
            for wave, count, size, started, finished in rows]


@app.delete("/api/waves/{wave_id}")
def delete_wave(wave_id: int, session: Session = Depends(get_session)) -> dict:
    wave = wave_or_404(session, wave_id)
    executed = session.scalar(select(Task.id).where(Task.wave_id == wave.id, Task.attempts > 0))
    progressed = session.scalar(select(ObjectRecord.id).where(
        ObjectRecord.wave_id == wave.id, ObjectRecord.state != ObjectState.WAVE_ASSIGNED
    ))
    if executed or progressed:
        raise HTTPException(status_code=409, detail="A wave with started restore, polling, transfer, or verification cannot be deleted")
    objects = list(session.scalars(select(ObjectRecord).where(ObjectRecord.wave_id == wave.id).with_for_update()))
    for obj in objects:
        obj.wave_id = None
        obj.state = ObjectState.DISCOVERED
    session.query(Event).filter(Event.wave_id == wave.id).delete(synchronize_session=False)
    session.query(Task).filter(Task.wave_id == wave.id).delete(synchronize_session=False)
    session.query(TransferQueueItem).filter(TransferQueueItem.wave_id == wave.id).delete(synchronize_session=False)
    source_id, name = wave.source_id, wave.name
    session.delete(wave)
    record_event(session, "WAVE_DELETED", f"Unexecuted wave '{name}' deleted; {len(objects)} object(s) returned to discovery", source_id=source_id)
    session.commit()
    return {"wave_id": wave_id, "objects_returned": len(objects)}


@app.get("/api/waves/{wave_id}/objects")
def wave_objects(wave_id: int, limit: int = 100, offset: int = 0, session: Session = Depends(get_session)) -> dict:
    wave_or_404(session, wave_id)
    limit = min(max(limit, 1), 1000)
    total = session.scalar(select(func.count(ObjectRecord.id)).where(ObjectRecord.wave_id == wave_id)) or 0
    rows = session.scalars(
        select(ObjectRecord).where(ObjectRecord.wave_id == wave_id).order_by(ObjectRecord.object_key).offset(offset).limit(limit)
    )
    return {"items": [{"id": obj.id, "key": obj.object_key, "size_bytes": obj.size_bytes,
                        "state": obj.state, "etag": obj.etag, "storage_class": obj.storage_class}
                      for obj in rows], "total": total, "limit": limit, "offset": offset}


@app.get("/api/waves/{wave_id}/manifest.csv")
def wave_manifest(wave_id: int, session: Session = Depends(get_session)) -> StreamingResponse:
    wave = session.get(Wave, wave_id)
    if not wave:
        raise HTTPException(status_code=404, detail="Wave not found")
    source = wave.source
    content = io.StringIO()
    writer = csv.writer(content, lineterminator="\n")
    for obj in session.scalars(select(ObjectRecord).where(ObjectRecord.wave_id == wave_id).order_by(ObjectRecord.object_key)):
        # S3 Batch Operations manifests require URL-encoded object keys. The
        # AWS adapter uploads this immutable text unchanged when credentials exist.
        from urllib.parse import quote
        row = [source.s3_bucket, quote(obj.object_key, safe="/")]
        if obj.version_id:
            row.append(obj.version_id)
        writer.writerow(row)
    filename = f"wave-{wave_id}-manifest.csv"
    return StreamingResponse(iter([content.getvalue()]), media_type="text/csv", headers={"Content-Disposition": f'attachment; filename="{filename}"'})


@app.get("/api/waves/{wave_id}/report")
def wave_report(wave_id: int, session: Session = Depends(get_session)) -> dict:
    wave = wave_or_404(session, wave_id)
    by_state = dict(session.execute(
        select(ObjectRecord.state, func.count(ObjectRecord.id))
        .where(ObjectRecord.wave_id == wave_id)
        .group_by(ObjectRecord.state)
    ).all())
    total_objects, total_bytes = session.execute(select(func.count(ObjectRecord.id), func.coalesce(func.sum(ObjectRecord.size_bytes), 0)).where(ObjectRecord.wave_id == wave_id)).one()
    integrity_verified, integrity_failed = session.execute(
        select(
            func.coalesce(func.sum(case((ObjectRecord.integrity_verified_at.is_not(None), 1), else_=0)), 0),
            func.coalesce(func.sum(case((ObjectRecord.integrity_error.is_not(None), 1), else_=0)), 0),
        ).where(ObjectRecord.wave_id == wave_id)
    ).one()
    attempts = list(session.scalars(select(RestoreAttempt).where(RestoreAttempt.wave_id == wave_id).order_by(RestoreAttempt.id.desc())))
    # Relationships have no implicit SQL order. Fetch the durable task
    # history explicitly so reports always tell the execution story in order.
    tasks = list(session.scalars(select(Task).where(Task.wave_id == wave_id).order_by(Task.id)))
    latest_poll = session.scalar(
        select(Task).where(Task.wave_id == wave_id, Task.kind == "POLL_RESTORE").order_by(Task.id.desc())
    )
    lane_segments = list(session.scalars(select(TransferLaneSegment).where(
        TransferLaneSegment.wave_id == wave_id
    ).order_by(TransferLaneSegment.started_at, TransferLaneSegment.id)))
    dispatch_batches = list(session.scalars(select(TransferDispatchBatch).where(
        TransferDispatchBatch.wave_id == wave_id
    ).order_by(TransferDispatchBatch.started_at, TransferDispatchBatch.id)))
    transfer_started_at, transfer_completed_at, transferred_files, transferred_bytes, failed_objects = session.execute(
        select(
            func.min(ObjectRecord.transfer_started_at),
            func.max(ObjectRecord.transferred_at),
            func.coalesce(func.sum(case((ObjectRecord.state.in_([ObjectState.TRANSFERRED, ObjectState.VERIFIED]), 1), else_=0)), 0),
            func.coalesce(func.sum(case((ObjectRecord.state.in_([ObjectState.TRANSFERRED, ObjectState.VERIFIED]), ObjectRecord.size_bytes), else_=0)), 0),
            func.coalesce(func.sum(case((ObjectRecord.state == ObjectState.FAILED, 1), else_=0)), 0),
        ).where(ObjectRecord.wave_id == wave_id)
    ).one()
    now = utcnow()
    transfer_elapsed_seconds = max(0, int(((transfer_completed_at or now) - transfer_started_at).total_seconds())) if transfer_started_at else None
    failed_tasks = sum(1 for task in tasks if task.state == TaskState.FAILED)
    timing = restore_timing(session, wave_id)
    attempt_diagnostics = {
        attempt.id: restore_result_diagnostics(session, attempt.id) if attempt.report_manifest_key else None
        for attempt in attempts
    }
    latest_attempt = attempts[0] if attempts else None
    restore_diagnosis = attempt_diagnostics.get(latest_attempt.id) if latest_attempt else None
    if latest_attempt and restore_diagnosis is None and latest_attempt.failure_summary:
        restore_diagnosis = {
            "action_required": True,
            "summary": latest_attempt.failure_summary,
            "recommended_action": "Import the AWS completion report before deciding whether a new restore submission is required.",
            "reasons": [],
            "raw_succeeded": int(latest_attempt.succeeded_objects or 0),
            "accepted_equivalent": 0,
            "effective_accepted": int(latest_attempt.succeeded_objects or 0),
            "unexpected_failed": int(latest_attempt.failed_objects or 0),
            "pending_evidence": 0,
        }
    if restore_diagnosis is not None:
        restore_diagnosis["can_retry_evidence"] = bool(
            wave.batch_job_id and latest_attempt and not latest_attempt.report_manifest_key
            and not session.scalar(select(Task.id).where(
                Task.wave_id == wave.id, Task.state.in_([TaskState.READY, TaskState.RUNNING])
            ))
        )
    # A completed restore can still become unavailable before the copy worker
    # consumes it (for example after its temporary restore window expires).
    # In that case the old Batch completion evidence remains useful history,
    # but it must not be presented as the current restore diagnosis.  The
    # operator needs an unambiguous reason for the required, potentially
    # billable, new restore approval.
    if wave.restore_reapproval_required:
        restore_diagnosis = {
            "action_required": True,
            "summary": "The previously restored copy is no longer available for transfer",
            "recommended_action": "Review the wave report and explicitly approve a new restore before reprocessing. A new restore may incur AWS charges.",
            "reasons": [{
                "code": "RESTORE_REAPPROVAL_REQUIRED",
                "http_status": None,
                "message": wave.restore_reapproval_reason or "A restored object is no longer available.",
                "count": int(total_objects or 0),
                "sample_keys": [],
            }],
            "raw_succeeded": int(latest_attempt.succeeded_objects or 0) if latest_attempt else 0,
            "accepted_equivalent": 0,
            "effective_accepted": 0,
            "unexpected_failed": 0,
            "pending_evidence": 0,
            "can_retry_evidence": False,
        }
    return {"wave_id": wave_id, "status": wave.status, "objects": total_objects, "bytes": total_bytes, "object_states": by_state,
            "summary": {"transfer_elapsed_seconds": transfer_elapsed_seconds,
                        "transfer_started_at": transfer_started_at, "transfer_completed_at": transfer_completed_at,
                        "transferred_files": int(transferred_files or 0), "transferred_bytes": int(transferred_bytes or 0),
                        "failed_objects": int(failed_objects or 0), "failed_tasks": failed_tasks,
                        "failures_or_errors": int(failed_objects or 0) + failed_tasks},
            "batch": {"job_id": wave.batch_job_id, "status": wave.batch_job_status, "manifest_key": wave.manifest_key, "last_poll_at": wave.last_poll_at, "poll_count": wave.poll_count},
            "restore_reapproval": {
                "required": bool(wave.restore_reapproval_required),
                "reason": wave.restore_reapproval_reason,
                "detected_at": wave.restore_reapproval_detected_at,
            },
            "restore_diagnosis": restore_diagnosis,
            "restore_attempts": [{"id": attempt.id, "job_id": attempt.job_id, "region": attempt.aws_region, "status": attempt.job_status,
                                  "expected": attempt.expected_objects, "succeeded": attempt.succeeded_objects, "failed": attempt.failed_objects,
                                  "manifest_key": attempt.manifest_key, "report_manifest_key": attempt.report_manifest_key,
                                  "submission": "AWS_ACCEPTED" if attempt.job_id else "NOT_SUBMITTED",
                                  "completion_evidence": "PUBLISHED" if attempt.report_manifest_key else "PENDING",
                                  "request_metrics": {"describe_requests": int(attempt.batch_describe_requests or 0),
                                                      "completion_report_list_requests": int(attempt.completion_report_list_requests or 0),
                                                      "completion_report_get_requests": int(attempt.completion_report_get_requests or 0)},
                                  "diagnosis": attempt_diagnostics.get(attempt.id),
                                  "failure_summary": attempt.failure_summary,
                                  "created_at": attempt.created_at, "completed_at": attempt.completed_at}
                                 for attempt in attempts],
            "restore_timing": timing,
            "polling": {"state": latest_poll.state if latest_poll else None, "error": latest_poll.error if latest_poll else None,
                        "method": "HeadObject (pending wave objects)",
                        "head_requests": int(wave.availability_head_requests or 0),
                        "elapsed_seconds": round(float(wave.availability_poll_elapsed_seconds or 0), 2),
                        "throttle_retries": int(wave.availability_throttle_retries or 0),
                        "last_checked_objects": int(wave.last_availability_poll_objects or 0),
                        "last_elapsed_seconds": round(float(wave.last_availability_poll_seconds or 0), 2)},
            "batch_evidence_polling": {"describe_requests": int(sum(attempt.batch_describe_requests or 0 for attempt in attempts)),
                                       "completion_report_list_requests": int(sum(attempt.completion_report_list_requests or 0 for attempt in attempts)),
                                       "completion_report_get_requests": int(sum(attempt.completion_report_get_requests or 0 for attempt in attempts))},
            "integrity": {"verified": integrity_verified, "failed": integrity_failed, "pending": total_objects - integrity_verified - integrity_failed},
            "continuous_lane": {
                "dispatch_batches": [{
                    "id": batch.id, "state": batch.state,
                    "priority_band": batch.priority_band,
                    "priority_score": int(batch.priority_score or 0),
                    "object_limit": int(batch.object_limit or 0),
                    "byte_limit": int(batch.byte_limit or 0),
                    "object_count": int(batch.object_count or 0),
                    "bytes_planned": int(batch.bytes_planned or 0),
                    "worker_target": int(batch.worker_target or 0),
                    "reason": batch.reason, "preempted_batch_id": batch.preempted_batch_id,
                    "started_at": batch.started_at, "completed_at": batch.completed_at,
                } for batch in dispatch_batches],
                "segments": [{
                    "id": segment.id, "worker_slot": segment.worker_slot,
                    "started_at": segment.started_at, "completed_at": segment.completed_at,
                    "bytes_transferred": int(segment.bytes_transferred or 0),
                    "object_count": int(segment.object_count or 0),
                    "entry_reason": segment.entry_reason, "exit_reason": segment.exit_reason,
                    "nearest_expiry_at": segment.nearest_expiry_at,
                } for segment in lane_segments],
                "dispatches": len(lane_segments),
                "bytes_transferred": int(sum(segment.bytes_transferred or 0 for segment in lane_segments)),
            },
            "tasks": [{"id": t.id, "kind": t.kind, "state": t.state, "attempts": t.attempts, "error": t.error} for t in tasks]}


@app.get("/api/waves/{wave_id}/cost-estimate")
def get_wave_cost_estimate(wave_id: int, session: Session = Depends(get_session)) -> dict:
    if not runtime_settings(session).cost_estimation_enabled:
        raise HTTPException(status_code=409, detail="Cost estimation is disabled in operational settings")
    return wave_cost_estimate(session, wave_or_404(session, wave_id))


@app.get("/api/sources/{source_id}/cost-estimate")
def get_source_cost_estimate(source_id: int, session: Session = Depends(get_session)) -> dict:
    if not runtime_settings(session).cost_estimation_enabled:
        raise HTTPException(status_code=409, detail="Cost estimation is disabled in operational settings")
    source = source_or_404(session, source_id)
    waves = list(session.scalars(select(Wave).where(Wave.source_id == source.id).order_by(Wave.id)))
    estimates = [wave_cost_estimate(session, wave) for wave in waves]
    unassigned_objects, unassigned_bytes = session.execute(select(func.count(ObjectRecord.id), func.coalesce(func.sum(ObjectRecord.size_bytes), 0)).where(ObjectRecord.source_id == source.id, ObjectRecord.wave_id.is_(None))).one()
    def total(key: str):
        values = [item["totals"][key] for item in estimates]
        return round(sum(values), 6) if values and all(value is not None for value in values) else None
    observed_values = [item["observed_temporary_restore"] for item in estimates]
    observed_costs = [item["cost"] for item in observed_values]
    return {"source_id": source.id, "source_name": source.name, "connection_label": source.aws_connection.label if source.aws_connection else None,
            "currency": estimates[0]["currency"] if estimates else "USD",
            "waves": len(estimates), "estimated_objects": sum(item["quantities"]["objects"] for item in estimates),
            "unassigned_objects": unassigned_objects, "unassigned_bytes": unassigned_bytes,
            "complete": bool(estimates) and not unassigned_objects and all(item["complete"] for item in estimates),
            "totals": {"one_time": total("one_time"), "recurring_monthly": total("recurring_monthly"), "optional_deep_audit": total("optional_deep_audit")},
            "observed_temporary_restore": {
                "objects": sum(int(item["objects"] or 0) for item in observed_values),
                "in_progress_objects": sum(int(item["in_progress_objects"] or 0) for item in observed_values),
                "gib_months": round(sum(float(item["gib_months"] or 0) for item in observed_values), 9),
                "cost": round(sum(float(value) for value in observed_costs), 6) if observed_costs and all(value is not None for value in observed_costs) else None,
                "complete": bool(observed_values) and all(item["complete"] for item in observed_values),
            },
            "total_completeness": {key: bool(estimates) and all(item["total_completeness"][key] for item in estimates)
                                   for key in ("one_time", "recurring_monthly", "optional_deep_audit")},
            "wave_estimates": [{"wave_id": item["wave_id"], "one_time": item["totals"]["one_time"], "complete": item["complete"]} for item in estimates]}


@app.get("/api/waves/{wave_id}/deep-audit-preview")
def deep_audit_preview(wave_id: int, session: Session = Depends(get_session)) -> dict:
    wave = wave_or_404(session, wave_id)
    if wave.status not in {"COMPLETED", "TRANSFERRED", "TRANSFERRED_WITH_ERRORS", "VERIFICATION_FAILED"}:
        raise HTTPException(status_code=409, detail="Integrity verification can only be requested after transfer completes")
    objects, total_bytes, multipart_evidence_objects = session.execute(select(
        func.count(ObjectRecord.id), func.coalesce(func.sum(ObjectRecord.size_bytes), 0),
        func.coalesce(func.sum(case((ObjectRecord.checksum_algorithm == "SHA256_MULTIPART_PARTS", 1), else_=0)), 0),
    ).where(ObjectRecord.wave_id == wave.id)).one()
    throughput_mbps = runtime_settings(session).max_throughput_mbps
    denominator = max(1, throughput_mbps * 1_000_000)
    minimum_seconds = max(1, (int(total_bytes) * 8 + denominator - 1) // denominator) if total_bytes else 0
    return {"wave_id": wave.id, "wave_name": wave.name, "source_name": wave.source.name,
            "objects": int(objects), "bytes": int(total_bytes), "multipart_evidence_objects": int(multipart_evidence_objects), "throughput_mbps": throughput_mbps,
            "minimum_seconds": minimum_seconds}


@app.post("/api/waves/{wave_id}/verify")
def verify_wave(wave_id: int, payload: DeepAuditStart, session: Session = Depends(get_session)) -> dict:
    """Queue a costly full OCI reread only after an explicit acknowledgement."""
    if not payload.confirmed:
        raise HTTPException(status_code=422, detail="Explicit deep-audit confirmation is required")
    wave = wave_or_404(session, wave_id)
    if wave.status not in {"COMPLETED", "TRANSFERRED", "TRANSFERRED_WITH_ERRORS", "VERIFICATION_FAILED"}:
        raise HTTPException(status_code=409, detail="Integrity verification can only be requested after transfer completes")
    queued = session.scalar(select(Task.id).where(
        Task.wave_id == wave.id, Task.kind == "VERIFY_WAVE", Task.state.in_([TaskState.READY, TaskState.RUNNING])
    ))
    if queued:
        raise HTTPException(status_code=409, detail="Integrity verification is already queued or running for this wave")
    wave.status = "VERIFICATION_QUEUED"
    session.add(Task(wave_id=wave.id, kind="VERIFY_WAVE"))
    record_event(session, "DEEP_AUDIT_QUEUED", f"Full OCI SHA-256 reread explicitly approved for wave '{wave.name}'", source_id=wave.source_id, wave_id=wave.id)
    session.commit()
    return {"wave_id": wave.id, "status": wave.status, "message": "Deep audit queued"}


@app.post("/api/waves/{wave_id}/pause")
def pause_wave(wave_id: int, session: Session = Depends(get_session)) -> dict:
    wave = wave_or_404(session, wave_id)
    if wave.status == "PAUSED":
        return {"wave_id": wave.id, "status": wave.status}
    cancelled = cancel_active_wave_tasks(session, wave, "wave paused by operator")
    wave.status = "PAUSED"
    record_event(session, "WAVE_PAUSED", f"Wave '{wave.name}' paused by operator; {cancelled} pending work item(s) cancelled", source_id=wave.source_id, wave_id=wave.id)
    session.commit()
    return {"wave_id": wave.id, "status": wave.status}


@app.post("/api/waves/{wave_id}/resume")
def resume_wave(wave_id: int, session: Session = Depends(get_session)) -> dict:
    wave = wave_or_404(session, wave_id)
    if wave.status != "PAUSED":
        raise HTTPException(status_code=409, detail="Only a paused wave can be resumed")
    active_task = session.scalar(
        select(Task)
        .where(Task.wave_id == wave.id, Task.state.in_([TaskState.READY, TaskState.RUNNING]))
        .order_by(Task.id.desc())
    )
    # A pause can happen while restore polling or copy work is already queued.
    # Preserve that durable step: submitting another Batch restore would be both
    # unnecessary and potentially billable.
    status_by_task = {
        "SUBMIT_BATCH_RESTORE": "READY_FOR_RESTORE",
        "POLL_RESTORE": "RESTORING",
        "TRANSFER_CONTINUOUS": "TRANSFER_DRAINING",
        "VERIFY_WAVE": "VERIFICATION_QUEUED",
    }
    restored_items = list(session.scalars(select(TransferQueueItem).where(
        TransferQueueItem.wave_id == wave.id,
        TransferQueueItem.state == TransferQueueState.CANCELLED,
    )))
    for item in restored_items:
        obj = session.get(ObjectRecord, item.object_id)
        if obj and obj.state == ObjectState.RESTORED:
            item.state, item.decision_reason = TransferQueueState.READY, "Reactivated after operator resume"
    if restored_items:
        wave.status = "TRANSFER_DRAINING"
        # A transfer task is anchored to any eligible wave of the same source;
        # Raikou will create it in its next reconciliation pass if none exists.
        resumed_step = "TRANSFER_CONTINUOUS"
    elif active_task:
        wave.status = status_by_task.get(active_task.kind, "READY_FOR_RESTORE")
        resumed_step = active_task.kind
    else:
        wave.status = "READY_FOR_RESTORE"
        session.add(Task(wave_id=wave.id, kind="SUBMIT_BATCH_RESTORE"))
        resumed_step = "SUBMIT_BATCH_RESTORE"
    record_event(
        session,
        "WAVE_RESUMED",
        f"Wave '{wave.name}' resumed at durable step {resumed_step}",
        source_id=wave.source_id,
        wave_id=wave.id,
    )
    session.commit()
    return {"wave_id": wave.id, "status": wave.status}


@app.post("/api/waves/{wave_id}/queue")
def queue_planned_wave(wave_id: int, session: Session = Depends(get_session)) -> dict:
    wave = wave_or_404(session, wave_id)
    if wave.status != "PLANNED":
        raise HTTPException(status_code=409, detail="Only a planned wave can be added to the queue")
    wave.status = "READY_FOR_RESTORE"
    session.add(Task(wave_id=wave.id, kind="SUBMIT_BATCH_RESTORE"))
    record_event(session, "WAVE_QUEUED", f"Wave '{wave.name}' added to the restore queue by operator", source_id=wave.source_id, wave_id=wave.id)
    session.commit()
    return {"wave_id": wave.id, "status": wave.status}


@app.post("/api/sources/{source_id}/waves/queue-all")
def queue_all_planned_waves(source_id: int, session: Session = Depends(get_session)) -> dict:
    source = active_source_or_409(session, source_id)
    waves = list(session.scalars(select(Wave).where(Wave.source_id == source.id, Wave.status == "PLANNED").order_by(Wave.id)))
    for wave in waves:
        wave.status = "READY_FOR_RESTORE"
        session.add(Task(wave_id=wave.id, kind="SUBMIT_BATCH_RESTORE"))
        record_event(session, "WAVE_QUEUED", f"Wave '{wave.name}' added to the restore queue by operator", source_id=source.id, wave_id=wave.id)
    session.commit()
    return {"queued": len(waves)}


@app.post("/api/waves/{wave_id}/reprocess")
def reprocess_wave(wave_id: int, payload: WaveReprocessRequest | None = None, session: Session = Depends(get_session)) -> dict:
    """Queue a new controlled restore submission; no external call is made here."""
    wave = wave_or_404(session, wave_id)
    if wave.restore_reapproval_required and not (payload and payload.approve_new_restore):
        raise HTTPException(
            status_code=409,
            detail=("A temporary restored copy is no longer available. A new restore may incur AWS charges; "
                    "review the wave report and explicitly approve the new restore before reprocessing."),
        )
    if wave.status == "PAUSED":
        raise HTTPException(status_code=409, detail="Resume the wave before reprocessing it")
    queued = session.scalar(select(Task.id).where(Task.wave_id == wave.id, Task.state.in_([TaskState.READY, TaskState.RUNNING])))
    if queued:
        raise HTTPException(status_code=409, detail="This wave already has a queued or running task")
    if wave.batch_job_id and not session.scalar(select(RestoreAttempt.id).where(RestoreAttempt.job_id == wave.batch_job_id)):
        session.add(RestoreAttempt(wave_id=wave.id, aws_region=wave.source.aws_region, job_id=wave.batch_job_id,
                                   job_status=wave.batch_job_status, manifest_key=wave.manifest_key,
                                   manifest_etag=wave.manifest_etag, expected_objects=session.scalar(select(func.count(ObjectRecord.id)).where(ObjectRecord.wave_id == wave.id)) or 0,
                                   failure_summary="Historic Batch job retained while starting a new restore attempt"))
    reset_states = [ObjectState.RESTORE_REQUESTED, ObjectState.RESTORE_REQUEST_ACCEPTED, ObjectState.RESTORING, ObjectState.RESTORED, ObjectState.TRANSFERRING]
    for obj in session.scalars(select(ObjectRecord).where(ObjectRecord.wave_id == wave.id, ObjectRecord.state.in_(reset_states))):
        obj.state, obj.restored_at = ObjectState.WAVE_ASSIGNED, None
    session.query(TransferQueueItem).filter(TransferQueueItem.wave_id == wave.id).delete(synchronize_session=False)
    wave.status, wave.batch_job_id, wave.batch_job_status = "READY_FOR_RESTORE", None, None
    wave.manifest_key, wave.manifest_etag, wave.last_poll_at, wave.poll_count = None, None, None, 0
    wave.availability_head_requests, wave.availability_poll_elapsed_seconds = 0, 0
    wave.availability_throttle_retries = 0
    wave.last_availability_poll_objects, wave.last_availability_poll_seconds = 0, 0
    wave.restore_reapproval_required, wave.restore_reapproval_reason = False, None
    wave.restore_reapproval_detected_at = None
    session.add(Task(wave_id=wave.id, kind="SUBMIT_BATCH_RESTORE"))
    record_event(session, "WAVE_REPROCESS_QUEUED", f"New restore submission queued for wave '{wave.name}' by operator", source_id=wave.source_id, wave_id=wave.id)
    session.commit()
    return {"wave_id": wave.id, "status": wave.status, "message": "Restore task queued"}


@app.post("/api/waves/{wave_id}/retry-restore-evidence")
def retry_restore_evidence(wave_id: int, session: Session = Depends(get_session)) -> dict:
    """Re-read evidence for an accepted Batch job without submitting a new paid restore."""
    wave = wave_or_404(session, wave_id)
    attempt = session.scalar(
        select(RestoreAttempt).where(RestoreAttempt.wave_id == wave.id).order_by(RestoreAttempt.id.desc())
    )
    if not wave.batch_job_id or not attempt or attempt.job_id != wave.batch_job_id:
        raise HTTPException(status_code=409, detail="No current AWS Batch restore job is available for evidence recovery")
    queued = session.scalar(select(Task.id).where(
        Task.wave_id == wave.id, Task.state.in_([TaskState.READY, TaskState.RUNNING])
    ))
    if queued:
        raise HTTPException(status_code=409, detail="This wave already has a queued or running task")
    if attempt.report_manifest_key:
        diagnosis = restore_result_diagnostics(session, attempt.id)
        if diagnosis["action_required"]:
            raise HTTPException(status_code=409, detail=diagnosis["recommended_action"])
    wave.status = "RESTORE_REQUESTED"
    session.add(Task(wave_id=wave.id, kind="POLL_RESTORE"))
    record_event(
        session, "RESTORE_EVIDENCE_RECOVERY_QUEUED",
        f"AWS completion evidence recovery queued for existing Batch job {wave.batch_job_id}; no new restore was submitted",
        source_id=wave.source_id, wave_id=wave.id,
    )
    session.commit()
    return {"wave_id": wave.id, "status": wave.status,
            "message": "Completion evidence recovery queued; no new restore job was submitted"}


@app.get("/api/tasks")
def list_tasks(limit: int = 100, state: TaskState | None = None, wave_id: int | None = None, session: Session = Depends(get_session)) -> list[dict]:
    limit = min(max(limit, 1), 500)
    query = select(Task)
    if state is not None:
        query = query.where(Task.state == state)
    if wave_id is not None:
        query = query.where(Task.wave_id == wave_id)
    tasks = session.scalars(query.order_by(Task.available_at, Task.id).limit(limit))
    return [{"id": task.id, "wave_id": task.wave_id, "kind": task.kind, "state": task.state,
             "attempts": task.attempts, "available_at": task.available_at,
             "lease_expires_at": task.lease_expires_at, "worker_id": task.worker_id, "error": task.error}
            for task in tasks]


@app.get("/api/tasks.csv")
def export_tasks(session: Session = Depends(get_session)) -> StreamingResponse:
    content = io.StringIO()
    writer = csv.writer(content, lineterminator="\n")
    writer.writerow(["id", "wave_id", "kind", "state", "attempts", "available_at", "lease_expires_at", "worker_id", "error"])
    for task in session.scalars(select(Task).order_by(Task.id)):
        writer.writerow([task.id, task.wave_id, task.kind, task.state, task.attempts, task.available_at.isoformat(),
                         task.lease_expires_at.isoformat() if task.lease_expires_at else "", task.worker_id or "", task.error or ""])
    return StreamingResponse(iter([content.getvalue()]), media_type="text/csv", headers={"Content-Disposition": 'attachment; filename="tasks.csv"'})


@app.get("/api/events")
def list_events(limit: int = 100, source_id: int | None = None, wave_id: int | None = None, session: Session = Depends(get_session)) -> list[dict]:
    limit = min(max(limit, 1), 500)
    # An event can be associated directly with a source, only with a wave, or
    # with both. Join both paths so the operational history remains readable
    # when wave names are reused by different sources.
    wave_source = aliased(Source)
    query = (
        select(
            Event,
            func.coalesce(Source.id, wave_source.id).label("resolved_source_id"),
            func.coalesce(Source.name, wave_source.name).label("source_name"),
            Wave.name.label("wave_name"),
        )
        .outerjoin(Source, Event.source_id == Source.id)
        .outerjoin(Wave, Event.wave_id == Wave.id)
        .outerjoin(wave_source, Wave.source_id == wave_source.id)
    )
    if source_id is not None:
        query = query.where(or_(Event.source_id == source_id, Wave.source_id == source_id))
    if wave_id is not None:
        query = query.where(Event.wave_id == wave_id)
    rows = session.execute(query.order_by(Event.created_at.desc(), Event.id.desc()).limit(limit))
    return [{"id": event.id, "kind": event.kind, "message": event.message,
             "source_id": resolved_source_id, "source_name": source_name,
             "wave_id": event.wave_id, "wave_name": wave_name,
             "created_at": event.created_at}
            for event, resolved_source_id, source_name, wave_name in rows]


@app.get("/api/events.csv")
def export_events(session: Session = Depends(get_session)) -> StreamingResponse:
    content = io.StringIO()
    writer = csv.writer(content, lineterminator="\n")
    writer.writerow(["id", "created_at", "kind", "source_name", "source_id", "wave_name", "wave_id", "message"])
    wave_source = aliased(Source)
    rows = session.execute(
        select(
            Event,
            func.coalesce(Source.id, wave_source.id).label("resolved_source_id"),
            func.coalesce(Source.name, wave_source.name).label("source_name"),
            Wave.name.label("wave_name"),
        )
        .outerjoin(Source, Event.source_id == Source.id)
        .outerjoin(Wave, Event.wave_id == Wave.id)
        .outerjoin(wave_source, Wave.source_id == wave_source.id)
        .order_by(Event.created_at.desc(), Event.id.desc())
    )
    for event, resolved_source_id, source_name, wave_name in rows:
        writer.writerow([event.id, event.created_at.isoformat(), event.kind, source_name or "", resolved_source_id or "",
                         wave_name or "", event.wave_id or "", event.message])
    return StreamingResponse(iter([content.getvalue()]), media_type="text/csv", headers={"Content-Disposition": 'attachment; filename="events.csv"'})


@app.post("/api/tasks/claim")
def claim_task(payload: ClaimRequest, session: Session = Depends(get_session)) -> dict | None:
    now = utcnow()
    expired = Task.state == TaskState.RUNNING
    available = (Task.state == TaskState.READY) | (expired & (Task.lease_expires_at < now))
    task = session.scalar(select(Task).join(Wave).join(Source).where(
        available, Task.available_at <= now, Wave.status != "PAUSED", Source.archived_at.is_(None)
    ).order_by(Task.available_at, Task.id).with_for_update(skip_locked=True).limit(1))
    if not task:
        return None
    task.state = TaskState.RUNNING
    task.worker_id = payload.worker_id
    task.attempts += 1
    task.lease_expires_at = now + timedelta(seconds=payload.lease_seconds)
    record_event(session, "TASK_CLAIMED", f"Task {task.id} claimed by worker '{payload.worker_id}'", wave_id=task.wave_id)
    session.commit()
    return {"task_id": task.id, "kind": task.kind, "wave_id": task.wave_id, "attempt": task.attempts, "lease_expires_at": task.lease_expires_at}


@app.post("/api/tasks/{task_id}/heartbeat")
def heartbeat_task(task_id: int, payload: ClaimRequest, session: Session = Depends(get_session)) -> dict:
    task = task_or_404(session, task_id)
    if task.state != TaskState.RUNNING or task.worker_id != payload.worker_id:
        raise HTTPException(status_code=409, detail="Task is not leased by this worker")
    task.lease_expires_at = utcnow() + timedelta(seconds=payload.lease_seconds)
    session.commit()
    return {"task_id": task.id, "lease_expires_at": task.lease_expires_at}


@app.post("/api/tasks/{task_id}/succeed")
def succeed_task(task_id: int, payload: TaskUpdate, session: Session = Depends(get_session)) -> dict:
    task = task_or_404(session, task_id)
    if task.state != TaskState.RUNNING or task.worker_id != payload.worker_id:
        raise HTTPException(status_code=409, detail="Task is not leased by this worker")
    task.state = TaskState.SUCCEEDED
    task.lease_expires_at = None
    task.error = None
    record_event(session, "TASK_SUCCEEDED", f"Task {task.id} succeeded", wave_id=task.wave_id)
    session.commit()
    return {"task_id": task.id, "state": task.state}


@app.post("/api/tasks/{task_id}/fail")
def fail_task(task_id: int, payload: TaskUpdate, session: Session = Depends(get_session)) -> dict:
    task = task_or_404(session, task_id)
    if task.state != TaskState.RUNNING or task.worker_id != payload.worker_id:
        raise HTTPException(status_code=409, detail="Task is not leased by this worker")
    task.state = TaskState.READY
    task.available_at = utcnow() + timedelta(seconds=payload.retry_after_seconds)
    task.lease_expires_at = None
    task.error = payload.error or "Worker reported failure"
    record_event(session, "TASK_RETRY_QUEUED", f"Task {task.id} returned to queue: {task.error}", wave_id=task.wave_id)
    session.commit()
    return {"task_id": task.id, "state": task.state, "available_at": task.available_at}


@app.post("/api/tasks/recover")
def recover_expired_tasks(session: Session = Depends(get_session)) -> dict:
    now = utcnow()
    expired = list(session.scalars(select(Task).where(Task.state == TaskState.RUNNING, Task.lease_expires_at < now).with_for_update(skip_locked=True)))
    for task in expired:
        task.state = TaskState.READY
        task.available_at = now
        task.worker_id = None
        task.lease_expires_at = None
        task.error = "Lease expired; task recovered after worker interruption"
        record_event(session, "TASK_RECOVERED", f"Task {task.id} recovered after lease expiration", wave_id=task.wave_id)
    session.commit()
    return {"recovered": len(expired)}
