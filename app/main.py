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
from urllib.request import Request, urlopen
from urllib.parse import urlparse

from fastapi import Depends, FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from sqlalchemy import BigInteger, DateTime, Float, ForeignKey, Index, Integer, String, Text, and_, case, create_engine, func, inspect, or_, select, text
from sqlalchemy.engine import URL
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, aliased, mapped_column, relationship, sessionmaker


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


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
password = read_secret(os.environ["POSTGRES_PASSWORD_FILE"])
database_url = database_url.replace("migration@", f"migration:{password}@")
platform_status_file = os.environ.get("PLATFORM_STATUS_FILE", "/run/platform-status/status.json")
oci_runtime_config_file = os.environ.get("OCI_RUNTIME_CONFIG_FILE", "/run/oci-runtime/oci-runtime.json")
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
    s3_prefix: Mapped[str] = mapped_column(String(1024), default="")
    aws_region: Mapped[str] = mapped_column(String(64))
    aws_bucket_region: Mapped[str | None] = mapped_column(String(64), nullable=True)
    aws_connection_id: Mapped[int | None] = mapped_column(ForeignKey("aws_connections.id"), nullable=True, index=True)
    destination_bucket: Mapped[str] = mapped_column(String(255))
    # The conservative default is durable and explicit. The governance worker
    # will later interpret AS_OBJECTS_AVAILABLE without changing source history.
    transfer_strategy: Mapped[str] = mapped_column(String(32), default="AFTER_ALL_RESTORED")
    status: Mapped[str] = mapped_column(String(32), default="CONFIGURED")
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
    discovery_pages_completed: Mapped[int] = mapped_column(Integer, default=0)
    discovery_objects_inserted: Mapped[int] = mapped_column(BigInteger, default=0)
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


class AwsConnection(Base):
    """Immutable local identity for one customer-managed AWS credential Secret."""
    __tablename__ = "aws_connections"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    label: Mapped[str] = mapped_column(String(255), unique=True)
    secret_ocid: Mapped[str] = mapped_column(String(255), unique=True)
    aws_account_id: Mapped[str] = mapped_column(String(12), index=True)
    default_region: Mapped[str] = mapped_column(String(64))
    control_bucket: Mapped[str] = mapped_column(String(255))
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
    planned_restore_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    planned_transfer_start_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    source: Mapped[Source] = relationship(back_populates="waves")
    objects: Mapped[list[ObjectRecord]] = relationship(back_populates="wave")
    tasks: Mapped[list[Task]] = relationship(back_populates="wave")


class DynamicPipelineRun(Base):
    """Immutable planning snapshot for one dynamic-wave creation operation."""
    __tablename__ = "dynamic_pipeline_runs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"), index=True)
    planner_version: Mapped[str] = mapped_column(String(32), default="v1")
    status: Mapped[str] = mapped_column(String(32), default="PLANNED", index=True)
    target_max_bytes: Mapped[int] = mapped_column(BigInteger, default=0)
    target_transfer_seconds: Mapped[int] = mapped_column(Integer, default=0)
    max_objects: Mapped[int] = mapped_column(Integer, default=0)
    restore_safety_seconds: Mapped[int] = mapped_column(Integer, default=0)
    restore_horizon_waves: Mapped[int] = mapped_column(Integer, default=2)
    restore_days: Mapped[int] = mapped_column(Integer, default=0)
    restore_tier: Mapped[str] = mapped_column(String(16), default="BULK")
    transfer_strategy: Mapped[str] = mapped_column(String(32), default="AFTER_ALL_RESTORED")
    scheduled_restores: Mapped[bool] = mapped_column(default=False)
    historical_samples: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)


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


class DiscoveryJob(Base):
    """Durable, observable queue record for one remote S3 discovery run."""
    __tablename__ = "discovery_jobs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"), index=True)
    mode: Mapped[str] = mapped_column(String(32), default="REMOTE_LIST")
    inventory_manifest_uri: Mapped[str | None] = mapped_column(String(4096), nullable=True)
    inventory_file_index: Mapped[int] = mapped_column(Integer, default=0)
    inventory_rows_completed: Mapped[int] = mapped_column(BigInteger, default=0)
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


class RuntimeSettings(Base):
    __tablename__ = "runtime_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    transfer_workers: Mapped[int] = mapped_column(Integer, default=4)
    max_throughput_mbps: Mapped[int] = mapped_column(Integer, default=1100)
    multipart_part_size_mib: Mapped[int] = mapped_column(Integer, default=64)
    default_wave_size_bytes: Mapped[int] = mapped_column(BigInteger, default=10 * 1024**4)
    default_restore_days: Mapped[int] = mapped_column(Integer, default=7)
    default_restore_tier: Mapped[str] = mapped_column(String(16), default="BULK")
    task_lease_seconds: Mapped[int] = mapped_column(Integer, default=300)
    simulation_enabled: Mapped[bool] = mapped_column(default=False)
    aws_migration_role_arn: Mapped[str] = mapped_column(String(2048), default="")
    aws_batch_role_arn: Mapped[str] = mapped_column(String(2048), default="")
    aws_control_bucket: Mapped[str] = mapped_column(String(255), default="")
    aws_control_prefix: Mapped[str] = mapped_column(String(1024), default="s3-oci-control/")
    preserve_s3_tags: Mapped[bool] = mapped_column(default=True)
    real_worker_enabled: Mapped[bool] = mapped_column(default=False)
    cost_estimation_enabled: Mapped[bool] = mapped_column(default=False)
    cost_pricing_auto_refresh_enabled: Mapped[bool] = mapped_column(default=True)
    cost_pricing_refresh_days: Mapped[int] = mapped_column(Integer, default=7)
    activity_auto_refresh_enabled: Mapped[bool] = mapped_column(default=True)
    activity_refresh_seconds: Mapped[int] = mapped_column(Integer, default=15)
    dynamic_wave_target_seconds: Mapped[int] = mapped_column(Integer, default=12 * 3600)
    dynamic_wave_max_objects: Mapped[int] = mapped_column(Integer, default=50000)
    dynamic_restore_safety_seconds: Mapped[int] = mapped_column(Integer, default=6 * 3600)
    dynamic_restore_horizon_waves: Mapped[int] = mapped_column(Integer, default=2)
    dynamic_pipeline_enabled: Mapped[bool] = mapped_column(default=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class SourceCreate(BaseModel):
    name: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{1,127}$")
    s3_bucket: str
    s3_prefix: str = ""
    aws_region: str
    aws_connection_id: int | None = None
    destination_bucket: str
    transfer_strategy: str = Field(default="AFTER_ALL_RESTORED", pattern="^(AFTER_ALL_RESTORED|AS_OBJECTS_AVAILABLE)$")


class SourceUpdate(SourceCreate):
    pass


class SourceTransferStrategyUpdate(BaseModel):
    transfer_strategy: str = Field(pattern="^(AFTER_ALL_RESTORED|AS_OBJECTS_AVAILABLE)$")


class LegacySourceConnectionMigration(BaseModel):
    """One-way adoption of a pre-connection source without changing audit data."""
    aws_connection_id: int = Field(gt=0)


class AwsConnectionCreate(BaseModel):
    secret_ocid: str = Field(min_length=20, max_length=255)
    label: str = Field(min_length=1, max_length=255)


class InventoryManifestImport(BaseModel):
    manifest_uri: str = Field(min_length=10, max_length=4096)


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


class AutomaticWaveCreate(BaseModel):
    max_bytes: int = Field(gt=0, le=10 * 1024**4)
    restore_days: int = Field(ge=1, le=30)
    restore_tier: str = Field(pattern="^(BULK|STANDARD)$")
    prefix: str = Field(default="", max_length=1024)


class DynamicWaveCreate(BaseModel):
    """Create static waves packed by byte, object-count and predicted duration."""
    max_bytes: int = Field(gt=0, le=10 * 1024**4)
    target_transfer_seconds: int = Field(ge=300, le=7 * 24 * 3600)
    max_objects: int = Field(ge=1, le=500000)
    restore_days: int = Field(ge=1, le=30)
    restore_tier: str = Field(pattern="^(BULK|STANDARD)$")
    prefix: str = Field(default="", max_length=1024)
    schedule_restores: bool = False


class ClaimRequest(BaseModel):
    worker_id: str = Field(min_length=1, max_length=128)
    lease_seconds: int = Field(default=300, ge=30, le=3600)


class TaskUpdate(BaseModel):
    worker_id: str = Field(min_length=1, max_length=128)
    error: str | None = Field(default=None, max_length=8000)
    retry_after_seconds: int = Field(default=300, ge=30, le=86400)


class RuntimeSettingsUpdate(BaseModel):
    transfer_workers: int = Field(ge=1, le=64)
    max_throughput_mbps: int = Field(ge=1, le=1200)
    multipart_part_size_mib: int = Field(ge=16, le=512)
    default_wave_size_bytes: int = Field(gt=0, le=10 * 1024**4)
    default_restore_days: int = Field(ge=1, le=30)
    default_restore_tier: str = Field(pattern="^(BULK|STANDARD)$")
    task_lease_seconds: int = Field(ge=30, le=3600)
    simulation_enabled: bool = False
    preserve_s3_tags: bool = True
    real_worker_enabled: bool = False
    cost_estimation_enabled: bool = False
    cost_pricing_auto_refresh_enabled: bool = True
    cost_pricing_refresh_days: int = Field(default=7, ge=1, le=90)
    dynamic_wave_target_seconds: int = Field(default=12 * 3600, ge=300, le=7 * 24 * 3600)
    dynamic_wave_max_objects: int = Field(default=50000, ge=1, le=500000)
    dynamic_restore_safety_seconds: int = Field(default=6 * 3600, ge=0, le=7 * 24 * 3600)
    dynamic_restore_horizon_waves: int = Field(default=2, ge=1, le=20)
    dynamic_pipeline_enabled: bool = False


class ActivityRefreshSettingsUpdate(BaseModel):
    enabled: bool
    seconds: int = Field(ge=5, le=300)


class DeepAuditStart(BaseModel):
    confirmed: bool = False


class SimulationTaskUpdate(BaseModel):
    worker_id: str = Field(min_length=1, max_length=128)


class IntegrityEvidence(BaseModel):
    source_checksum: str | None = Field(default=None, max_length=256)
    destination_checksum: str | None = Field(default=None, max_length=256)
    checksum_algorithm: str = Field(pattern="^(SHA256|MD5)$")
    verified: bool
    error: str | None = Field(default=None, max_length=4000)


app = FastAPI(title="S3 to OCI Migration", version="0.4.0")
app.mount("/static", StaticFiles(directory="app/static"), name="static")


@app.on_event("startup")
def create_schema() -> None:
    Base.metadata.create_all(engine)
    # Lightweight additive migrations keep the single-VM deployment upgradeable. No
    # destructive schema operation is performed automatically.
    expected_columns = {
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
        "real_worker_enabled": "BOOLEAN NOT NULL DEFAULT FALSE",
        "cost_estimation_enabled": "BOOLEAN NOT NULL DEFAULT FALSE",
        "cost_pricing_auto_refresh_enabled": "BOOLEAN NOT NULL DEFAULT TRUE",
        "cost_pricing_refresh_days": "INTEGER NOT NULL DEFAULT 7",
        "multipart_part_size_mib": "INTEGER NOT NULL DEFAULT 64",
        "activity_auto_refresh_enabled": "BOOLEAN NOT NULL DEFAULT TRUE",
        "activity_refresh_seconds": "INTEGER NOT NULL DEFAULT 15",
        "dynamic_wave_target_seconds": "INTEGER NOT NULL DEFAULT 43200",
        "dynamic_wave_max_objects": "INTEGER NOT NULL DEFAULT 50000",
        "dynamic_restore_safety_seconds": "INTEGER NOT NULL DEFAULT 21600",
        "dynamic_restore_horizon_waves": "INTEGER NOT NULL DEFAULT 2",
        "dynamic_pipeline_enabled": "BOOLEAN NOT NULL DEFAULT FALSE",
    }
    source_columns = {"discovery_requested_at": "TIMESTAMP WITH TIME ZONE", "discovery_started_at": "TIMESTAMP WITH TIME ZONE", "discovery_elapsed_seconds": "DOUBLE PRECISION NOT NULL DEFAULT 0", "discovery_completed_at": "TIMESTAMP WITH TIME ZONE", "discovery_error": "TEXT", "discovery_continuation_token": "TEXT", "discovery_pages_completed": "INTEGER NOT NULL DEFAULT 0", "discovery_objects_inserted": "BIGINT NOT NULL DEFAULT 0", "aws_connection_id": "INTEGER", "aws_bucket_region": "VARCHAR(64)", "transfer_strategy": "VARCHAR(32) NOT NULL DEFAULT 'AFTER_ALL_RESTORED'"}
    source_columns["archived_at"] = "TIMESTAMP WITH TIME ZONE"
    source_columns.update({"destination_validation_at": "TIMESTAMP WITH TIME ZONE", "destination_validation_status": "VARCHAR(32)", "destination_missing_count": "INTEGER NOT NULL DEFAULT 0", "destination_size_mismatch_count": "INTEGER NOT NULL DEFAULT 0", "destination_metadata_mismatch_count": "INTEGER NOT NULL DEFAULT 0", "destination_extra_count": "INTEGER NOT NULL DEFAULT 0"})
    wave_columns = {"batch_job_id": "VARCHAR(128)", "batch_job_status": "VARCHAR(64)", "manifest_key": "VARCHAR(2048)", "manifest_etag": "VARCHAR(128)", "last_poll_at": "TIMESTAMP WITH TIME ZONE", "poll_count": "INTEGER NOT NULL DEFAULT 0", "pipeline_run_id": "BIGINT", "availability_head_requests": "BIGINT NOT NULL DEFAULT 0", "availability_poll_elapsed_seconds": "DOUBLE PRECISION NOT NULL DEFAULT 0", "availability_throttle_retries": "INTEGER NOT NULL DEFAULT 0", "last_availability_poll_objects": "INTEGER NOT NULL DEFAULT 0", "last_availability_poll_seconds": "DOUBLE PRECISION NOT NULL DEFAULT 0", "planner_mode": "VARCHAR(32) NOT NULL DEFAULT 'MANUAL'", "predicted_transfer_seconds": "DOUBLE PRECISION NOT NULL DEFAULT 0", "prediction_samples": "INTEGER NOT NULL DEFAULT 0", "planned_restore_at": "TIMESTAMP WITH TIME ZONE", "planned_transfer_start_at": "TIMESTAMP WITH TIME ZONE"}
    existing_runtime_columns = {column["name"] for column in inspect(engine).get_columns("runtime_settings")}
    existing_source_columns = {column["name"] for column in inspect(engine).get_columns("sources")}
    existing_wave_columns = {column["name"] for column in inspect(engine).get_columns("waves")}
    existing_bucket_columns = {column["name"] for column in inspect(engine).get_columns("oci_bucket_cache")}
    existing_cost_pricing_columns = {column["name"] for column in inspect(engine).get_columns("cost_pricing")}
    existing_discovery_job_columns = {column["name"] for column in inspect(engine).get_columns("discovery_jobs")}
    existing_run_columns = {column["name"] for column in inspect(engine).get_columns("dynamic_pipeline_runs")}
    existing_restore_attempt_columns = {column["name"] for column in inspect(engine).get_columns("restore_attempts")}
    with engine.begin() as connection:
        for column, sql_type in expected_columns.items():
            if column not in existing_columns:
                connection.execute(text(f"ALTER TABLE objects ADD COLUMN {column} {sql_type}"))
        for column, sql_type in runtime_columns.items():
            if column not in existing_runtime_columns:
                connection.execute(text(f"ALTER TABLE runtime_settings ADD COLUMN {column} {sql_type}"))
        for column, sql_type in source_columns.items():
            if column not in existing_source_columns:
                connection.execute(text(f"ALTER TABLE sources ADD COLUMN {column} {sql_type}"))
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
        for column, sql_type in {"restore_horizon_waves": "INTEGER NOT NULL DEFAULT 2"}.items():
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
        }.items():
            if column not in existing_discovery_job_columns:
                connection.execute(text(f"ALTER TABLE discovery_jobs ADD COLUMN {column} {sql_type}"))
    # Preserve prior dynamic waves as a single, clearly labelled historical
    # run per source. Their existing plans, object timestamps and task/event
    # history remain authoritative; this only creates the missing grouping.
    with SessionLocal() as session:
        legacy_source_ids = list(session.scalars(select(Wave.source_id).where(
            Wave.planner_mode == "DYNAMIC", Wave.pipeline_run_id.is_(None)
        ).distinct()))
        for source_id in legacy_source_ids:
            run = DynamicPipelineRun(source_id=source_id, planner_version="v1-legacy-import",
                                     status="HISTORICAL", transfer_strategy="AFTER_ALL_RESTORED")
            session.add(run); session.flush()
            for wave in session.scalars(select(Wave).where(
                Wave.source_id == source_id, Wave.planner_mode == "DYNAMIC", Wave.pipeline_run_id.is_(None)
            )):
                wave.pipeline_run_id = run.id
        if legacy_source_ids:
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
        if run.status != "COMPLETED":
            completed_at = session.scalar(select(func.max(ObjectRecord.transferred_at)).where(
                ObjectRecord.wave_id.in_(select(Wave.id).where(Wave.pipeline_run_id == run.id))
            ))
            run.status, run.completed_at = "COMPLETED", completed_at or utcnow()
    elif any(status in {"TRANSFERRED_WITH_ERRORS", "RESTORE_REQUEST_FAILED", "VERIFICATION_FAILED"} for status in statuses):
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


def settings_dict(settings: RuntimeSettings) -> dict:
    return {"transfer_workers": settings.transfer_workers, "max_throughput_mbps": settings.max_throughput_mbps,
            "multipart_part_size_mib": settings.multipart_part_size_mib,
            "default_wave_size_bytes": settings.default_wave_size_bytes, "default_restore_days": settings.default_restore_days,
            "default_restore_tier": settings.default_restore_tier, "task_lease_seconds": settings.task_lease_seconds,
            "simulation_enabled": settings.simulation_enabled, "real_worker_enabled": settings.real_worker_enabled,
            "preserve_s3_tags": settings.preserve_s3_tags, "cost_estimation_enabled": settings.cost_estimation_enabled,
            "cost_pricing_auto_refresh_enabled": settings.cost_pricing_auto_refresh_enabled,
            "cost_pricing_refresh_days": settings.cost_pricing_refresh_days,
            "activity_auto_refresh_enabled": settings.activity_auto_refresh_enabled,
            "activity_refresh_seconds": settings.activity_refresh_seconds,
            "dynamic_wave_target_seconds": settings.dynamic_wave_target_seconds,
            "dynamic_wave_max_objects": settings.dynamic_wave_max_objects,
            "dynamic_restore_safety_seconds": settings.dynamic_restore_safety_seconds,
            "dynamic_restore_horizon_waves": settings.dynamic_restore_horizon_waves,
            "dynamic_pipeline_enabled": settings.dynamic_pipeline_enabled,
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
            "created_at": connection.created_at, "sources": int(sources)}


PRICING_RATE_FIELDS = (
    "aws_batch_job_usd", "aws_batch_object_usd_per_1000", "aws_s3_put_list_usd_per_1000",
    "aws_s3_get_usd_per_1000", "aws_glacier_bulk_retrieval_usd_per_gib",
    "aws_glacier_standard_retrieval_usd_per_gib", "aws_deep_archive_bulk_retrieval_usd_per_gib",
    "aws_deep_archive_standard_retrieval_usd_per_gib", "aws_transfer_out_usd_per_gib",
    "aws_restore_temp_standard_usd_per_gib_month", "oci_put_usd_per_10000",
    "oci_get_usd_per_10000", "oci_storage_usd_per_gib_month",
)


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
            **{field: getattr(pricing, field) for field in PRICING_RATE_FIELDS}, "updated_at": pricing.updated_at}


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
        # AWS publishes byte/GB prices in decimal GB. Raijin presents binary
        # GiB, so normalize all data-sized rates here once.
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
            "source_url": row.source_url if row else None, "fetched_at": row.fetched_at if row else None,
            "error": row.error if row else None}


def active_pricing_regions(session: Session) -> list[str]:
    """Include legacy source regions as well as current connection defaults.

    New sources inherit their connection region, but existing migration history
    can legitimately retain an older source region. Its wave must use the same
    public price list as the S3 operations it records.
    """
    connection_regions = session.scalars(select(AwsConnection.default_region).where(AwsConnection.archived_at.is_(None)))
    source_regions = session.scalars(select(Source.aws_region).where(Source.archived_at.is_(None), Source.aws_connection_id.is_not(None)))
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
    if not source.aws_connection_id:
        raise HTTPException(status_code=409, detail="Wave source has no AWS connection pricing profile")
    pricing = pricing_or_create(session, source.aws_connection_id)
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
        custom_rate = getattr(pricing, rate_field)
        rate = custom_rate if custom_rate is not None else public_rates.get(rate_field)
        entry = {"key": key, "label": label, "quantity": quantity, "unit": unit,
                 "rate_field": rate_field, "rate": rate, "category": category,
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
        add("restore_polling", "S3 restore availability checks (HeadObject)", len(archived) * pricing.expected_restore_poll_cycles, "requests", "aws_s3_get_usd_per_1000", 1000)
        add("restore_temp", "Temporary S3 Standard restored copy", (glacier_gib + deep_gib) * wave.restore_days / 30, "GiB-month", "aws_restore_temp_standard_usd_per_gib_month")
        retrieval_field = "aws_glacier_bulk_retrieval_usd_per_gib" if wave.restore_tier == "BULK" else "aws_glacier_standard_retrieval_usd_per_gib"
        add("glacier_retrieval", f"S3 Glacier {wave.restore_tier} retrieval", glacier_gib, "GiB", retrieval_field)
        retrieval_field = "aws_deep_archive_bulk_retrieval_usd_per_gib" if wave.restore_tier == "BULK" else "aws_deep_archive_standard_retrieval_usd_per_gib"
        add("deep_archive_retrieval", f"S3 Deep Archive {wave.restore_tier} retrieval", deep_gib, "GiB", retrieval_field)
    add("discovery", "Allocated S3 discovery ListObjectsV2 pages", wave_pages, "requests", "aws_s3_put_list_usd_per_1000", 1000)
    add("source_reads", "S3 object reads during transfer", len(objects), "requests", "aws_s3_get_usd_per_1000", 1000)
    if settings.preserve_s3_tags:
        add("tag_reads", "S3 object-tag reads", len(objects), "requests", "aws_s3_get_usd_per_1000", 1000)
    if pricing.include_aws_transfer_out:
        add("aws_transfer_out", "AWS data transfer out to OCI", total_gib, "GiB", "aws_transfer_out_usd_per_gib")
    if pricing.include_oci_costs:
        add("oci_writes", "OCI Object Storage write operations", oci_put_operations, "operations", "oci_put_usd_per_10000", 10000)
        add("oci_storage", "OCI destination storage (one month)", total_gib, "GiB-month", "oci_storage_usd_per_gib_month", category="recurring")
        add("deep_audit", "Optional deep SHA-256 audit OCI reads", len(objects), "operations", "oci_get_usd_per_10000", 10000, category="optional")
    one_time = [item["cost"] for item in components if item["category"] == "one_time"]
    recurring = [item["cost"] for item in components if item["category"] == "recurring"]
    optional = [item["cost"] for item in components if item["category"] == "optional"]
    def estimated(values: list[float | None]) -> float:
        return round(sum(value for value in values if value is not None), 6)
    return {"wave_id": wave.id, "connection_id": source.aws_connection_id, "connection_label": source.aws_connection.label,
            "currency": pricing.currency.upper(), "pricing_reference": pricing.reference, "pricing_updated_at": pricing.updated_at,
            "global_pricing": public,
            "complete": not missing_by_category["one_time"] and not unpriced_archive_gib,
            "missing_rates": sorted(set(missing)), "unpriced_archive_gib": round(unpriced_archive_gib, 6),
            "totals": {"one_time": estimated(one_time), "recurring_monthly": estimated(recurring), "optional_deep_audit": estimated(optional)},
            "total_completeness": {"one_time": not missing_by_category["one_time"] and not unpriced_archive_gib,
                                   "recurring_monthly": not missing_by_category["recurring"],
                                   "optional_deep_audit": not missing_by_category["optional"]},
            "quantities": {"objects": len(objects), "archive_objects": len(archived), "bytes": total_bytes,
                           "source_inventory_objects": int(source_objects), "multipart_part_mib": settings.multipart_part_size_mib,
                           "estimated_poll_cycles": pricing.expected_restore_poll_cycles, "oci_write_operations": oci_put_operations},
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
        return page.read()


@app.get("/healthz")
def healthcheck(session: Session = Depends(get_session)) -> dict:
    session.execute(select(1))
    return {"status": "ok"}


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
    volume = shutil.disk_usage("/")
    return {
        "generated_at": now, "tasks": {"failed": int(failed_tasks), "retrying": int(retrying_tasks), "stale_leases": int(stale_leases)},
        "transfers": {"active_multipart_checkpoints": int(active_multipart), "stalled": int(stalled_transfers)},
        "events": {"failures_last_24h": int(recent_failures)},
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
    latest_task_id = select(func.max(Task.id)).where(Task.wave_id == Wave.id).correlate(Wave).scalar_subquery()
    actionable_failed_tasks = session.scalar(
        select(func.count(Task.id)).join(Wave).where(
            Task.state == TaskState.FAILED,
            Task.id == latest_task_id,
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
        ObjectRecord.wave_id.in_(select(Task.wave_id).where(Task.kind == "TRANSFER_WAVE", Task.state == TaskState.RUNNING)),
    )) or 0)
    # Tiny objects can finish before a two-second in-flight sample exists. Add
    # their completion throughput from a short fixed window so current activity
    # remains meaningful for workloads with many small files.
    live_window_seconds = 15
    live_since = utcnow() - timedelta(seconds=live_window_seconds)
    recently_completed_bytes = int(session.scalar(select(func.coalesce(func.sum(ObjectRecord.size_bytes), 0)).where(
        ObjectRecord.transferred_at >= live_since,
    )) or 0)
    live_transfer_mbps += (recently_completed_bytes * 8) / live_window_seconds / 1_000_000
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
        .where(Wave.id.in_(select(Task.wave_id).where(Task.kind == "TRANSFER_WAVE", Task.state == TaskState.RUNNING)))
        .group_by(Wave.id, Source.name).order_by(Wave.id)
    ).all()
    active_transfers = [
        {"wave_id": wave_id, "wave_name": wave_name, "source_name": source_name,
         "total_files": int(total_files), "total_bytes": int(total_bytes),
         "transferred_files": int(done_files), "transferred_bytes": int(done_bytes),
         "in_flight_files": int(in_flight_files), "in_flight_bytes": int(in_flight_bytes), "live_mbps": round(float(live_mbps), 2)}
        for wave_id, wave_name, source_name, total_files, total_bytes, done_files, done_bytes, in_flight_files, in_flight_bytes, live_mbps in active_transfer_rows
    ]
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
            "transfer_mbps": round((transferred_bytes * 8) / transfer_seconds / 1_000_000, 2),
            "transfer_live_mbps": round(live_transfer_mbps, 2),
            "transfer_live_window_seconds": live_window_seconds,
            "restored_files": restored_files,
            "restore_requested_total": int(restore_requested_total or 0),
            "restore_available_total": int(restore_available_total or 0),
            "restored_per_minute": round(restored_files / (restore_seconds / 60), 2),
            "restored_per_hour": round(restored_files / (restore_seconds / 3600), 2),
            "active_transfers": active_transfers,
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
        "waiting_seconds": max(0, int((now - task.created_at).total_seconds())) if task.kind in {"SUBMIT_BATCH_RESTORE", "POLL_RESTORE"} else 0,
        "last_error": task.error,
        "availability_poll_interval_seconds": restore_availability_poll_delay_seconds(
            attempt.completed_at if attempt else None, now, wave.restore_tier,
            partial_availability=bool(timing["available_objects"] and timing["pending_objects"]),
            transfer_strategy=getattr(getattr(wave, "source", None), "transfer_strategy", "AFTER_ALL_RESTORED"),
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
    transfer_kinds = ("SUBMIT_BATCH_RESTORE", "POLL_RESTORE", "TRANSFER_WAVE")
    tasks = list(session.scalars(
        select(Task).join(Wave).where(
            Task.kind.in_(transfer_kinds), Task.state.in_([TaskState.READY, TaskState.RUNNING]),
            Wave.status != "PAUSED",
        ).order_by(Task.available_at, Task.id)
    ))
    # At most one current task represents each wave.  Prefer a running task
    # when a stale/retried READY row exists for the same wave.
    by_wave: dict[int, Task] = {}
    for task in tasks:
        previous = by_wave.get(task.wave_id)
        if not previous or (task.state == TaskState.RUNNING and previous.state != TaskState.RUNNING):
            by_wave[task.wave_id] = task

    def elapsed_seconds(objects: list[ObjectRecord]) -> int:
        elapsed = 0.0
        for obj in objects:
            value = float(obj.transfer_elapsed_seconds or 0)
            # Waves created before the accumulated-duration field retain an
            # accurate useful fallback from their recorded start/end stamps.
            if value <= 0 and obj.transfer_started_at and obj.transferred_at:
                value = max(0, (obj.transferred_at - obj.transfer_started_at).total_seconds())
            elapsed += value
        return int(elapsed)

    waves = []
    for task in by_wave.values():
        wave = task.wave
        objects = list(session.scalars(select(ObjectRecord).where(ObjectRecord.wave_id == wave.id).order_by(ObjectRecord.id)))
        completed = [obj for obj in objects if obj.state in {ObjectState.TRANSFERRED, ObjectState.VERIFIED}]
        in_flight = [obj for obj in objects if obj.state == ObjectState.TRANSFERRING]
        active = task.kind == "TRANSFER_WAVE" and task.state == TaskState.RUNNING
        workers = []
        capacity = runtime_settings(session).transfer_workers
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
            "wave_id": wave.id, "wave_name": wave.name, "source_name": wave.source.name,
            "status": wave.status, "task_kind": task.kind, "task_state": task.state,
            "available_at": task.available_at, "active": active,
            # Batch data comes from durable local records only.  It makes the
            # wait explainable without turning dashboard refreshes into AWS API
            # calls (and therefore without adding request cost).
            "restore": restore_queue_details(wave, task, now, session=session),
            "transferred_files": len(completed), "total_files": len(objects),
            "transferred_bytes": sum(obj.size_bytes for obj in completed),
            "total_bytes": sum(obj.size_bytes for obj in objects),
            "elapsed_seconds": elapsed_seconds(objects), "workers": workers if active else [],
        })
    waves.sort(key=lambda item: (not item["active"], item["available_at"], item["wave_id"]))
    return {"waves": waves, "generated_at": now}


@app.get("/api/flight-board/availability")
def flight_board_availability(session: Session = Depends(get_session)) -> dict:
    """Cheap indicator used to show the dynamic-pipeline flight-board button."""
    dynamic_waves = session.scalar(select(func.count(Wave.id)).where(Wave.planner_mode == "DYNAMIC")) or 0
    return {"available": bool(dynamic_waves), "waves": int(dynamic_waves)}


@app.get("/api/flight-board")
def flight_board(run_id: int | None = Query(default=None, ge=1), session: Session = Depends(get_session)) -> dict:
    """Return local-only planned and actual phases for dynamic migration waves."""
    now = utcnow()
    filters = [Wave.planner_mode == "DYNAMIC"]
    if run_id is not None:
        filters.append(Wave.pipeline_run_id == run_id)
    rows = list(session.execute(
        select(Wave, Source.name).join(Source).where(*filters)
        .order_by(Wave.planned_transfer_start_at.nulls_last(), Wave.id).limit(500)
    ))
    wave_ids = [wave.id for wave, _source_name in rows]
    if not wave_ids:
        return {"waves": [], "generated_at": now, "truncated": False}
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

    def phase(kind: str, start: datetime | None, end: datetime | None, *, planned: bool = False) -> dict | None:
        if not start or not end or end <= start:
            return None
        return {"kind": kind, "start_at": start, "end_at": end, "planned": planned}

    board_waves = []
    for wave, source_name in rows:
        times = object_times.get(wave.id, {})
        request_at = times.get("restore_requested_at")
        available_at = times.get("last_available_at")
        transfer_started_at = times.get("transfer_started_at")
        transfer_completed_at = times.get("transfer_completed_at")
        task = latest_tasks.get(wave.id)
        phases = []
        queue_end = request_at or wave.planned_restore_at
        queued = phase("QUEUE", wave.created_at, queue_end, planned=not bool(request_at))
        if queued:
            phases.append(queued)
        restore_start = request_at or wave.planned_restore_at
        if request_at:
            restore_end = available_at or transfer_started_at or now
        else:
            restore_end = wave.planned_transfer_start_at
        restore = phase("RESTORE", restore_start, restore_end, planned=not bool(request_at))
        if restore:
            phases.append(restore)
        transfer_start = transfer_started_at or wave.planned_transfer_start_at
        if transfer_started_at:
            transfer_end = transfer_completed_at or now
        elif transfer_start:
            transfer_end = transfer_start + timedelta(seconds=max(1, int(wave.predicted_transfer_seconds or 0)))
        else:
            transfer_end = None
        transfer = phase("TRANSFER", transfer_start, transfer_end, planned=not bool(transfer_started_at))
        if transfer:
            phases.append(transfer)
        status = wave.status
        if transfer_started_at and not transfer_completed_at:
            status = "TRANSFERRING"
        elif request_at and not available_at:
            status = "RESTORING"
        elif available_at and not transfer_started_at:
            status = "RESTORED"
        board_waves.append({
            "wave_id": wave.id, "wave_name": wave.name, "source_name": source_name,
            "pipeline_run_id": wave.pipeline_run_id,
            "status": status, "task_state": task.state if task else None,
            "started_at": request_at or transfer_started_at,
            "completed_at": transfer_completed_at if status in {"COMPLETED", "VERIFIED", "TRANSFERRED"} else None,
            "planned_restore_at": wave.planned_restore_at,
            "planned_transfer_start_at": wave.planned_transfer_start_at,
            "predicted_transfer_seconds": int(wave.predicted_transfer_seconds or 0),
            "phases": phases,
        })
    timeline_points = [point for wave in board_waves for phase_item in wave["phases"] for point in (phase_item["start_at"], phase_item["end_at"])]
    return {"waves": board_waves, "generated_at": now,
            "timeline_start_at": min(timeline_points) if timeline_points else now,
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
    settings = runtime_settings(session)
    for field, value in payload.model_dump().items():
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
        setattr(pricing, field, value)
    record_event(session, "COST_PRICING_UPDATED", f"Cost pricing updated for AWS connection '{connection.label}'", source_id=None)
    session.commit()
    return pricing_dict(pricing)


@app.get("/api/global-aws-pricing")
def get_global_aws_pricing(session: Session = Depends(get_session)) -> list[dict]:
    regions = active_pricing_regions(session)
    return [global_pricing_summary(session, region) for region in regions]


@app.post("/api/global-aws-pricing/refresh")
def refresh_global_aws_pricing_now(session: Session = Depends(get_session)) -> dict:
    regions = active_pricing_regions(session)
    if not regions:
        raise HTTPException(status_code=409, detail="No active AWS connections are available for public pricing refresh")
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
    return [{"id": s.id, "name": s.name, "s3_bucket": s.s3_bucket, "s3_prefix": s.s3_prefix,
             "aws_region": s.aws_region, "destination_bucket": s.destination_bucket, "transfer_strategy": s.transfer_strategy, "status": s.status,
             "aws_bucket_region": s.aws_bucket_region,
             "aws_connection_id": s.aws_connection_id,
             "aws_connection_label": s.aws_connection.label if s.aws_connection else "Unassigned AWS connection",
             "discovery_requested_at": s.discovery_requested_at, "discovery_completed_at": s.discovery_completed_at,
             "discovery_error": s.discovery_error, "discovery_pages_completed": s.discovery_pages_completed,
             "discovery_objects_inserted": s.discovery_objects_inserted,
             "discovery_can_resume": bool(s.discovery_continuation_token), "archived_at": s.archived_at,
             "destination_validation": {"status": s.destination_validation_status, "at": s.destination_validation_at,
                                        "missing": s.destination_missing_count, "size_mismatches": s.destination_size_mismatch_count},
             "migration_status": migration_status(s), "can_delete": not source_has_executed_wave(session, s.id)}
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
    source = Source(**payload.model_dump())
    session.add(source)
    session.flush()
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
    for field, value in payload.model_dump().items():
        setattr(source, field, value)
    record_event(session, "SOURCE_UPDATED", f"Source '{source.name}' configuration updated", source_id=source.id)
    session.commit()
    return {"id": source.id, "name": source.name, "status": source.status}


@app.patch("/api/sources/{source_id}/transfer-strategy")
def update_source_transfer_strategy(source_id: int, payload: SourceTransferStrategyUpdate,
                                    session: Session = Depends(get_session)) -> dict:
    """Update the durable transfer-release policy without editing source identity."""
    source = active_source_or_409(session, source_id)
    source.transfer_strategy = payload.transfer_strategy
    record_event(session, "SOURCE_TRANSFER_STRATEGY_UPDATED",
                 f"Source '{source.name}' transfer strategy set to {payload.transfer_strategy}",
                 source_id=source.id)
    session.commit()
    return {"id": source.id, "transfer_strategy": source.transfer_strategy}


def aws_bucket_region_from_connection(connection: AwsConnection, bucket: str) -> str:
    """Use HeadBucket's region header; it needs no Secret value in API responses."""
    import boto3
    from botocore.config import Config
    values = aws_secret_payload(connection.secret_ocid)
    config = Config(connect_timeout=10, read_timeout=30, retries={"max_attempts": 3, "mode": "standard"})
    bootstrap = boto3.Session(aws_access_key_id=values["bootstrap_access_key_id"], aws_secret_access_key=values["bootstrap_secret_access_key"], region_name=values["default_region"])
    assumed = bootstrap.client("sts", config=config).assume_role(RoleArn=values["migration_role_arn"], RoleSessionName="raijin-source-region-sync", DurationSeconds=900)["Credentials"]
    s3 = boto3.Session(aws_access_key_id=assumed["AccessKeyId"], aws_secret_access_key=assumed["SecretAccessKey"], aws_session_token=assumed["SessionToken"], region_name=values["default_region"]).client("s3", config=config)
    headers = s3.head_bucket(Bucket=bucket).get("ResponseMetadata", {}).get("HTTPHeaders", {})
    region = headers.get("x-amz-bucket-region")
    if not region:
        raise RuntimeError("AWS did not return the source bucket region")
    return "eu-west-1" if region == "EU" else region


@app.post("/api/sources/{source_id}/sync-aws-region")
def sync_source_aws_region(source_id: int, session: Session = Depends(get_session)) -> dict:
    """Correct a source region using AWS' HeadBucket response without rediscovery."""
    source = active_source_or_409(session, source_id)
    if not source.aws_connection:
        raise HTTPException(status_code=422, detail="Source has no AWS connection")
    running = session.scalar(select(Task.id).join(Wave).where(Wave.source_id == source.id, Task.state == TaskState.RUNNING).limit(1))
    if running:
        raise HTTPException(status_code=409, detail="Wait for the running task before synchronizing the source region")
    try:
        actual = aws_bucket_region_from_connection(source.aws_connection, source.s3_bucket)
    except Exception as error:
        raise HTTPException(status_code=422, detail=f"Source AWS region lookup failed: {safe_aws_error_summary(error)}") from error
    if actual != source.aws_connection.default_region:
        raise HTTPException(status_code=422, detail="Source bucket region differs from its AWS connection. Create a connection with a control bucket in the source region instead of changing this source")
    previous = source.aws_region
    source.aws_bucket_region, source.aws_region = actual, actual
    pending_task_ids = select(Task.id).join(Wave).where(
        Wave.source_id == source.id,
        Task.kind.in_(["SUBMIT_BATCH_RESTORE", "POLL_RESTORE"]),
        Task.state == TaskState.READY,
    )
    # SQLAlchemy deliberately disallows UPDATE against a joined ORM query.
    # Select the immutable task ids first, then update the base table.  This is
    # also safe when a source has several historical waves.
    superseded = session.query(Task).filter(Task.id.in_(pending_task_ids)).update(
        {Task.state: TaskState.FAILED, Task.error: "Superseded after source AWS region synchronization"},
        synchronize_session=False,
    )

    invalidated_waves = 0
    active_restore_states = ["RESTORE_REQUESTED", "RESTORING", "RESTORE_REQUEST_ACCEPTED"]
    for wave in session.scalars(select(Wave).where(Wave.source_id == source.id, Wave.status.in_(active_restore_states))):
        # Retain an immutable local record of the old Batch job before it is
        # invalidated.  It may never be treated as proof that a restore request
        # was accepted; the operator must explicitly reprocess the wave.
        if wave.batch_job_id and not session.scalar(select(RestoreAttempt.id).where(RestoreAttempt.job_id == wave.batch_job_id)):
            session.add(RestoreAttempt(
                wave_id=wave.id,
                aws_region=previous,
                job_id=wave.batch_job_id,
                job_status=wave.batch_job_status or "INVALIDATED_REGION_MISMATCH",
                manifest_key=wave.manifest_key,
                manifest_etag=wave.manifest_etag,
                expected_objects=session.scalar(select(func.count(ObjectRecord.id)).where(ObjectRecord.wave_id == wave.id)) or 0,
                failure_summary="Invalidated after AWS source-region synchronization; operator must reprocess this wave.",
                completed_at=utcnow(),
            ))
        for obj in session.scalars(select(ObjectRecord).where(
            ObjectRecord.wave_id == wave.id,
            ObjectRecord.state.in_([ObjectState.RESTORE_REQUESTED, ObjectState.RESTORE_REQUEST_ACCEPTED, ObjectState.RESTORING]),
        )):
            obj.state, obj.restored_at = ObjectState.WAVE_ASSIGNED, None
        wave.status = "RESTORE_REQUEST_FAILED"
        wave.batch_job_status = "INVALIDATED_REGION_MISMATCH"
        invalidated_waves += 1
        record_event(session, "RESTORE_INVALIDATED_BY_REGION_SYNC", f"Wave '{wave.name}' restore state invalidated after AWS region changed from {previous} to {actual}; reprocess is required", source_id=source.id, wave_id=wave.id)
    record_event(session, "SOURCE_AWS_REGION_SYNCED", f"Source '{source.name}' region synchronized from {previous} to {actual}; {superseded} pending restore task(s) and {invalidated_waves} active restore wave(s) invalidated", source_id=source.id)
    session.commit()
    return {"source_id": source.id, "previous_region": previous, "aws_region": actual, "superseded_tasks": superseded, "invalidated_waves": invalidated_waves}


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


@app.delete("/api/sources/{source_id}")
def delete_source(source_id: int, session: Session = Depends(get_session)) -> dict:
    source = source_or_404(session, source_id)
    if source_has_executed_wave(session, source_id):
        raise HTTPException(status_code=409, detail="This source has an executed wave and must be archived, not deleted")
    wave_ids = list(session.scalars(select(Wave.id).where(Wave.source_id == source_id)))
    if wave_ids:
        session.query(Event).filter((Event.source_id == source_id) | (Event.wave_id.in_(wave_ids))).delete(synchronize_session=False)
        session.query(Task).filter(Task.wave_id.in_(wave_ids)).delete(synchronize_session=False)
    else:
        session.query(Event).filter(Event.source_id == source_id).delete(synchronize_session=False)
    session.query(ObjectRecord).filter(ObjectRecord.source_id == source_id).delete(synchronize_session=False)
    session.query(Wave).filter(Wave.source_id == source_id).delete(synchronize_session=False)
    session.delete(source)
    session.commit()
    return {"id": source_id, "deleted": True}


@app.post("/api/sources/{source_id}/archive")
def archive_source(source_id: int, session: Session = Depends(get_session)) -> dict:
    source = source_or_404(session, source_id)
    if not source_has_executed_wave(session, source_id):
        raise HTTPException(status_code=409, detail="A source without an executed wave must be deleted, not archived")
    for wave in session.scalars(select(Wave).where(Wave.source_id == source_id, Wave.status.not_in(["COMPLETED", "VERIFIED", "TRANSFERRED", "TRANSFERRED_WITH_ERRORS", "VERIFICATION_FAILED"]))):
        wave.status = "PAUSED"
    source.archived_at, source.status = utcnow(), "ARCHIVED"
    record_event(session, "SOURCE_ARCHIVED", f"Source '{source.name}' archived; historical data retained", source_id=source.id)
    session.commit()
    return {"id": source.id, "status": source.status, "archived_at": source.archived_at}


@app.post("/api/sources/{source_id}/inventory/import", status_code=201)
def import_inventory(source_id: int, payload: InventoryImport, session: Session = Depends(get_session)) -> dict:
    active_source_or_409(session, source_id)
    inserted = 0
    for item in payload.items:
        duplicate = session.scalar(select(ObjectRecord.id).where(
            ObjectRecord.source_id == source_id,
            ObjectRecord.object_key == item.object_key,
            ObjectRecord.version_id == item.version_id,
        ))
        if duplicate:
            continue
        session.add(ObjectRecord(source_id=source_id, **item.model_dump()))
        inserted += 1
    record_event(session, "INVENTORY_IMPORTED", f"Imported {inserted} inventory record(s); skipped {len(payload.items) - inserted} duplicate(s)", source_id=source_id)
    session.commit()
    return {"inserted": inserted, "skipped_duplicates": len(payload.items) - inserted}


def inventory_file_value(row: dict[str, str], *names: str) -> str | None:
    """Read an inventory column case-insensitively, including S3 CSV names."""
    normalized = {str(key).strip().lower().replace("_", ""): value for key, value in row.items() if key}
    for name in names:
        value = normalized.get(name.lower().replace("_", ""))
        if value is not None and str(value).strip() != "":
            return str(value).strip()
    return None


@app.post("/api/sources/{source_id}/inventory/upload", status_code=201)
def upload_inventory_file(source_id: int, inventory_file: UploadFile = File(...), session: Session = Depends(get_session)) -> dict:
    """Import a scalable CSV inventory without calling AWS.

    The source must still be pristine.  This avoids a potentially ambiguous
    merge between a supplied inventory and an AWS discovery, and lets a large
    file be parsed in bounded batches directly on the VM.
    """
    source = active_source_or_409(session, source_id)
    if session.scalar(select(Wave.id).where(Wave.source_id == source_id)):
        raise HTTPException(status_code=409, detail="Inventory file import is immutable after waves are created")
    if session.scalar(select(ObjectRecord.id).where(ObjectRecord.source_id == source_id).limit(1)):
        raise HTTPException(status_code=409, detail="This source already has inventory records; create a new source or delete the unexecuted discovery first")
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
                session.bulk_insert_mappings(ObjectRecord, pending)
                inserted += len(pending)
                pending.clear()
        if pending:
            session.bulk_insert_mappings(ObjectRecord, pending)
            inserted += len(pending)
        if not inserted:
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
        record_event(session, "INVENTORY_FILE_IMPORTED", f"Imported {inserted} record(s) from inventory file '{filename}' without AWS discovery", source_id=source_id)
        session.commit()
        return {"source_id": source.id, "status": source.status, "inserted": inserted, "filename": filename}
    except HTTPException:
        session.rollback()
        raise
    except (OSError, UnicodeDecodeError, csv.Error) as error:
        session.rollback()
        raise HTTPException(status_code=422, detail=f"Could not read inventory CSV: {type(error).__name__}") from error
    finally:
        inventory_file.file.close()


@app.post("/api/sources/{source_id}/discovery")
def request_discovery(source_id: int, session: Session = Depends(get_session)) -> dict:
    source = active_source_or_409(session, source_id)
    if source.status == "DISCOVERING":
        raise HTTPException(status_code=409, detail="Discovery already running for this source")
    if session.scalar(select(func.count(Wave.id)).where(Wave.source_id == source_id)):
        raise HTTPException(status_code=409, detail="Discovery is immutable after waves are created")
    resume = source.status == "DISCOVERY_FAILED" and bool(source.discovery_continuation_token)
    # A remote discovery has one exact S3 continuation cursor.  Mixing it with
    # a previous inventory-file import (or silently starting over on completed
    # rows) makes the origin non-auditable and forces expensive duplicate
    # checks.  A failed remote discovery is the only valid resume case.
    if not resume and session.scalar(select(ObjectRecord.id).where(ObjectRecord.source_id == source_id).limit(1)):
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
        source.discovery_pages_completed = 0
        source.discovery_objects_inserted = 0
        source.discovery_started_at = None
        source.discovery_elapsed_seconds = 0
    job = DiscoveryJob(source_id=source.id)
    session.add(job)
    record_event(session, "DISCOVERY_QUEUED", f"AWS discovery job queued {'from its durable checkpoint' if resume else ''} for source '{source.name}'", source_id=source.id)
    session.commit()
    return {"source_id": source.id, "status": source.status, "job_id": job.id}


@app.post("/api/sources/{source_id}/inventory/manifest")
def request_inventory_manifest_import(source_id: int, payload: InventoryManifestImport,
                                      session: Session = Depends(get_session)) -> dict:
    """Queue direct import of a S3 Inventory manifest and all of its shards."""
    source = active_source_or_409(session, source_id)
    parsed = urlparse(payload.manifest_uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
        raise HTTPException(status_code=422, detail="Manifest URI must use s3://bucket/key")
    if source.status == "DISCOVERING" or session.scalar(select(Wave.id).where(Wave.source_id == source.id).limit(1)):
        raise HTTPException(status_code=409, detail="Inventory manifest import is immutable after discovery starts or waves are created")
    if session.scalar(select(ObjectRecord.id).where(ObjectRecord.source_id == source.id).limit(1)):
        raise HTTPException(status_code=409, detail="Inventory manifest import cannot merge with an existing inventory; create a new source")
    queued = session.scalar(select(DiscoveryJob.id).where(
        DiscoveryJob.source_id == source.id,
        DiscoveryJob.state.in_([TaskState.READY, TaskState.RUNNING]),
    ).limit(1))
    if queued:
        raise HTTPException(status_code=409, detail=f"Discovery job {queued} is already queued or running for this source")
    source.status, source.discovery_requested_at = "DISCOVERY_QUEUED", utcnow()
    source.discovery_started_at, source.discovery_completed_at, source.discovery_error = None, None, None
    source.discovery_continuation_token, source.discovery_pages_completed = None, 0
    source.discovery_objects_inserted, source.discovery_elapsed_seconds = 0, 0
    job = DiscoveryJob(source_id=source.id, mode="S3_INVENTORY_MANIFEST", inventory_manifest_uri=payload.manifest_uri)
    session.add(job)
    record_event(session, "INVENTORY_MANIFEST_QUEUED", f"S3 Inventory manifest import queued from {payload.manifest_uri}", source_id=source.id)
    session.commit()
    return {"source_id": source.id, "status": source.status, "job_id": job.id, "mode": job.mode}


@app.get("/api/sources/{source_id}/summary")
def source_summary(source_id: int, session: Session = Depends(get_session)) -> dict:
    source_or_404(session, source_id)
    count, bytes_total = session.execute(select(func.count(ObjectRecord.id), func.coalesce(func.sum(ObjectRecord.size_bytes), 0)).where(ObjectRecord.source_id == source_id)).one()
    states = dict(session.execute(
        select(ObjectRecord.state, func.count(ObjectRecord.id))
        .where(ObjectRecord.source_id == source_id)
        .group_by(ObjectRecord.state)
    ).all())
    source = source_or_404(session, source_id)
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
                                       "metadata_mismatches": source.destination_metadata_mismatch_count, "extras": source.destination_extra_count},
            "discovery": {"status": source.status, "requested_at": source.discovery_requested_at,
                          "completed_at": source.discovery_completed_at, "error": source.discovery_error,
                          "duration_seconds": int(discovery_duration_seconds),
                          "pages_completed": source.discovery_pages_completed,
                          "objects_inserted": source.discovery_objects_inserted,
                          "objects_per_second": round(count / discovery_duration_seconds, 2) if discovery_duration_seconds else 0,
                          "pages_per_minute": round(source.discovery_pages_completed * 60 / discovery_duration_seconds, 2) if discovery_duration_seconds else 0,
                          "can_resume": bool(source.discovery_continuation_token)}}


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
        select(ObjectRecord).where(ObjectRecord.source_id == source.id)
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
        start = None
        while True:
            arguments = {"prefix": source.s3_prefix, "limit": 1000, "fields": "name,size"}
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
            ObjectRecord.source_id == source.id, ObjectRecord.object_key.in_(divergent_keys[offset:offset + 1000])
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
    filters = [ObjectRecord.source_id == source_id]
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
    for obj in session.scalars(select(ObjectRecord).where(ObjectRecord.source_id == source_id).order_by(ObjectRecord.object_key)):
        writer.writerow([obj.object_key, obj.version_id or "", obj.size_bytes, obj.etag or "", obj.storage_class or "",
                         obj.last_modified.isoformat() if obj.last_modified else "", obj.state, obj.wave_id or "", obj.metadata_json, obj.tags_json])
    return StreamingResponse(iter([content.getvalue()]), media_type="text/csv", headers={"Content-Disposition": f'attachment; filename="source-{source_id}-inventory.csv"'})


def discovered_object_filters(source_id: int, prefix: str = "") -> list:
    filters = [ObjectRecord.source_id == source_id, ObjectRecord.state == ObjectState.DISCOVERED]
    if prefix.strip():
        filters.append(ObjectRecord.object_key.startswith(prefix.strip()))
    return filters


def assign_wave(session: Session, source_id: int, name: str, max_bytes: int, restore_days: int,
                restore_tier: str, objects: list[ObjectRecord], oversized: bool = False,
                *, planner_mode: str = "MANUAL", predicted_transfer_seconds: float = 0,
                prediction_samples: int = 0, planned_restore_at: datetime | None = None,
                planned_transfer_start_at: datetime | None = None,
                pipeline_run_id: int | None = None) -> Wave:
    assigned_bytes = sum(obj.size_bytes for obj in objects)
    wave = Wave(source_id=source_id, name=name, max_bytes=max_bytes, restore_days=restore_days,
                restore_tier=restore_tier, status="PLANNED", planner_mode=planner_mode,
                predicted_transfer_seconds=predicted_transfer_seconds, prediction_samples=prediction_samples,
                planned_restore_at=planned_restore_at, planned_transfer_start_at=planned_transfer_start_at,
                pipeline_run_id=pipeline_run_id)
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
    bucket = prediction_bucket(obj.size_bytes, settings.multipart_part_size_mib)
    profile = profiles.get(bucket, {})
    if profile.get("samples", 0) >= 5 and profile.get("p75_seconds", 0) > 0:
        return float(profile["p75_seconds"]), int(profile["samples"])
    # Link-model fallback: service/object overhead + the object's fair share
    # of configured aggregate bandwidth.  Multipart has a modest setup term.
    throughput_per_worker = max(1.0, settings.max_throughput_mbps / max(1, settings.transfer_workers))
    # Small-object operations are dominated by request, TLS and OCI write
    # overhead.  The prior 0.25-second cold estimate underpredicted the
    # Deep Archive validation workload by about 4x.  Use a conservative
    # baseline until this source has its own durable P75 history.
    seconds = 1.25 + (obj.size_bytes * 8 / (throughput_per_worker * 1_000_000))
    if obj.size_bytes >= settings.multipart_part_size_mib * 1024 ** 2:
        parts = math.ceil(obj.size_bytes / (settings.multipart_part_size_mib * 1024 ** 2))
        seconds += 1.0 + parts * .08
    return max(.25, seconds), 0


def dynamic_wave_plan(session: Session, source_id: int, max_bytes: int, target_transfer_seconds: int,
                      max_objects: int, prefix: str = "") -> dict:
    """Build, but do not persist, deterministic groups for the dynamic planner."""
    settings = runtime_settings(session)
    profiles = transfer_history_profiles(session, source_id, settings.multipart_part_size_mib)
    waves: list[dict] = []
    current: list[tuple[ObjectRecord, float, int]] = []
    bytes_total = predicted_sum = sample_count = 0

    def flush(exclusive: bool = False) -> None:
        nonlocal current, bytes_total, predicted_sum, sample_count
        if not current:
            return
        # Individual estimates represent worker time. Wall time is bounded by
        # aggregate throughput and by worker parallelism, plus a small setup.
        link_seconds = bytes_total * 8 / max(1, settings.max_throughput_mbps * 1_000_000)
        wall_seconds = max(link_seconds, predicted_sum / max(1, settings.transfer_workers)) + 30
        waves.append({"objects": list(current), "bytes": bytes_total, "object_count": len(current),
                      "predicted_transfer_seconds": math.ceil(wall_seconds), "prediction_samples": sample_count,
                      "exclusive": exclusive})
        current, bytes_total, predicted_sum, sample_count = [], 0, 0.0, 0

    for obj in session.scalars(select(ObjectRecord).where(*discovered_object_filters(source_id, prefix)).order_by(ObjectRecord.object_key, ObjectRecord.id)).yield_per(1000):
        predicted, samples = predict_object_transfer_seconds(obj, settings, profiles)
        projected_bytes = bytes_total + obj.size_bytes
        projected_count = len(current) + 1
        projected_sum = predicted_sum + predicted
        projected_wall = max(projected_bytes * 8 / max(1, settings.max_throughput_mbps * 1_000_000),
                             projected_sum / max(1, settings.transfer_workers)) + 30
        exceeds = projected_bytes > max_bytes or projected_count > max_objects or projected_wall > target_transfer_seconds
        if current and exceeds:
            flush()
        if not current:
            # An object beyond any hard/soft target forms an exclusive wave;
            # it is never silently skipped.
            current.append((obj, predicted, samples))
            bytes_total, predicted_sum, sample_count = obj.size_bytes, predicted, samples
            one_wall = max(bytes_total * 8 / max(1, settings.max_throughput_mbps * 1_000_000),
                           predicted_sum / max(1, settings.transfer_workers)) + 30
            if bytes_total > max_bytes or one_wall > target_transfer_seconds:
                flush(exclusive=True)
            continue
        current.append((obj, predicted, samples))
        # A wave may mix size classes. Keep the strongest historical sample
        # count as a confidence indicator; do not inflate it per object.
        bytes_total, predicted_sum, sample_count = projected_bytes, projected_sum, max(sample_count, samples)
    flush()
    return {"waves": waves, "profiles": profiles, "settings": settings}


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
    if session.scalar(select(Wave).where(Wave.source_id == source_id, Wave.name == payload.name)):
        raise HTTPException(status_code=409, detail="Wave name already exists for this source")
    objects, oversized = next_wave_objects(session, source_id, payload.max_bytes)
    if not objects:
        raise HTTPException(status_code=409, detail="No discovered objects are available for this wave")
    wave = assign_wave(session, source.id, payload.name, payload.max_bytes, payload.restore_days,
                       payload.restore_tier, objects, oversized)
    session.commit()
    return {"id": wave.id, "name": wave.name, "objects": len(objects), "bytes": sum(obj.size_bytes for obj in objects),
            "status": wave.status, "oversized": oversized}


@app.post("/api/sources/{source_id}/waves/automatic", status_code=201)
def create_automatic_waves(source_id: int, payload: AutomaticWaveCreate, session: Session = Depends(get_session)) -> dict:
    source = active_source_or_409(session, source_id)
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
                           payload.restore_tier, objects, oversized)
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
    # The first transfer cannot begin before the selected restore SLA.  The
    # former forecast started at ``now + safety``, which displayed an
    # impossible Bulk transfer window and released every restore immediately.
    first_tier = plans[0].get("restore_tier") if plans else "BULK"
    first_restore_lead = (48 if first_tier == "BULK" else 12) * 3600 + safety_seconds
    transfer_start = now + timedelta(seconds=first_restore_lead)
    times: list[tuple[datetime, datetime]] = []
    for plan in plans:
        # BULK maximum latency is 48h; Standard is planned conservatively at
        # 12h. The additional configured safety protects the handoff window.
        restore_lead = (48 if plan.get("restore_tier") == "BULK" else 12) * 3600 + safety_seconds
        restore_at = max(now, transfer_start - timedelta(seconds=restore_lead))
        times.append((restore_at, transfer_start))
        transfer_start += timedelta(seconds=plan["predicted_transfer_seconds"])
    return times


def release_dynamic_restore_horizon(session: Session, settings: RuntimeSettings, now: datetime | None = None) -> int:
    """Release only due restores inside each dynamic run's durable horizon.

    Waves remain fully planned and auditable from creation time, but charged
    S3 Batch jobs are emitted gradually.  A slot is occupied from submission
    until the wave reaches a terminal success state; this avoids restoring an
    unbounded set of temporary copies while preserving the transfer pipeline.
    """
    now = now or utcnow()
    released = 0
    active_statuses = {"RESTORE_REQUESTED", "RESTORE_REQUEST_ACCEPTED", "RESTORING", "RESTORED", "TRANSFERRING"}
    runs = list(session.scalars(select(DynamicPipelineRun).where(
        DynamicPipelineRun.scheduled_restores.is_(True),
        DynamicPipelineRun.status.not_in(["COMPLETED", "HISTORICAL"]),
    )))
    for run in runs:
        horizon = max(1, int(run.restore_horizon_waves or settings.dynamic_restore_horizon_waves or 1))
        waves = list(session.scalars(select(Wave).where(Wave.pipeline_run_id == run.id).order_by(
            Wave.planned_restore_at, Wave.id
        )))
        occupied = sum(1 for wave in waves if wave.status in active_statuses or (
            wave.status == "RESTORE_SCHEDULED" and session.scalar(select(Task.id).where(
                Task.wave_id == wave.id, Task.kind == "SUBMIT_BATCH_RESTORE"
            ).limit(1)) is not None
        ))
        for wave in waves:
            if occupied >= horizon:
                break
            if wave.status != "RESTORE_SCHEDULED" or (wave.planned_restore_at and wave.planned_restore_at > now):
                continue
            exists = session.scalar(select(Task.id).where(
                Task.wave_id == wave.id, Task.kind == "SUBMIT_BATCH_RESTORE"
            ).limit(1))
            if exists is not None:
                continue
            session.add(Task(wave_id=wave.id, kind="SUBMIT_BATCH_RESTORE", available_at=now))
            record_event(session, "DYNAMIC_RESTORE_RELEASED",
                         f"Dynamic pipeline run {run.id} released restore for wave '{wave.name}' within horizon {horizon}",
                         source_id=wave.source_id, wave_id=wave.id)
            occupied += 1
            released += 1
        refresh_dynamic_pipeline_run(session, run)
    return released


@app.get("/api/sources/{source_id}/waves/dynamic-preview")
def preview_dynamic_waves(source_id: int, max_bytes: int = Query(gt=0, le=10 * 1024**4),
                          target_transfer_seconds: int = Query(ge=300, le=7 * 24 * 3600),
                          max_objects: int = Query(ge=1, le=500000), prefix: str = Query(default="", max_length=1024),
                          restore_tier: str = Query(default="BULK", pattern="^(BULK|STANDARD)$"),
                          session: Session = Depends(get_session)) -> dict:
    active_source_or_409(session, source_id)
    result = dynamic_wave_plan(session, source_id, max_bytes, target_transfer_seconds, max_objects, prefix)
    plans = result["waves"]
    for plan in plans:
        plan["restore_tier"] = restore_tier
    times = dynamic_schedule_times(utcnow(), plans, result["settings"].dynamic_restore_safety_seconds)
    return {
        "objects": sum(plan["object_count"] for plan in plans), "bytes": sum(plan["bytes"] for plan in plans),
        "estimated_waves": len(plans), "target_transfer_seconds": target_transfer_seconds,
        "max_objects": max_objects, "profiles": result["profiles"],
        "historical_samples": sum(value["samples"] for value in result["profiles"].values()),
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
    result = dynamic_wave_plan(session, source_id, payload.max_bytes, payload.target_transfer_seconds, payload.max_objects, payload.prefix)
    plans = result["waves"]
    if not plans:
        raise HTTPException(status_code=409, detail="No discovered objects match this dynamic-wave selection")
    if len(plans) > 10000:
        raise HTTPException(status_code=422, detail="Dynamic plan exceeds the safety limit of 10000 waves. Increase the target size, time, or object limit.")
    for plan in plans:
        plan["restore_tier"] = payload.restore_tier
    if payload.schedule_restores and not result["settings"].dynamic_pipeline_enabled:
        raise HTTPException(status_code=422, detail="Enable and save the dynamic restore pipeline in Settings before scheduling early restores.")
    schedule_restores = payload.schedule_restores
    times = dynamic_schedule_times(utcnow(), plans, result["settings"].dynamic_restore_safety_seconds)
    historical_samples = sum(value["samples"] for value in result["profiles"].values())
    run = DynamicPipelineRun(
        source_id=source.id, status="SCHEDULED" if schedule_restores else "PLANNED",
        target_max_bytes=payload.max_bytes, target_transfer_seconds=payload.target_transfer_seconds,
        max_objects=payload.max_objects, restore_safety_seconds=result["settings"].dynamic_restore_safety_seconds,
        restore_days=payload.restore_days, restore_tier=payload.restore_tier,
        transfer_strategy=source.transfer_strategy, scheduled_restores=schedule_restores,
        restore_horizon_waves=result["settings"].dynamic_restore_horizon_waves,
        historical_samples=historical_samples,
    )
    session.add(run); session.flush()
    created: list[Wave] = []
    for sequence, (plan, timing) in enumerate(zip(plans, times), start=1):
        name = automatic_wave_name(session, source, payload.prefix, sequence)
        objects = [obj for obj, _prediction, _samples in plan["objects"]]
        for obj, prediction, _samples in plan["objects"]:
            obj.planned_transfer_seconds = prediction
        wave = assign_wave(session, source.id, name, payload.max_bytes, payload.restore_days, payload.restore_tier,
                           objects, plan["exclusive"], planner_mode="DYNAMIC",
                           predicted_transfer_seconds=plan["predicted_transfer_seconds"],
                           prediction_samples=plan["prediction_samples"], planned_restore_at=timing[0],
                           planned_transfer_start_at=timing[1], pipeline_run_id=run.id)
        if schedule_restores:
            wave.status = "RESTORE_SCHEDULED"
            record_event(session, "DYNAMIC_RESTORE_SCHEDULED",
                         f"Wave '{wave.name}' restore scheduled for {timing[0].isoformat()} before predicted transfer window {timing[1].isoformat()}",
                         source_id=source.id, wave_id=wave.id)
        created.append(wave)
    record_event(session, "DYNAMIC_WAVES_CREATED",
                 f"Dynamic pipeline run {run.id} created {len(created)} wave(s); historical samples: {historical_samples}; restore scheduling {'enabled' if schedule_restores else 'not enabled'}",
                 source_id=source.id)
    if schedule_restores:
        release_dynamic_restore_horizon(session, result["settings"])
    session.commit()
    return {"waves": len(created), "objects": sum(plan["object_count"] for plan in plans),
            "bytes": sum(plan["bytes"] for plan in plans), "scheduled_restores": schedule_restores,
            "historical_samples": historical_samples, "pipeline_run_id": run.id,
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
    transferring_wave_ids = set(session.scalars(
        select(Task.wave_id).join(Wave).where(Wave.source_id == source_id, Task.kind == "TRANSFER_WAVE",
                                               Task.state == TaskState.RUNNING)
    ))
    ready_transfer_wave_ids = set(session.scalars(
        select(Task.wave_id).join(Wave).where(Wave.source_id == source_id, Task.kind == "TRANSFER_WAVE",
                                               Task.state == TaskState.READY)
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
        if wave.id in transferring_wave_ids:
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
    return {"wave_id": wave_id, "status": wave.status, "objects": total_objects, "bytes": total_bytes, "object_states": by_state,
            "summary": {"transfer_elapsed_seconds": transfer_elapsed_seconds,
                        "transfer_started_at": transfer_started_at, "transfer_completed_at": transfer_completed_at,
                        "transferred_files": int(transferred_files or 0), "transferred_bytes": int(transferred_bytes or 0),
                        "failed_objects": int(failed_objects or 0), "failed_tasks": failed_tasks,
                        "failures_or_errors": int(failed_objects or 0) + failed_tasks},
            "batch": {"job_id": wave.batch_job_id, "status": wave.batch_job_status, "manifest_key": wave.manifest_key, "last_poll_at": wave.last_poll_at, "poll_count": wave.poll_count},
            "restore_attempts": [{"id": attempt.id, "job_id": attempt.job_id, "region": attempt.aws_region, "status": attempt.job_status,
                                  "expected": attempt.expected_objects, "succeeded": attempt.succeeded_objects, "failed": attempt.failed_objects,
                                  "manifest_key": attempt.manifest_key, "report_manifest_key": attempt.report_manifest_key,
                                  "submission": "AWS_ACCEPTED" if attempt.job_id else "NOT_SUBMITTED",
                                  "completion_evidence": "PUBLISHED" if attempt.report_manifest_key else "PENDING",
                                  "request_metrics": {"describe_requests": int(attempt.batch_describe_requests or 0),
                                                      "completion_report_list_requests": int(attempt.completion_report_list_requests or 0),
                                                      "completion_report_get_requests": int(attempt.completion_report_get_requests or 0)},
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
            "integrity": {"verified": integrity_verified, "failed": integrity_failed, "pending": total_objects - integrity_verified - integrity_failed},
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
    return {"source_id": source.id, "source_name": source.name, "connection_label": source.aws_connection.label if source.aws_connection else None,
            "currency": estimates[0]["currency"] if estimates else "USD",
            "waves": len(estimates), "estimated_objects": sum(item["quantities"]["objects"] for item in estimates),
            "unassigned_objects": unassigned_objects, "unassigned_bytes": unassigned_bytes,
            "complete": bool(estimates) and not unassigned_objects and all(item["complete"] for item in estimates),
            "totals": {"one_time": total("one_time"), "recurring_monthly": total("recurring_monthly"), "optional_deep_audit": total("optional_deep_audit")},
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
    wave.status = "PAUSED"
    record_event(session, "WAVE_PAUSED", f"Wave '{wave.name}' paused by operator", source_id=wave.source_id, wave_id=wave.id)
    session.commit()
    return {"wave_id": wave.id, "status": wave.status}


@app.post("/api/waves/{wave_id}/resume")
def resume_wave(wave_id: int, session: Session = Depends(get_session)) -> dict:
    wave = wave_or_404(session, wave_id)
    if wave.status != "PAUSED":
        raise HTTPException(status_code=409, detail="Only a paused wave can be resumed")
    wave.status = "READY_FOR_RESTORE"
    queued = session.scalar(select(Task.id).where(Task.wave_id == wave.id, Task.state.in_([TaskState.READY, TaskState.RUNNING])))
    if not queued:
        session.add(Task(wave_id=wave.id, kind="SUBMIT_BATCH_RESTORE"))
    record_event(session, "WAVE_RESUMED", f"Wave '{wave.name}' resumed by operator", source_id=wave.source_id, wave_id=wave.id)
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
def reprocess_wave(wave_id: int, session: Session = Depends(get_session)) -> dict:
    """Queue a new controlled restore submission; no external call is made here."""
    wave = wave_or_404(session, wave_id)
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
    reset_states = [ObjectState.RESTORE_REQUESTED, ObjectState.RESTORE_REQUEST_ACCEPTED, ObjectState.RESTORING]
    for obj in session.scalars(select(ObjectRecord).where(ObjectRecord.wave_id == wave.id, ObjectRecord.state.in_(reset_states))):
        obj.state, obj.restored_at = ObjectState.WAVE_ASSIGNED, None
    wave.status, wave.batch_job_id, wave.batch_job_status = "READY_FOR_RESTORE", None, None
    wave.manifest_key, wave.manifest_etag, wave.last_poll_at, wave.poll_count = None, None, None, 0
    wave.availability_head_requests, wave.availability_poll_elapsed_seconds = 0, 0
    wave.availability_throttle_retries = 0
    wave.last_availability_poll_objects, wave.last_availability_poll_seconds = 0, 0
    session.add(Task(wave_id=wave.id, kind="SUBMIT_BATCH_RESTORE"))
    record_event(session, "WAVE_REPROCESS_QUEUED", f"New restore submission queued for wave '{wave.name}' by operator", source_id=wave.source_id, wave_id=wave.id)
    session.commit()
    return {"wave_id": wave.id, "status": wave.status, "message": "Restore task queued"}


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
    task = session.scalar(select(Task).join(Wave).where(available, Task.available_at <= now, Wave.status != "PAUSED").order_by(Task.available_at, Task.id).with_for_update(skip_locked=True).limit(1))
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


@app.post("/api/tasks/{task_id}/simulate")
def simulate_task(task_id: int, payload: SimulationTaskUpdate, session: Session = Depends(get_session)) -> dict:
    """Advance a task without any AWS/OCI call; restricted to explicit simulation mode."""
    settings = runtime_settings(session)
    if not settings.simulation_enabled:
        raise HTTPException(status_code=409, detail="Simulation mode is disabled")
    task = task_or_404(session, task_id)
    if task.state != TaskState.RUNNING or task.worker_id != payload.worker_id:
        raise HTTPException(status_code=409, detail="Task is not leased by this worker")
    wave = task.wave
    objects = list(session.scalars(select(ObjectRecord).where(ObjectRecord.wave_id == wave.id)))
    next_kind: str | None = None
    if task.kind == "SUBMIT_BATCH_RESTORE":
        for obj in objects:
            if obj.state == ObjectState.WAVE_ASSIGNED:
                obj.state = ObjectState.RESTORE_REQUESTED
        wave.status = "RESTORE_REQUESTED"
        next_kind = "POLL_RESTORE"
    elif task.kind == "POLL_RESTORE":
        for obj in objects:
            if obj.state in [ObjectState.RESTORE_REQUESTED, ObjectState.RESTORING]:
                obj.state = ObjectState.RESTORED
                obj.restored_at = utcnow()
        wave.status = "RESTORED"
        next_kind = "TRANSFER_WAVE"
    elif task.kind == "TRANSFER_WAVE":
        for obj in objects:
            if obj.state == ObjectState.RESTORED:
                obj.state = ObjectState.TRANSFERRED
                obj.transferred_at = utcnow()
        wave.status = "TRANSFERRED"
    elif task.kind == "VERIFY_WAVE":
        for obj in objects:
            if obj.state == ObjectState.TRANSFERRED:
                obj.state = ObjectState.VERIFIED
                obj.integrity_verified_at = utcnow()
        wave.status = "VERIFIED"
    else:
        raise HTTPException(status_code=422, detail=f"Task kind '{task.kind}' is not supported by simulation")
    task.state = TaskState.SUCCEEDED
    task.lease_expires_at = None
    task.error = None
    if next_kind:
        session.add(Task(wave_id=wave.id, kind=next_kind))
    record_event(session, "TASK_SIMULATED", f"Simulated {task.kind} for wave '{wave.name}'", source_id=wave.source_id, wave_id=wave.id)
    session.commit()
    return {"task_id": task.id, "state": task.state, "wave_id": wave.id, "wave_status": wave.status, "next_task": next_kind}


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
