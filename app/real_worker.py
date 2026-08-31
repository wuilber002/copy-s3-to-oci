"""Durable AWS S3 to OCI Object Storage worker.

It is deliberately separate from the API process. All external calls happen
only after durable state is committed, and all progress is represented by a
task lease or source discovery state so a VM restart is recoverable.
"""

from __future__ import annotations

import base64
import csv
import gzip
import hashlib
import io
import json
import math
import os
import re
import socket
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import quote, unquote, urlparse
from uuid import UUID, NAMESPACE_URL, uuid4, uuid5

import boto3
import oci
from botocore.config import Config
from botocore.exceptions import ClientError
from sqlalchemy import and_, case, func, or_, select, update

from app.backend_contracts import (
    DescribeRestoreBatchRequest,
    ExecutionContext,
    HeadObjectRequest,
    ListObjectsRequest,
    LogicalTransferRequest,
    MultipartCommitRequest,
    MultipartCreateRequest,
    MultipartPartEvidence,
    MultipartPartRequest,
    ObjectDescriptor,
    ObjectIdentity,
    PutObjectRequest,
    ReadRangeRequest,
    SubmitRestoreBatchRequest,
)
from app.simulator_admin import SimulatorAdminClient
from app.simulator_ports import SimulatedDestinationPort, SimulatedSourcePort, SimulatorTransportError
from app.main import (
    AwsConnection, DiscoveryJob, Event, ObjectRecord, ObjectState, RestoreAttempt, RestoreObjectResult, SessionLocal, Source, Task, TaskState, TransferDispatchBatch, TransferLaneSegment, TransferQueueItem, TransferQueueState, merge_discovery_rows, source_key_in_scope, source_prefix_values,
    DynamicPipelineRun, RAIJU_MIN_WORKERS, TRANSFER_LANE_CLAIM_CANDIDATE_PAGE_SIZE, Wave, cloud_backend, enqueue_available_transfer_objects, materialize_dynamic_pipeline_horizon, parse_aws_connection_payload, read_oci_runtime_config, reconcile_archived_source_work, refresh_dynamic_pipeline_run, refresh_due_global_aws_pricing, refresh_transfer_queue_priorities, release_dynamic_restore_horizon, replan_dynamic_pipeline, restore_availability_poll_delay_seconds, restore_result_diagnostics, runtime_context, runtime_settings, utcnow,
)

# Raiju is the operational worker identity.  Raikou is the separate governance
# identity that plans and protects the migration; both use the same durable
# queue and roles remain intentionally small and explicit.
# Keep the hostname fallback for local development/tests that do not use it.
WORKER_ID = os.getenv("RAIJIN_WORKER_ID", f"raiju-{socket.gethostname()}")
# The two worker identities share PostgreSQL's durable queue but never claim
# each other's responsibility. ``all`` preserves a simple local invocation.
WORKER_ROLE = os.getenv("RAIJIN_WORKER_ROLE", "all").strip().lower()
RAIKOU_TASK_KINDS = frozenset({"SUBMIT_BATCH_RESTORE", "POLL_RESTORE", "VERIFY_WAVE"})
RAIJU_TASK_KINDS = frozenset({"TRANSFER_CONTINUOUS"})
# Compatibility aliases are private Python names only; the console and
# runtime identities use Raikou/Raiju.
GOVERNANCE_TASK_KINDS = RAIKOU_TASK_KINDS
TRANSFER_TASK_KINDS = RAIJU_TASK_KINDS
RAIJU_MAX_WORKERS = 64
# A cooperative handoff never interrupts an object or a multipart part.  This
# short fence only suppresses repeated *handoff decisions* while an already
# chosen normal Raiju is still completing its current object.  Urgent objects
# remain eligible for the next free slot at all times.
COOPERATIVE_PREEMPTION_COOLDOWN_SECONDS = 60
ARCHIVE_CLASSES = {"GLACIER", "DEEP_ARCHIVE", "INTELLIGENT_TIERING_ARCHIVE_ACCESS", "INTELLIGENT_TIERING_DEEP_ARCHIVE_ACCESS"}


class RestoreReapprovalRequired(RuntimeError):
    """A restored copy disappeared before transfer and a paid retry is required."""


class SimulatedNetworkRecoveryPending(RuntimeError):
    """Transfer is waiting for a deterministic simulated network recovery."""

    def __init__(self, retry_after_virtual_seconds: int):
        self.retry_after_virtual_seconds = max(1, int(retry_after_virtual_seconds))
        super().__init__(
            "Simulated network outage; advanced the isolated virtual clock "
            f"by {self.retry_after_virtual_seconds}s to its recovery point"
        )


def simulated_network_retry_after(error: Exception) -> int | None:
    """Extract the versioned simulator outage hint from a typed 503 response."""
    if not isinstance(error, SimulatorTransportError):
        return None
    text = str(error)
    if "SIMULATED_NETWORK_UNAVAILABLE" not in text:
        return None
    match = re.search(r'"retry_after_virtual_seconds"\s*:\s*(\d+)', text)
    return max(1, int(match.group(1))) if match else 1
# Restore status is not queryable by an arbitrary set of object keys through
# ListObjectsV2. Poll only objects assigned to the wave with HeadObject rather
# than repeatedly scanning a full source prefix that may contain many other
# waves. Requests are bounded concurrently to preserve predictable pressure
# on S3 and the VM; this changes speed, not the number of billed HEAD calls.
RESTORE_POLL_HEAD_CONCURRENCY = 10
RESTORE_POLL_HEAD_REQUESTS_PER_SECOND = 10
RESTORE_POLL_HEAD_BATCH_SIZE = 1_000
# A discovery response contains at most 1,000 S3 keys.  Persisting each
# response separately is very safe, but it turns a 100-million-object bucket
# into 100,000 PostgreSQL commits.  Commit a bounded group atomically instead:
# the object rows and its continuation token always advance together.  After a
# sudden VM stop Raijin can therefore replay at most nine already-read pages,
# never lose a confirmed page and never issue a per-object database lookup.
DISCOVERY_MAX_KEYS = 1000
DISCOVERY_CHECKPOINT_PAGES = 10
# The source scan has one worker and uses a deliberate ceiling of ten List API
# calls per second. It is far below S3's documented scaling envelope, while
# keeping the customer's API request burst and the VM/database pressure
# predictable. SlowDown after the SDK retry budget receives exponential wait.
DISCOVERY_REQUEST_INTERVAL_SECONDS = 0.1
DISCOVERY_MAX_THROTTLE_RETRIES = 5
# Explicit network bounds prevent an interrupted TCP stream from holding the
# only real worker forever. A task-level retry preserves its multipart
# checkpoint and lets the worker resume the accepted OCI parts safely.
AWS_CLIENT_CONFIG = Config(connect_timeout=10, read_timeout=120, retries={"max_attempts": 4, "mode": "standard"})

# Retrying an invalid policy, a missing bucket, or a malformed Batch request
# only produces charged calls and hides the actionable fault.  Keep this list
# deliberately conservative: unknown programming/configuration errors fail
# visibly, while known service/network pressure is retried by the durable task.
TRANSIENT_AWS_CODES = {
    "RequestTimeout", "RequestTimeoutException", "SlowDown", "Throttling",
    "ThrottlingException", "TooManyRequestsException", "RequestLimitExceeded",
    "InternalError", "ServiceUnavailable", "ServiceUnavailableException",
}
PERMANENT_AWS_CODES = {
    "AccessDenied", "AccessDeniedException", "AllAccessDisabled", "NoSuchBucket",
    "NoSuchKey", "NoSuchVersion", "InvalidRequest", "InvalidArgument",
    "InvalidManifestContent", "MalformedPolicyDocument", "ValidationException",
    "KMS.NotFoundException", "KMS.AccessDeniedException",
}


def classify_task_error(error: Exception) -> tuple[str, str]:
    """Classify an external worker failure without exposing AWS response text.

    Returns ``(retry|failed, safe_summary)``.  The SDK already retries bounded
    request attempts; this decision controls only the durable queue retry.
    """
    response = getattr(error, "response", None) or {}
    metadata, details = response.get("ResponseMetadata", {}), response.get("Error", {})
    status, code = metadata.get("HTTPStatusCode"), details.get("Code")
    name = type(error).__name__
    summary = f"{name} ({status} {code})" if status or code else f"{name}: {str(error)[:500]}"
    if isinstance(error, SimulatedNetworkRecoveryPending):
        return "retry", str(error)
    if isinstance(error, SimulatorTransportError):
        # Contract/validation responses are deterministic and require a code
        # or scenario correction.  Connection failures are retryable through
        # the same durable task policy used for real provider interruptions.
        return ("failed" if "HTTP 4" in str(error) else "retry"), summary
    if code in TRANSIENT_AWS_CODES or status in {429, 500, 502, 503, 504}:
        return "retry", summary
    if code in PERMANENT_AWS_CODES or (isinstance(status, int) and 400 <= status < 500):
        return "failed", summary
    if name in {"EndpointConnectionError", "ConnectTimeoutError", "ReadTimeoutError", "ConnectionClosedError"}:
        return "retry", summary
    return "failed", summary


def restored_object_is_unavailable(error: Exception) -> bool:
    """Return whether a transfer hit an expired/unavailable restored copy.

    ``InvalidObjectState`` is the S3 response for an archived object that is
    not currently restored.  It is deliberately narrower than generic 4xx
    errors: permissions, deleted keys and malformed requests must remain
    independently actionable and must never be misrepresented as a billable
    restore re-approval.
    """
    if isinstance(error, SimulatorTransportError):
        return "HTTP 409" in str(error)
    if isinstance(error, ClientError):
        code = ((error.response or {}).get("Error") or {}).get("Code")
        return code in {"InvalidObjectState", "RestoreNotAvailable"}
    return False


def event(session, kind: str, message: str, source_id: int | None = None, wave_id: int | None = None) -> None:
    session.add(Event(kind=kind, message=message, source_id=source_id, wave_id=wave_id))


def connection_values(connection: AwsConnection) -> dict[str, str]:
    """Read the current connection payload; credentials never enter PostgreSQL."""
    runtime_context.require_real_cloud("read OCI Secret for AWS connection")
    signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
    bundle = oci.secrets.SecretsClient({}, signer=signer).get_secret_bundle(connection.secret_ocid).data
    values = parse_aws_connection_payload(base64.b64decode(bundle.secret_bundle_content.content).decode("utf-8").strip())
    if values["aws_account_id"] != connection.aws_account_id:
        raise RuntimeError("AWS connection Secret account ID no longer matches its registered connection")
    return values


def aws_operation_config(source: Source, settings) -> dict[str, str]:
    """Resolve the immutable source-specific AWS connection configuration."""
    if not source.aws_connection:
        raise RuntimeError("Source has no AWS connection; migrate or archive it before worker execution")
    values = connection_values(source.aws_connection)
    return {
        "access_key_id": values["bootstrap_access_key_id"],
        "secret_access_key": values["bootstrap_secret_access_key"],
        "migration_role_arn": values["migration_role_arn"],
        "batch_role_arn": values["batch_operations_role_arn"],
        "control_bucket": values["control_bucket"],
        # Generated IDs, never a human label, isolate manifests/reports.
        "control_prefix": f"raijin/connections/{source.aws_connection.id}/sources/{source.id}",
        "expected_account_id": values["aws_account_id"],
    }


def aws_clients(settings, region: str, source: Source):
    values = aws_operation_config(source, settings)
    bootstrap = boto3.Session(
        aws_access_key_id=values["access_key_id"],
        aws_secret_access_key=values["secret_access_key"],
        region_name=region,
    )
    assumed = bootstrap.client("sts", config=AWS_CLIENT_CONFIG).assume_role(
        RoleArn=values["migration_role_arn"],
        RoleSessionName="s3-oci-migration-worker",
        DurationSeconds=3600,
    )["Credentials"]
    session = boto3.Session(
        aws_access_key_id=assumed["AccessKeyId"],
        aws_secret_access_key=assumed["SecretAccessKey"],
        aws_session_token=assumed["SessionToken"],
        region_name=region,
    )
    account_id = session.client("sts", config=AWS_CLIENT_CONFIG).get_caller_identity()["Account"]
    if values["expected_account_id"] and account_id != values["expected_account_id"]:
        raise RuntimeError("Assumed AWS account does not match the registered AWS connection")
    return (
        session.client("s3", config=AWS_CLIENT_CONFIG),
        session.client("s3control", config=AWS_CLIENT_CONFIG),
        account_id,
    )


def worker_can_reclaim_lease(task_worker_id: str | None, lease_expires_at, now) -> bool:
    """Whether this single-VM worker may recover an interrupted task now."""
    return task_worker_id == WORKER_ID or not lease_expires_at or lease_expires_at < now


@contextmanager
def task_lease_heartbeat(task_id: int, lease_seconds: int):
    """Renew a long-running task lease from an independent DB session.

    AWS calls can legitimately outlive the default five-minute lease. The
    heartbeat prevents another worker process from reclaiming the same durable
    task while the original process is still polling or waiting on SDK retries.
    """
    stop = threading.Event()
    interval = max(5, min(60, max(1, lease_seconds) // 3))

    def renew() -> None:
        while not stop.wait(interval):
            try:
                with SessionLocal() as heartbeat_session:
                    heartbeat_session.execute(update(Task).where(
                        Task.id == task_id,
                        Task.state == TaskState.RUNNING,
                        Task.worker_id == WORKER_ID,
                    ).values(lease_expires_at=utcnow() + timedelta(seconds=lease_seconds)))
                    heartbeat_session.commit()
            except Exception as error:
                print(f"task lease heartbeat failed for task {task_id}: {type(error).__name__}", flush=True)

    thread = threading.Thread(target=renew, name=f"task-{task_id}-lease-heartbeat", daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=max(1, min(5, interval)))


def claim_task(session, lease_seconds: int, allowed_kinds: frozenset[str] | None = None) -> Task | None:
    now = utcnow()
    # The worker id is stable for this single-VM architecture.  After a
    # controlled service/VM restart, the replacement process can therefore
    # reclaim *its own* in-flight lease immediately and resume a multipart
    # checkpoint instead of idling until the previous lease timeout. A task
    # owned by another worker remains protected until that lease expires.
    available = (Task.state == TaskState.READY) | (
        (Task.state == TaskState.RUNNING) & (
            (Task.lease_expires_at < now) | (Task.worker_id == WORKER_ID)
        )
    )
    query = select(Task).join(Wave).join(Source).where(
        available, Task.available_at <= now, Wave.status != "PAUSED", Source.archived_at.is_(None)
    )
    if allowed_kinds is not None:
        query = query.where(Task.kind.in_(allowed_kinds))
    candidates = list(session.scalars(
        query.order_by(Task.available_at, Task.id).with_for_update(skip_locked=True).limit(64)
    ))
    task = None
    for candidate in candidates:
        if candidate.kind != "TRANSFER_CONTINUOUS":
            task = candidate
            break
        # One dispatcher per source protects object leases while allowing an
        # independent source lane to run in another Raiju process.  The
        # future project scheduler can widen this scope without changing the
        # item lease contract.
        live_same_source = session.scalar(select(Task.id).join(Wave).where(
            Task.kind == "TRANSFER_CONTINUOUS",
            Task.state == TaskState.RUNNING,
            Task.lease_expires_at >= now,
            Wave.source_id == candidate.wave.source_id,
            Task.id != candidate.id,
        ).limit(1))
        if live_same_source is None:
            task = candidate
            break
    if not task:
        return None
    task.state, task.worker_id = TaskState.RUNNING, WORKER_ID
    task.attempts += 1
    task.lease_expires_at = now + timedelta(seconds=lease_seconds)
    event(session, "TASK_CLAIMED", f"Task {task.id} claimed by {WORKER_ROLE} worker", wave_id=task.wave_id)
    session.commit()
    return task


def claim_discovery_job(session, lease_seconds: int) -> DiscoveryJob | None:
    """Claim one source discovery from its own durable queue."""
    now = utcnow()
    available = (DiscoveryJob.state == TaskState.READY) | (
        (DiscoveryJob.state == TaskState.RUNNING) & (
            (DiscoveryJob.lease_expires_at < now) | (DiscoveryJob.worker_id == WORKER_ID)
        )
    )
    job = session.scalar(
        select(DiscoveryJob)
        .where(available, DiscoveryJob.available_at <= now)
        .order_by(DiscoveryJob.available_at, DiscoveryJob.id)
        .with_for_update(skip_locked=True).limit(1)
    )
    if not job:
        return None
    job.state, job.worker_id = TaskState.RUNNING, WORKER_ID
    job.attempts += 1
    job.lease_expires_at = now + timedelta(seconds=max(lease_seconds, 300))
    event(session, "DISCOVERY_JOB_CLAIMED", f"Discovery job {job.id} claimed by real worker", source_id=job.source_id)
    session.commit()
    return job


def succeed(session, task: Task, next_kind: str | None = None) -> None:
    if next_kind:
        session.add(Task(wave_id=task.wave_id, kind=next_kind))
    task.state, task.lease_expires_at, task.error = TaskState.SUCCEEDED, None, None
    event(session, "TASK_SUCCEEDED", f"Real worker completed {task.kind}", wave_id=task.wave_id)
    session.commit()


def ensure_transfer_task(session, wave: Wave, settings=None) -> bool:
    """Ensure the source-scoped continuous lane has one Raiju dispatcher.

    The durable task anchors operational history to the first eligible wave,
    but it consumes object-level items from every eligible wave of the source.
    That distinction keeps waves as restore/accounting boundaries without
    allowing one partially restored wave to monopolize copy capacity.
    """
    source = wave.source
    live_lane = session.scalar(select(Task.id).join(Wave).join(Source).where(
        Task.kind == "TRANSFER_CONTINUOUS",
        Task.state.in_([TaskState.READY, TaskState.RUNNING]),
        Wave.source_id == source.id,
        Wave.status != "PAUSED",
        Source.archived_at.is_(None),
    ).limit(1))
    if live_lane is not None:
        return False
    # Leases and retry delays are wall-clock concerns.  Deadline scoring uses
    # the source clock, which may be virtual for a Fujin execution.
    refresh_transfer_queue_priorities(session, source.id, now=_continuous_source_now(source))
    now = utcnow()
    item = session.scalar(select(TransferQueueItem).join(Wave).join(Source).where(
        TransferQueueItem.source_id == source.id,
        TransferQueueItem.state.in_([
            TransferQueueState.READY,
            TransferQueueState.MULTIPART_RESUME,
            TransferQueueState.RETRY_WAIT,
        ]),
        Wave.status != "PAUSED", Source.archived_at.is_(None),
    ).order_by(
        # A delayed retry remains durable work.  Its dispatcher is queued for
        # the retry boundary instead of being lost after a previous batch
        # succeeds, while immediately eligible work still wins by priority.
        case((TransferQueueItem.retry_at.is_not(None), TransferQueueItem.retry_at), else_=now),
        TransferQueueItem.priority_score.desc(),
        TransferQueueItem.restore_expires_at.is_(None),
        TransferQueueItem.restore_expires_at,
        TransferQueueItem.available_at,
        TransferQueueItem.id,
    ).limit(1))
    if item is None:
        return False
    selected = session.get(Wave, item.wave_id)
    retry_boundary = item.retry_at if item.state == TransferQueueState.RETRY_WAIT else None
    if retry_boundary is not None and retry_boundary.tzinfo is None:
        # SQLite fixtures may round-trip a naive timestamp; queue leases are
        # always UTC wall time in production, so normalize at the boundary.
        retry_boundary = retry_boundary.replace(tzinfo=timezone.utc)
    session.add(Task(
        wave_id=selected.id,
        kind="TRANSFER_CONTINUOUS",
        available_at=max(now, retry_boundary) if retry_boundary else now,
    ))
    event(
        session,
        "CONTINUOUS_TRANSFER_DISPATCHER_QUEUED",
        f"Raikou queued the continuous transfer dispatcher from wave '{selected.name}'; "
        f"next item priority {item.priority_score}/100 ({item.priority_band})"
        f"{' after its retry boundary' if retry_boundary and retry_boundary > now else ''}.",
        source_id=source.id,
        wave_id=selected.id,
    )
    return True


def reconcile_restored_transfer_lane(session, settings=None) -> int:
    """Release the next eligible wave when Raiju's transfer lane is free.

    Restore polling is intentionally not the sole place that releases a
    transfer.  A poll can finish while a Raiju batch is active; without this
    reconciliation durable, available objects could remain idle after that
    batch drains.  There is no percentage threshold: object availability is
    the release condition for the continuous lane.

    The ordering and single-lane rules stay centralized in
    :func:`ensure_transfer_task`.  This pass merely retries the durable
    hand-off on subsequent governance cycles.
    """
    released = 0
    waves = session.scalars(
        select(Wave)
        .join(Source)
        .where(
            Wave.status.in_(["RESTORING", "RESTORE_DRAINING", "RESTORED", "TRANSFER_DRAINING"]),
            Source.archived_at.is_(None),
        )
        .order_by(Wave.source_id, Wave.planned_transfer_start_at, Wave.id)
    )
    for wave in waves:
        # Releases are idempotent. This also migrates any already-restored
        # operational work from the former wave-exclusive executor to the
        # continuous lane without another AWS call.
        pending_restore = session.scalar(select(func.count(ObjectRecord.id)).where(
            ObjectRecord.wave_id == wave.id,
            ObjectRecord.state.in_([
                ObjectState.RESTORE_REQUESTED, ObjectState.RESTORE_REQUEST_ACCEPTED,
                ObjectState.RESTORING,
            ]),
        )) or 0
        if wave.transfer_release_policy == "AS_OBJECTS_AVAILABLE" or not pending_restore:
            enqueue_available_transfer_objects(session, wave)
        if ensure_transfer_task(session, wave, settings):
            released += 1
            event(
                session,
                "RESTORED_TRANSFER_RECONCILED",
                "Restored wave promoted after its transfer lane became available",
                source_id=wave.source_id,
                wave_id=wave.id,
            )
    return released


def retry(session, task: Task, error: Exception | str, seconds: int | None = None) -> None:
    delay = seconds if seconds is not None else min(1800, max(60, task.attempts * 60))
    task.state, task.lease_expires_at = TaskState.READY, None
    task.available_at = utcnow() + timedelta(seconds=delay)
    task.error = str(error)[:8000]
    if task.kind == "TRANSFER_CONTINUOUS":
        wave = session.get(Wave, task.wave_id)
        # A retry remains a transfer in progress.  Reverting to RESTORED made
        # a single wave visually oscillate between RESTORING/TRANSFERRING and
        # obscured an otherwise healthy retry.
        if wave and wave.status == "TRANSFERRING":
            wave.status = "TRANSFERRING"
    event(session, "TASK_RETRY_QUEUED", f"{task.kind} retry in {delay}s: {task.error}", wave_id=task.wave_id)
    session.commit()


def fail_permanently(session, task: Task, error: str) -> None:
    """End a task that cannot succeed without an operator correction."""
    task.state, task.lease_expires_at, task.error = TaskState.FAILED, None, error[:8000]
    wave = session.get(Wave, task.wave_id)
    # A Batch job may have been accepted by AWS even when a later, local
    # polling/report-processing step fails. Persist that distinction on the
    # attempt itself so the operator can see that re-submitting a paid restore
    # is not necessarily required.
    if wave and task.kind == "POLL_RESTORE":
        attempt = session.scalar(
            select(RestoreAttempt)
            .where(RestoreAttempt.wave_id == wave.id)
            .order_by(RestoreAttempt.id.desc())
        )
        if attempt and attempt.job_id and not attempt.failure_summary:
            attempt.failure_summary = (
                "Raijin polling/report processing failed after AWS accepted "
                f"Batch job {attempt.job_id}: {task.error}"
            )[:8000]
    if wave and wave.status not in {"PAUSED", "VERIFIED", "RESTORE_REQUEST_FAILED", "RESTORE_REAPPROVAL_REQUIRED"}:
        wave.status = "FAILED"
    event(session, "TASK_FAILED_PERMANENTLY", f"{task.kind} requires operator action: {task.error}", wave_id=task.wave_id)
    session.commit()


def discover(session, source: Source, settings, job: DiscoveryJob | None = None) -> None:
    source.status, source.discovery_error = "DISCOVERING", None
    source.discovery_started_at = utcnow()
    session.commit()
    if runtime_context.is_simulation:
        SimulatorAdminClient(runtime_context.simulator_base_url).set_execution_state(
            source.simulation_execution_id, "RUNNING"
        )
    remote_discover(session, source, settings, job)


def simulation_execution_context(source: Source) -> ExecutionContext:
    required = {
        "execution": source.simulation_execution_id,
        "correlation": source.simulation_correlation_id,
        "tenant": source.simulation_tenant_id,
        "project": source.simulation_project_id,
    }
    missing = [name for name, value in required.items() if not value]
    if source.backend_kind != "SIMULATED" or missing:
        raise RuntimeError(
            "Simulated source has incomplete execution context: " + ", ".join(missing)
        )
    return ExecutionContext(
        execution_id=UUID(required["execution"]),
        correlation_id=UUID(required["correlation"]),
        tenant_id=UUID(required["tenant"]),
        project_id=UUID(required["project"]),
    )


def simulation_virtual_now(source: Source) -> datetime:
    """Read the source-owned virtual clock as a durable phase timestamp."""
    payload = SimulatorAdminClient(runtime_context.simulator_base_url).clock_status(
        source.simulation_execution_id
    )
    value = datetime.fromisoformat(str(payload["virtual_now"]).replace("Z", "+00:00"))
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


@contextmanager
def simulation_phase_clock_hold(source: Source, phase: str):
    """Prevent accelerated virtual time from consuming retention during work.

    PostgreSQL task leases and retry delays remain on wall-clock time.  Only
    the virtual AWS/OCI behavior is paused, so a long local streaming pass or
    many HeadObject calls cannot artificially expire a restored object.
    """
    if not runtime_context.is_simulation:
        yield
        return
    admin = SimulatorAdminClient(runtime_context.simulator_base_url)
    admin.control_clock(source.simulation_execution_id, "HOLD")
    try:
        yield
    finally:
        admin.control_clock(source.simulation_execution_id, "RELEASE")


def retain_simulation_clock_for_transfer(session, wave: Wave) -> None:
    """Keep virtual time frozen across the queue hand-off to transfer.

    The availability poll itself holds the accelerated clock.  Taking one
    additional durable hold before that poll releases closes the otherwise
    unavoidable gap until the continuous Raiju dispatcher is claimed.
    """
    if not runtime_context.is_simulation or wave.simulation_transfer_clock_held:
        return
    SimulatorAdminClient(runtime_context.simulator_base_url).control_clock(
        wave.source.simulation_execution_id, "HOLD"
    )
    wave.simulation_transfer_clock_held = True
    session.flush()


def release_simulation_clock_after_transfer(session, wave: Wave) -> None:
    if not runtime_context.is_simulation or not wave.simulation_transfer_clock_held:
        return
    try:
        SimulatorAdminClient(runtime_context.simulator_base_url).control_clock(
            wave.source.simulation_execution_id, "RELEASE"
        )
    finally:
        wave.simulation_transfer_clock_held = False
        session.flush()


def advance_simulation_clock(session, source: Source, seconds: float, reason: str,
                             wave: Wave | None = None) -> float:
    """Advance isolated cloud time only through a durable simulator decision.

    Simulation must never consume a restore-retention window merely because a
    real worker is processing a large manifest or waiting for its next lease.
    This helper is the sole path used by worker phases to move virtual time.
    """
    if not runtime_context.is_simulation or seconds <= 0:
        return 0
    bounded = max(0.0, float(seconds))
    SimulatorAdminClient(runtime_context.simulator_base_url).control_clock(
        source.simulation_execution_id, "ADVANCE", bounded
    )
    event(
        session,
        "SIMULATION_CLOCK_ADVANCED",
        f"Virtual clock advanced {bounded:.0f}s for {reason}",
        source_id=source.id,
        wave_id=wave.id if wave else None,
    )
    return bounded


def simulation_transfer_task_active(session, wave: Wave) -> bool:
    """Whether a Raiju dispatcher currently owns this source's lane."""
    return session.scalar(select(Task.id).join(Wave).where(
        Wave.source_id == wave.source_id,
        Task.kind == "TRANSFER_CONTINUOUS",
        # A READY dispatcher has not started I/O.  Treating it as active
        # freezes the virtual clock before any transfer takes place and can
        # stall the restore scheduler indefinitely.
        Task.state == TaskState.RUNNING,
    ).limit(1)) is not None


def synchronize_simulation_source_clocks(session) -> int:
    """Keep every source-owned virtual clock stopped between explicit ticks.

    Clock acceleration is a *planning/display* property, not permission for
    wall-clock worker time to burn through S3 restore retention.  Restore
    polling and the dynamic scheduler advance their own clock in durable,
    bounded increments via :func:`advance_simulation_clock`.
    """
    if not runtime_context.is_simulation:
        return 0
    admin = SimulatorAdminClient(runtime_context.simulator_base_url)
    changed = 0
    sources = list(session.scalars(select(Source).where(Source.backend_kind == "SIMULATED")))
    for source in sources:
        if not source.simulation_execution_id:
            continue
        try:
            clock = admin.clock_status(source.simulation_execution_id)
            if not clock.get("paused"):
                admin.control_clock(source.simulation_execution_id, "PAUSE")
                changed += 1
        except Exception:
            # A simulator status request is observability/control only.  The
            # durable task must retain its normal retry/error behavior.
            continue
    return changed


def simulation_restore_expiry_confirmed(session, wave: Wave, source: Source) -> bool:
    """Confirm expiry from durable evidence before asking for a paid restore.

    A simulated 409 by itself is not enough: a backend restart or clock-state
    mismatch must be diagnosed as a simulator defect, never presented to an
    operator as a new billable restore.  The retained expiry timestamps are
    observed from the same per-object ``HeadObject`` evidence used by the
    regular migration path.
    """
    if not runtime_context.is_simulation:
        return True
    now = simulation_virtual_now(source)
    expiries = list(session.scalars(select(ObjectRecord.restore_expires_at).where(
        ObjectRecord.wave_id == wave.id,
        ObjectRecord.state.in_([ObjectState.RESTORED, ObjectState.TRANSFERRING]),
        ObjectRecord.restore_expires_at.is_not(None),
    )))
    return bool(expiries) and now >= min(expiries)


def restore_reapproval_is_required(wave: Wave) -> bool:
    """Return whether a wave has reached the terminal restore approval gate.

    This is deliberately based on both the durable flag and the visible
    status.  A poll and a transfer run in separate database sessions, so a
    stale poll must never turn a stopped wave back into ``RESTORING`` after a
    transfer has detected that a new, potentially billable restore is needed.
    """
    return bool(wave.restore_reapproval_required) or wave.status == "RESTORE_REAPPROVAL_REQUIRED"


def cancel_superseded_wave_tasks(session, wave: Wave, active_task_id: int | None = None) -> int:
    """Cancel queued work that can no longer run after a restore expiry.

    Running work is not overwritten here because its worker owns the lease and
    will observe :func:`restore_reapproval_is_required` before persisting a
    later state transition.  Ready tasks, however, are safe to cancel now and
    must not be claimed after the explicit operator-approval gate is set.
    """
    tasks = list(session.scalars(select(Task).where(
        Task.wave_id == wave.id,
        Task.state == TaskState.READY,
    )))
    cancelled = 0
    for pending in tasks:
        if active_task_id is not None and pending.id == active_task_id:
            continue
        pending.state = TaskState.CANCELLED
        pending.lease_expires_at = None
        pending.error = "Superseded: a new restore requires explicit operator approval"
        cancelled += 1
    return cancelled


def finish_superseded_restore_poll(session, task: Task, wave: Wave) -> None:
    """Finish a stale polling task without changing a terminal wave state."""
    task.state, task.lease_expires_at = TaskState.CANCELLED, None
    task.error = "Superseded: the wave requires explicit approval for a new restore"
    event(
        session,
        "RESTORE_POLL_SUPERSEDED",
        "Availability polling stopped because the wave requires explicit approval for a new restore",
        source_id=wave.source_id,
        wave_id=wave.id,
    )
    session.commit()


def require_new_restore_approval(session, wave: Wave, reason: str) -> None:
    """Stop only this wave and retain clear evidence before another restore.

    The operator must later approve reprocessing explicitly.  No automatic
    re-submit is allowed because a second restore may incur AWS charges.
    """
    reason = reason[:8000]
    for obj in session.scalars(select(ObjectRecord).where(
        ObjectRecord.wave_id == wave.id,
        ObjectRecord.state.notin_([ObjectState.TRANSFERRED, ObjectState.VERIFIED]),
    )):
        obj.state = ObjectState.WAVE_ASSIGNED
        obj.restored_at = obj.restore_expires_at = None
    wave.status = "RESTORE_REAPPROVAL_REQUIRED"
    wave.restore_reapproval_required = True
    wave.restore_reapproval_reason = reason
    wave.restore_reapproval_detected_at = utcnow()
    cancelled = cancel_superseded_wave_tasks(session, wave)
    release_simulation_clock_after_transfer(session, wave)
    event(
        session,
        "RESTORE_REAPPROVAL_REQUIRED",
        "Restored copy became unavailable before transfer. Operator approval is required before a new paid restore"
        f"; {cancelled} queued task(s) cancelled: " + reason,
        source_id=wave.source_id,
        wave_id=wave.id,
    )
    session.commit()


def inventory_timestamp(value: str | None):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def import_s3_inventory_manifest(session, source: Source, settings, job: DiscoveryJob) -> None:
    """Stream every S3 Inventory shard directly from S3 with a durable cursor.

    Inventory CSV shards have no header. The manifest's ``fileSchema`` is the
    authority for their positional columns. The job cursor is a shard index
    plus the number of rows committed within that shard; an interruption can
    reread only the uncommitted tail and never exposes a partial inventory.
    """
    if not job.inventory_manifest_uri:
        raise RuntimeError("Inventory manifest URI is missing")
    parsed = urlparse(job.inventory_manifest_uri)
    manifest_bucket, manifest_key = parsed.netloc, parsed.path.lstrip("/")
    source.status, source.discovery_error, source.discovery_started_at = "DISCOVERING", None, utcnow()
    session.commit()
    s3, _, _ = aws_clients(settings, source.aws_region, source)
    manifest_response = s3.get_object(Bucket=manifest_bucket, Key=manifest_key)
    manifest = json.loads(manifest_response["Body"].read())
    schema = [field.strip() for field in str(manifest.get("fileSchema") or "").split(",")]
    files = manifest.get("files") or []
    if not schema or not files or "Key" not in schema or "Size" not in schema:
        raise RuntimeError("S3 Inventory manifest has no CSV files or required Key/Size schema")
    if manifest.get("sourceBucket") and str(manifest["sourceBucket"]).removeprefix("arn:aws:s3:::") != source.s3_bucket:
        raise RuntimeError("S3 Inventory manifest source bucket does not match this Raijin source")
    inserted = 0
    for file_index in range(job.inventory_file_index, len(files)):
        file_key = files[file_index].get("key")
        if not file_key:
            raise RuntimeError(f"S3 Inventory manifest shard {file_index} has no key")
        response = s3.get_object(Bucket=manifest_bucket, Key=file_key)
        body = response["Body"]
        raw = gzip.GzipFile(fileobj=body) if file_key.endswith(".gz") else body
        text_stream = io.TextIOWrapper(raw, encoding="utf-8", newline="")
        reader = csv.DictReader(text_stream, fieldnames=schema)
        skip_rows = job.inventory_rows_completed if file_index == job.inventory_file_index else 0
        pending: list[dict] = []
        processed_since_checkpoint = 0
        row_number = 0

        def checkpoint() -> None:
            nonlocal inserted, processed_since_checkpoint
            if not processed_since_checkpoint:
                return
            if pending:
                if job.is_rediscovery:
                    added, _updated, _changed = merge_discovery_rows(session, source, pending, job)
                    inserted += added
                    source.discovery_objects_inserted += added
                else:
                    session.bulk_insert_mappings(ObjectRecord, pending)
                    inserted += len(pending)
                    source.discovery_objects_inserted += len(pending)
            job.inventory_rows_completed = row_number
            job.lease_expires_at = utcnow() + timedelta(seconds=max(settings.task_lease_seconds, 300))
            session.commit()
            pending.clear()
            processed_since_checkpoint = 0

        try:
            for row in reader:
                row_number += 1
                if row_number <= skip_rows:
                    continue
                processed_since_checkpoint += 1
                key = unquote((row.get("Key") or "").strip())
                if key and source_key_in_scope(source, key):
                    try:
                        size = int(row.get("Size") or 0)
                    except ValueError:
                        size = -1
                    if size >= 0:
                        pending.append({
                            "source_id": source.id, "object_key": key,
                            "version_id": (row.get("VersionId") or "").strip() or None,
                            "size_bytes": size,
                            "etag": (row.get("ETag") or "").strip('"') or None,
                            "storage_class": (row.get("StorageClass") or "").strip() or None,
                            "last_modified": inventory_timestamp(row.get("LastModifiedDate")),
                            "state": ObjectState.DISCOVERED,
                        })
                if processed_since_checkpoint >= 5000:
                    checkpoint()
            checkpoint()
        finally:
            text_stream.close()
            body.close()
        job.inventory_file_index, job.inventory_rows_completed = file_index + 1, 0
        source.discovery_pages_completed += 1
        job.lease_expires_at = utcnow() + timedelta(seconds=max(settings.task_lease_seconds, 300))
        session.commit()
    finished_at = utcnow()
    if source.discovery_started_at:
        source.discovery_elapsed_seconds = float(source.discovery_elapsed_seconds or 0) + max(0, (finished_at - source.discovery_started_at).total_seconds())
    source.discovery_started_at, source.discovery_completed_at, source.status = None, finished_at, "DISCOVERED"
    source.last_discovery_mode, source.discovery_generation = job.mode, int(source.discovery_generation or 0) + 1
    job.state, job.error, job.lease_expires_at, job.completed_at = TaskState.SUCCEEDED, None, None, finished_at
    detail = f"new {job.objects_new}, updated {job.objects_updated}, changed {job.objects_changed}" if job.is_rediscovery else f"{inserted} object(s) in this run"
    event(session, "INVENTORY_MANIFEST_IMPORTED", f"S3 Inventory manifest import completed: {detail}; {source.discovery_objects_inserted} new object(s) across {source.discovery_pages_completed} shard(s)", source_id=source.id)
    session.commit()


def remote_discover(session, source: Source, settings, job: DiscoveryJob | None = None) -> None:
    simulated_port = (
        SimulatedSourcePort(runtime_context.simulator_base_url)
        if runtime_context.is_simulation and runtime_context.simulator_base_url
        else None
    )
    s3 = None
    if simulated_port is None:
        s3, _, _ = aws_clients(settings, source.aws_region, source)
    inserted = 0
    continuation_token = source.discovery_continuation_token
    prefixes = source_prefix_values(source)
    prefix_index = min(max(0, int(source.discovery_prefix_index or 0)), len(prefixes))
    pending_rows: list[dict] = []
    pending_pages = 0
    pending_token = continuation_token
    next_request_at = 0.0
    connection = source.aws_connection
    request_interval = (
        0.0
        if simulated_port
        else 1.0 / max(1, int(getattr(connection, "discovery_requests_per_second", 10) or 10))
    )

    def checkpoint_batch() -> None:
        """Atomically persist a bounded discovery batch and its S3 cursor."""
        nonlocal inserted, pending_pages
        if not pending_pages:
            return
        if pending_rows:
            # Mappings bypass the ORM identity map, keeping memory bounded even
            # when a bucket contains tens of millions of objects.
            if job and job.is_rediscovery:
                added, _updated, _changed = merge_discovery_rows(session, source, pending_rows, job)
                inserted += added
            else:
                session.bulk_insert_mappings(ObjectRecord, pending_rows)
                inserted += len(pending_rows)
        source.discovery_pages_completed += pending_pages
        source.discovery_objects_inserted += (inserted - source.discovery_objects_inserted) if job and job.is_rediscovery else len(pending_rows)
        source.discovery_continuation_token = pending_token
        source.discovery_prefix_index = prefix_index
        if job:
            job.lease_expires_at = utcnow() + timedelta(seconds=max(settings.task_lease_seconds, 300))
        session.commit()
        pending_rows.clear()
        pending_pages = 0

    while prefix_index < len(prefixes):
        request = {"Bucket": source.s3_bucket, "Prefix": prefixes[prefix_index], "MaxKeys": DISCOVERY_MAX_KEYS}
        if continuation_token:
            request["ContinuationToken"] = continuation_token
        throttle_attempt = 0
        while True:
            wait = next_request_at - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            try:
                if simulated_port:
                    simulated_page = simulated_port.list_objects(
                        ListObjectsRequest(
                            context=simulation_execution_context(source),
                            bucket=source.s3_bucket,
                            prefix=prefixes[prefix_index],
                            continuation_token=continuation_token,
                            max_keys=DISCOVERY_MAX_KEYS,
                        )
                    )
                    page = {
                        "Contents": [
                            {
                                "Key": item.key,
                                "Size": item.size_bytes,
                                "ETag": item.etag,
                                "StorageClass": item.storage_class,
                                "LastModified": item.last_modified,
                            }
                            for item in simulated_page.objects
                        ],
                        "NextContinuationToken": simulated_page.next_continuation_token,
                    }
                else:
                    page = s3.list_objects_v2(**request)
                next_request_at = time.monotonic() + request_interval
                break
            except ClientError as error:
                code = (error.response.get("Error") or {}).get("Code")
                if code not in {"SlowDown", "Throttling", "ThrottlingException", "RequestLimitExceeded"} or throttle_attempt >= DISCOVERY_MAX_THROTTLE_RETRIES:
                    raise
                throttle_attempt += 1
                # Do not checkpoint ahead of S3. The next request keeps the
                # same continuation token, so this retry cannot skip keys.
                time.sleep(min(30, 2 ** throttle_attempt))
                next_request_at = time.monotonic() + request_interval
        items = page.get("Contents", [])
        pending_rows.extend({
            "source_id": source.id,
            "object_key": item["Key"],
            "size_bytes": item["Size"],
            "etag": item.get("ETag", "").strip('"') or None,
            "storage_class": item.get("StorageClass"),
            "last_modified": item.get("LastModified"),
            "state": ObjectState.DISCOVERED,
        } for item in items)
        pending_pages += 1
        continuation_token = page.get("NextContinuationToken")
        if not continuation_token:
            prefix_index += 1
        pending_token = continuation_token
        if pending_pages >= DISCOVERY_CHECKPOINT_PAGES or not continuation_token:
            checkpoint_batch()
    finished_at = utcnow()
    if source.discovery_started_at:
        source.discovery_elapsed_seconds = float(source.discovery_elapsed_seconds or 0) + max(0, (finished_at - source.discovery_started_at).total_seconds())
    source.discovery_started_at = None
    source.discovery_continuation_token, source.discovery_prefix_index = None, 0
    source.status, source.discovery_completed_at = "DISCOVERED", finished_at
    source.last_discovery_mode, source.discovery_generation = "REMOTE_LIST", int(source.discovery_generation or 0) + 1
    if job:
        job.state, job.error, job.lease_expires_at, job.completed_at = TaskState.SUCCEEDED, None, None, finished_at
    detail = f"new {job.objects_new}, updated {job.objects_updated}, changed {job.objects_changed}" if job and job.is_rediscovery else f"inserted {inserted} object(s)"
    event(session, "DISCOVERY_COMPLETED", f"Remote discovery completed: {detail}; {source.discovery_objects_inserted} new object(s) across {source.discovery_pages_completed} ListObjectsV2 page(s)", source_id=source.id)
    session.commit()


def restored_from_head_response(response: dict) -> bool:
    """Whether S3's HeadObject Restore header says an archive is available."""
    restore = str(response.get("Restore") or "").lower()
    return 'ongoing-request="false"' in restore and "expiry-date=" in restore


def restore_expiry_from_head_response(response: dict):
    """Parse S3's documented `x-amz-restore` expiry date without another call."""
    restore = str(response.get("Restore") or "")
    match = re.search(r'expiry-date="([^"]+)"', restore, re.IGNORECASE)
    if not match:
        return None
    value = parsedate_to_datetime(match.group(1))
    return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)


def should_poll_restore_with_head(wave_archive_objects: int, source_objects: int) -> bool:
    """Compatibility helper: availability polling is now always wave-scoped."""
    return True


def restored_pending_archives_from_head(s3, source: Source, archives: list[ObjectRecord],
                                        progress_callback=None) -> tuple[dict[int, datetime | None], dict[str, float | int]]:
    """Return only wave objects observed as restored by bounded HeadObject calls.

    Each request is for one still-pending archive object belonging to this
    wave. S3 has no multi-key HeadObject API and ListObjectsV2 only supports a
    prefix, so this avoids scanning unrelated source objects while retaining
    durable per-object evidence.
    """
    ready: dict[int, datetime | None] = {}
    metrics: dict[str, float | int] = {"requests": 0, "throttle_retries": 0, "elapsed_seconds": 0.0}
    started_at = time.monotonic()
    connection = source.aws_connection
    bucket_name = source.s3_bucket
    requests_per_second = max(1, int(getattr(connection, "restore_poll_requests_per_second", RESTORE_POLL_HEAD_REQUESTS_PER_SECOND) or RESTORE_POLL_HEAD_REQUESTS_PER_SECOND))
    concurrency = max(1, int(getattr(connection, "restore_poll_concurrency", RESTORE_POLL_HEAD_CONCURRENCY) or RESTORE_POLL_HEAD_CONCURRENCY))
    # Plain immutable values remain safe after a progress callback commits the
    # SQLAlchemy session and expires ORM instances.
    targets = [(obj.id, obj.object_key, obj.version_id) for obj in archives]

    def check(target: tuple[int, str, str | None]) -> tuple[int, datetime | None] | None:
        object_id, object_key, version_id = target
        arguments = {"Bucket": bucket_name, "Key": object_key}
        if version_id:
            arguments["VersionId"] = version_id
        response = s3.head_object(**arguments)
        if restored_from_head_response(response):
            return object_id, restore_expiry_from_head_response(response)
        return None

    try:
        for start in range(0, len(targets), RESTORE_POLL_HEAD_BATCH_SIZE):
            batch = targets[start:start + RESTORE_POLL_HEAD_BATCH_SIZE]
            for second_start in range(0, len(batch), requests_per_second):
                second_batch = batch[second_start:second_start + requests_per_second]
                second_started_at = time.monotonic()
                with ThreadPoolExecutor(max_workers=min(concurrency, len(second_batch)),
                                        thread_name_prefix="s3-oci-restore-poll") as executor:
                    futures = [executor.submit(check, obj) for obj in second_batch]
                    for future in as_completed(futures):
                        metrics["requests"] = int(metrics["requests"]) + 1
                        try:
                            result = future.result()
                        except ClientError as error:
                            code = str((error.response or {}).get("Error", {}).get("Code", ""))
                            if code in TRANSIENT_AWS_CODES:
                                metrics["throttle_retries"] = int(metrics["throttle_retries"]) + 1
                            raise
                        if result:
                            ready[result[0]] = result[1]
                # This is an explicit start-rate ceiling in addition to SDK
                # retries and concurrency. It protects S3 and makes API cost
                # and load predictable for large waves.
                has_next = second_start + requests_per_second < len(batch) or start + len(batch) < len(targets)
                remaining = 1.0 - (time.monotonic() - second_started_at)
                if has_next and remaining > 0:
                    time.sleep(remaining)
            metrics["elapsed_seconds"] = round(time.monotonic() - started_at, 3)
            if progress_callback:
                progress_callback(dict(metrics), dict(ready))
    except Exception as error:
        metrics["elapsed_seconds"] = round(time.monotonic() - started_at, 3)
        setattr(error, "raijin_restore_poll_metrics", metrics)
        raise
    metrics["elapsed_seconds"] = round(time.monotonic() - started_at, 3)
    return ready, metrics


def persist_restore_poll_metrics(wave: Wave, metrics: dict[str, float | int]) -> None:
    """Persist request/load evidence regardless of a successful poll outcome."""
    requests = int(metrics.get("requests", 0) or 0)
    elapsed = float(metrics.get("elapsed_seconds", 0) or 0)
    throttles = int(metrics.get("throttle_retries", 0) or 0)
    wave.availability_head_requests = int(wave.availability_head_requests or 0) + requests
    wave.availability_poll_elapsed_seconds = float(wave.availability_poll_elapsed_seconds or 0) + elapsed
    wave.availability_throttle_retries = int(wave.availability_throttle_retries or 0) + throttles
    wave.last_availability_poll_objects, wave.last_availability_poll_seconds = requests, elapsed


def archive_objects(session, wave_id: int) -> list[ObjectRecord]:
    return list(session.scalars(select(ObjectRecord).where(ObjectRecord.wave_id == wave_id, ObjectRecord.storage_class.in_(ARCHIVE_CLASSES), ObjectRecord.state == ObjectState.WAVE_ASSIGNED).order_by(ObjectRecord.object_key)))


def batch_manifest_fields(has_versions: bool) -> list[str]:
    """Field names mandated by the S3 Batch Operations CSV manifest format."""
    return ["Bucket", "Key"] + (["VersionId"] if has_versions else [])


def reported_bucket_region(client, bucket: str) -> str:
    """Read HeadBucket's region header, including redirects from a wrong endpoint."""
    try:
        response = client.head_bucket(Bucket=bucket)
        headers = response.get("ResponseMetadata", {}).get("HTTPHeaders", {})
    except Exception as error:
        headers = (getattr(error, "response", {}) or {}).get("ResponseMetadata", {}).get("HTTPHeaders", {})
        if not headers.get("x-amz-bucket-region"):
            raise
    region = headers.get("x-amz-bucket-region")
    if not region:
        raise RuntimeError(f"AWS did not return a region for bucket '{bucket}'")
    return "eu-west-1" if region == "EU" else region


def validate_restore_preflight(session, source: Source, s3, operation: dict[str, str]) -> None:
    """Block a charged Batch job when region or control-bucket topology is invalid."""
    source_region = reported_bucket_region(s3, source.s3_bucket)
    control_region = reported_bucket_region(s3, operation["control_bucket"])
    source.aws_bucket_region = source_region
    if source.aws_region != source_region:
        raise RuntimeError(f"Source AWS region mismatch: configured {source.aws_region}, bucket is {source_region}")
    if control_region != source_region:
        raise RuntimeError(f"Control bucket region mismatch: {control_region}; restore source bucket is {source_region}")


def restore_attempt_for_job(session, wave: Wave, source: Source) -> RestoreAttempt:
    attempt = session.scalar(select(RestoreAttempt).where(RestoreAttempt.job_id == wave.batch_job_id)) if wave.batch_job_id else None
    if attempt:
        return attempt
    attempt = RestoreAttempt(wave_id=wave.id, aws_region=source.aws_region, job_id=wave.batch_job_id,
                             job_status=wave.batch_job_status, manifest_key=wave.manifest_key,
                             manifest_etag=wave.manifest_etag,
                             expected_objects=session.scalar(select(func.count(ObjectRecord.id)).where(ObjectRecord.wave_id == wave.id)) or 0,
                             failure_summary="Legacy restore attempt observed after platform upgrade")
    session.add(attempt)
    session.flush()
    return attempt


def import_completion_report(session, s3, operation: dict[str, str], attempt: RestoreAttempt, objects: list[ObjectRecord]) -> tuple[bool, dict[str, int]]:
    """Import per-object S3 Batch evidence. False means AWS has not published it yet."""
    prefix = f"{operation['control_prefix'].rstrip('/')}/reports/wave-{attempt.wave_id}/attempt-{attempt.id}/"
    metrics = {"list_requests": 0, "get_requests": 0}
    keys: list[str] = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=operation["control_bucket"], Prefix=prefix):
        metrics["list_requests"] += 1
        keys.extend(entry["Key"] for entry in page.get("Contents", []))
    manifest_key = next((key for key in keys if key.endswith("manifest.json")), None)
    if not manifest_key:
        return False, metrics
    metrics["get_requests"] += 1
    manifest_response = s3.get_object(Bucket=operation["control_bucket"], Key=manifest_key)
    manifest_body = manifest_response["Body"].read().decode("utf-8")
    manifest = json.loads(manifest_body)
    attempt.report_manifest_key = manifest_key
    attempt.report_manifest_etag = manifest_response.get("ETag", "").strip('"') or None
    objects_by_key = {(obj.object_key, obj.version_id or ""): obj for obj in objects}
    for result in manifest.get("Results", []):
        report_key = result.get("Key")
        if not report_key:
            continue
        metrics["get_requests"] += 1
        report = s3.get_object(Bucket=result.get("Bucket", operation["control_bucket"]), Key=report_key)
        report_etag = report.get("ETag", "").strip('"') or None
        for row in csv.reader(io.StringIO(report["Body"].read().decode("utf-8", "replace"))):
            if len(row) < 7:
                continue
            # AWS currently emits the actionable values after TaskStatus as
            # HTTP status then service error code (for example
            # ``failed,409,RestoreAlreadyInProgress``). Preserve the observed
            # wire format: it is the evidence operators need in the report.
            _, key, version_id, task_status, http_status, error_code, error_message = (row + [""] * 7)[:7]
            obj = objects_by_key.get((unquote(key), version_id or "")) or objects_by_key.get((key, version_id or ""))
            if not obj:
                continue
            outcome = session.scalar(select(RestoreObjectResult).where(RestoreObjectResult.attempt_id == attempt.id, RestoreObjectResult.object_id == obj.id))
            if not outcome:
                outcome = RestoreObjectResult(attempt_id=attempt.id, object_id=obj.id)
                session.add(outcome)
            outcome.task_status, outcome.http_status = task_status.upper(), int(http_status) if http_status.isdigit() else None
            outcome.error_code, outcome.error_message, outcome.report_key, outcome.report_etag = error_code or None, error_message or None, report_key, report_etag
    return True, metrics


def fail_restore_attempt(session, task: Task, wave: Wave, attempt: RestoreAttempt, message: str) -> None:
    attempt.failure_summary, attempt.completed_at = message[:8000], utcnow()
    wave.status, wave.batch_job_status = "RESTORE_REQUEST_FAILED", attempt.job_status
    event(session, "RESTORE_REQUEST_FAILED", message[:8000], source_id=wave.source_id, wave_id=wave.id)
    fail_permanently(session, task, message)


def submit_restore(session, task: Task, settings) -> None:
    wave = session.get(Wave, task.wave_id)
    source = wave.source
    archives = archive_objects(session, wave.id)
    if not archives:
        for obj in session.scalars(select(ObjectRecord).where(ObjectRecord.wave_id == wave.id, ObjectRecord.state == ObjectState.WAVE_ASSIGNED)):
            obj.state, obj.restored_at = ObjectState.RESTORED, utcnow()
        wave.status = "RESTORED"
        enqueue_available_transfer_objects(session, wave)
        ensure_transfer_task(session, wave, settings)
        succeed(session, task)
        return
    if runtime_context.is_simulation:
        submit_restore_simulated(session, task, wave, source, archives)
        return
    operation = aws_operation_config(source, settings)
    if not operation["control_bucket"] or not operation["batch_role_arn"]:
        raise RuntimeError("AWS control bucket and Batch Operations role ARN must be configured")
    try:
        s3, s3control, account_id = aws_clients(settings, source.aws_region, source)
        validate_restore_preflight(session, source, s3, operation)
    except Exception as error:
        _, summary = classify_task_error(error)
        raise RuntimeError(f"Restore preflight failed: {summary}") from error
    attempt = RestoreAttempt(wave_id=wave.id, aws_region=source.aws_region, expected_objects=len(archives))
    session.add(attempt)
    session.flush()
    has_versions = any(obj.version_id for obj in archives)
    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    for obj in archives:
        row = [source.s3_bucket, quote(obj.object_key, safe="/")]
        if has_versions:
            row.append(obj.version_id or "")
        writer.writerow(row)
    manifest_key = f"{operation['control_prefix'].rstrip('/')}/manifests/wave-{wave.id}/attempt-{attempt.id}.csv"
    try:
        response = s3.put_object(Bucket=operation["control_bucket"], Key=manifest_key, Body=output.getvalue().encode("utf-8"), ContentType="text/csv")
    except Exception as error:
        _, summary = classify_task_error(error)
        raise RuntimeError(f"Control-bucket manifest upload failed: {summary}") from error
    fields = batch_manifest_fields(has_versions)
    try:
        job = s3control.create_job(
            AccountId=account_id, ConfirmationRequired=False, Priority=10, RoleArn=operation["batch_role_arn"],
            Operation={"S3InitiateRestoreObject": {"ExpirationInDays": wave.restore_days, "GlacierJobTier": wave.restore_tier}},
            Manifest={"Spec": {"Format": "S3BatchOperations_CSV_20180820", "Fields": fields}, "Location": {"ObjectArn": f"arn:aws:s3:::{operation['control_bucket']}/{manifest_key}", "ETag": response["ETag"].strip('"')}},
            Report={"Bucket": f"arn:aws:s3:::{operation['control_bucket']}", "Prefix": f"{operation['control_prefix'].rstrip('/')}/reports/wave-{wave.id}/attempt-{attempt.id}/", "Format": "Report_CSV_20180820", "Enabled": True, "ReportScope": "AllTasks"},
            Description=f"S3 to OCI restore wave {wave.id}",
            # A retry of this durable task keeps the same token, while an
            # operator-approved reprocess creates a new task and therefore a
            # new Batch submission. Reusing only the wave id would make AWS
            # reject a legitimately new manifest as an idempotency conflict.
            ClientRequestToken=f"odmt-wave-{wave.id}-task-{task.id}",
        )
    except Exception as error:
        _, summary = classify_task_error(error)
        raise RuntimeError(f"S3 Batch Operations job creation failed: {summary}") from error
    attempt.job_id, attempt.job_status, attempt.manifest_key, attempt.manifest_etag = job["JobId"], "Preparing", manifest_key, response["ETag"].strip('"')
    wave.batch_job_id, wave.batch_job_status, wave.manifest_key, wave.manifest_etag, wave.status = job["JobId"], "Preparing", manifest_key, response["ETag"].strip('"'), "RESTORE_REQUESTED"
    for obj in archives:
        obj.state, obj.restore_requested_at, obj.restore_attempt_id = ObjectState.RESTORE_REQUESTED, utcnow(), attempt.id
        session.add(RestoreObjectResult(attempt_id=attempt.id, object_id=obj.id))
    event(session, "BATCH_RESTORE_SUBMITTED", f"Batch job {wave.batch_job_id} submitted for {len(archives)} archive object(s); awaiting per-object acceptance evidence", source_id=source.id, wave_id=wave.id)
    succeed(session, task, "POLL_RESTORE")


def simulated_manifest_line(obj: ObjectRecord) -> bytes:
    return (
        json.dumps(
            {"key": obj.object_key, "version_id": obj.version_id},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )


def submit_restore_simulated(
    session, task: Task, wave: Wave, source: Source, archives: list[ObjectRecord]
) -> None:
    context = simulation_execution_context(source)
    port = SimulatedSourcePort(runtime_context.simulator_base_url)
    attempt = RestoreAttempt(
        wave_id=wave.id,
        aws_region=source.aws_region,
        expected_objects=len(archives),
    )
    session.add(attempt)
    session.flush()
    manifest_sha = hashlib.sha256()
    for obj in archives:
        manifest_sha.update(simulated_manifest_line(obj))
    # Idempotency belongs to a durable submission task, not to the wave
    # forever.  Retrying this task remains safe, while a later explicit
    # reprocess has a distinct task id and may submit its new manifest.
    idempotency_key = uuid5(
        NAMESPACE_URL, f"odmt:simulation:restore:wave:{wave.id}:task:{task.id}"
    )
    response = port.submit_restore_batch(
        SubmitRestoreBatchRequest(
            context=context,
            bucket=source.s3_bucket,
            tier=wave.restore_tier,
            retention_days=wave.restore_days,
            object_count=len(archives),
            manifest_sha256=manifest_sha.hexdigest(),
            idempotency_key=idempotency_key,
        ),
        (simulated_manifest_line(obj) for obj in archives),
    )
    manifest_key = f"simulated://restore-manifests/wave-{wave.id}/attempt-{attempt.id}"
    attempt.job_id = response.job_id
    attempt.job_status = response.status
    attempt.manifest_key = manifest_key
    attempt.manifest_etag = manifest_sha.hexdigest()
    wave.batch_job_id = response.job_id
    wave.batch_job_status = response.status
    wave.manifest_key = manifest_key
    wave.manifest_etag = manifest_sha.hexdigest()
    wave.status = "RESTORE_REQUESTED"
    wave.restore_requested_virtual_at = wave.restore_requested_virtual_at or simulation_virtual_now(source)
    requested_at = utcnow()
    for obj in archives:
        obj.state = ObjectState.RESTORE_REQUESTED
        obj.restore_requested_at = requested_at
        obj.restore_attempt_id = attempt.id
        session.add(RestoreObjectResult(attempt_id=attempt.id, object_id=obj.id))
    event(
        session,
        "SIMULATED_BATCH_RESTORE_SUBMITTED",
        f"SIMULATED Batch job {response.job_id} accepted manifest with {len(archives)} object(s)",
        source_id=source.id,
        wave_id=wave.id,
    )
    succeed(session, task, "POLL_RESTORE")


def poll_restore_simulated(
    session, task: Task, settings, wave: Wave, source: Source
) -> None:
    if restore_reapproval_is_required(wave):
        finish_superseded_restore_poll(session, task, wave)
        return
    context = simulation_execution_context(source)
    port = SimulatedSourcePort(runtime_context.simulator_base_url)
    attempt = restore_attempt_for_job(session, wave, source)
    objects = list(
        session.scalars(select(ObjectRecord).where(ObjectRecord.wave_id == wave.id))
    )
    if attempt.completed_at is None:
        by_key = {(obj.object_key, obj.version_id or ""): obj for obj in objects}
        token = None
        status = "PROCESSING"
        accepted = failed = 0
        while True:
            page = port.describe_restore_batch(
                DescribeRestoreBatchRequest(
                    context=context,
                    job_id=wave.batch_job_id,
                    continuation_token=token,
                    max_results=1000,
                )
            )
            status = page.status
            accepted, failed = page.accepted_count, page.failed_count
            for result in page.results:
                obj = by_key.get((result.key, result.version_id or ""))
                if obj is None:
                    continue
                outcome = session.scalar(
                    select(RestoreObjectResult).where(
                        RestoreObjectResult.attempt_id == attempt.id,
                        RestoreObjectResult.object_id == obj.id,
                    )
                )
                if outcome is None:
                    outcome = RestoreObjectResult(attempt_id=attempt.id, object_id=obj.id)
                    session.add(outcome)
                outcome.task_status = "SUCCEEDED" if result.accepted else "FAILED"
                outcome.http_status = 200 if result.accepted else 409
                outcome.error_code = result.error_code
                outcome.error_message = result.error_message
                outcome.report_key = f"simulated://restore-jobs/{page.job_id}/results"
            token = page.next_continuation_token
            if not token:
                break
        attempt.batch_describe_requests = int(attempt.batch_describe_requests or 0) + 1
        attempt.job_status = wave.batch_job_status = status
        attempt.succeeded_objects = accepted
        attempt.failed_objects = failed
        if status != "COMPLETE":
            if status == "FAILED":
                fail_restore_attempt(
                    session, task, wave, attempt, f"SIMULATED Batch job {wave.batch_job_id} failed"
                )
                return
            wave.status = "RESTORING"
            session.commit()
            retry(session, task, f"SIMULATED Batch job status is {status}", 60)
            return
        if failed:
            attempt.report_manifest_key = f"simulated://restore-jobs/{wave.batch_job_id}/results"
            fail_restore_attempt(
                session,
                task,
                wave,
                attempt,
                f"SIMULATED Batch restore rejected {failed} of {len(objects)} object(s)",
            )
            return
        attempt.report_manifest_key = f"simulated://restore-jobs/{wave.batch_job_id}/results"
        attempt.completed_at = utcnow()
        attempt.failure_summary = None
        for obj in objects:
            if obj.storage_class in ARCHIVE_CLASSES and obj.state in {
                ObjectState.RESTORE_REQUESTED,
                ObjectState.RESTORING,
            }:
                obj.state = ObjectState.RESTORE_REQUEST_ACCEPTED
        wave.status = "RESTORE_REQUEST_ACCEPTED"
        event(
            session,
            "SIMULATED_RESTORE_REQUEST_ACCEPTED",
            f"SIMULATED Batch job {wave.batch_job_id}: all {len(objects)} object(s) have individual acceptance evidence",
            source_id=source.id,
            wave_id=wave.id,
        )
        session.commit()

    pending_archives = [
        obj
        for obj in objects
        if obj.storage_class in ARCHIVE_CLASSES
        and obj.state
        in {
            ObjectState.RESTORE_REQUESTED,
            ObjectState.RESTORE_REQUEST_ACCEPTED,
            ObjectState.RESTORING,
        }
    ]
    ready_expiries: dict[int, datetime | None] = {}
    recommended_real_delays: list[float] = []
    started = time.monotonic()
    for index, obj in enumerate(pending_archives, start=1):
        result = port.head_object(
            HeadObjectRequest(
                context=context,
                object=ObjectIdentity(
                    bucket=source.s3_bucket,
                    key=obj.object_key,
                    version_id=obj.version_id,
                ),
            )
        )
        if result.exists and not result.restore_in_progress and result.restore_expires_at:
            ready_expiries[obj.id] = result.restore_expires_at
        elif result.simulator_recommended_real_poll_seconds is not None:
            recommended_real_delays.append(result.simulator_recommended_real_poll_seconds)
        if index % RESTORE_POLL_HEAD_BATCH_SIZE == 0:
            task.lease_expires_at = utcnow() + timedelta(seconds=settings.task_lease_seconds)
            session.commit()
    poll_elapsed = time.monotonic() - started
    # A transfer worker can discover a genuine expiry while this polling
    # worker is collecting per-object evidence. Refresh the wave before any
    # status mutation so this stale session cannot resurrect it as RESTORING.
    session.refresh(wave)
    if restore_reapproval_is_required(wave):
        finish_superseded_restore_poll(session, task, wave)
        return
    persist_restore_poll_metrics(
        wave,
        {
            "requests": len(pending_archives),
            "throttle_retries": 0,
            "elapsed_seconds": poll_elapsed,
        },
    )
    wave.last_availability_poll_objects = len(pending_archives)
    wave.last_availability_poll_seconds = poll_elapsed
    availability_observed_at = utcnow()
    availability_virtual_at = simulation_virtual_now(source)
    for obj in objects:
        if obj.storage_class not in ARCHIVE_CLASSES or obj.id in ready_expiries:
            if obj.state in {
                ObjectState.RESTORE_REQUESTED,
                ObjectState.RESTORE_REQUEST_ACCEPTED,
                ObjectState.RESTORING,
                ObjectState.WAVE_ASSIGNED,
            }:
                obj.state = ObjectState.RESTORED
                obj.restored_at = obj.restored_at or availability_observed_at
                if obj.storage_class in ARCHIVE_CLASSES:
                    obj.restore_expires_at = ready_expiries.get(obj.id)
        elif obj.state in {ObjectState.RESTORE_REQUESTED, ObjectState.RESTORE_REQUEST_ACCEPTED}:
            obj.state = ObjectState.RESTORING
    pending = int(
        session.scalar(
            select(func.count(ObjectRecord.id)).where(
                ObjectRecord.wave_id == wave.id,
                ObjectRecord.state.in_(
                    [
                        ObjectState.RESTORE_REQUESTED,
                        ObjectState.RESTORE_REQUEST_ACCEPTED,
                        ObjectState.RESTORING,
                    ]
                ),
            )
        )
        or 0
    )
    wave.last_poll_at, wave.poll_count = utcnow(), wave.poll_count + 1
    if pending:
        # An early-release source can be polling remaining objects while a
        # transfer batch owns the same wave. Do not overwrite the primary
        # visible TRANSFERRING state during that overlap.
        if not simulation_transfer_task_active(session, wave):
            wave.status = "RESTORING"
        ready_for_transfer = int(
            session.scalar(
                select(func.count(ObjectRecord.id)).where(
                    ObjectRecord.wave_id == wave.id,
                    ObjectRecord.state == ObjectState.RESTORED,
                )
            )
            or 0
        )
        if ready_for_transfer:
            wave.first_restore_available_virtual_at = (
                wave.first_restore_available_virtual_at or availability_virtual_at
            )
        released = (
            enqueue_available_transfer_objects(session, wave, availability_virtual_at)
            if wave.transfer_release_policy == "AS_OBJECTS_AVAILABLE" else 0
        )
        if released:
            ensure_transfer_task(session, wave, settings)
            event(
                session,
                "SIMULATED_RESTORE_OBJECTS_RELEASED",
                f"SIMULATED: {released} object(s) released immediately to the continuous transfer lane; {pending} remain unavailable.",
                source_id=source.id,
                wave_id=wave.id,
            )
        session.commit()
        delay = restore_availability_poll_delay_seconds(
            attempt.completed_at,
            utcnow(),
            wave.restore_tier,
            partial_availability=bool(ready_for_transfer),
            transfer_strategy=wave.transfer_release_policy,
            pending_objects=pending,
        )
        # The polling policy is expressed in operational (virtual) time. The
        # simulator clock is paused between durable decisions, so move it by
        # one bounded polling interval rather than letting a 3,600x wall-clock
        # race consume days of retention while the worker performs HeadObject
        # calls.  Never advance it while a Raiju transfer hold is active: that
        # would consume a temporary restore window while actual copy work is
        # still running.
        if wave.simulation_transfer_clock_held or simulation_transfer_task_active(session, wave):
            retry(
                session,
                task,
                "SIMULATED: availability polling deferred while transfer retains the virtual restore window",
                1,
            )
            return
        virtual_delay = delay
        try:
            clock = SimulatorAdminClient(runtime_context.simulator_base_url).clock_status(
                source.simulation_execution_id
            )
            acceleration = max(1.0, float(clock.get("acceleration") or 1.0))
            if recommended_real_delays:
                virtual_delay = min(
                    virtual_delay,
                    max(1, math.ceil(min(recommended_real_delays) * acceleration)),
                )
        except Exception:
            # Retain the conservative policy interval if the status endpoint
            # has a transient issue; no free-running fallback is allowed.
            virtual_delay = delay
        advance_simulation_clock(
            session,
            source,
            virtual_delay,
            "restore availability polling interval",
            wave,
        )
        # The next durable poll is intentionally soon in real time; virtual
        # time has already advanced by the controlled interval above.
        delay = 1
        delay_label = f"{virtual_delay}s virtual" if virtual_delay < 60 else f"{math.ceil(virtual_delay / 60)} virtual minute(s)"
        retry(
            session,
            task,
            f"SIMULATED: {pending} object(s) still unavailable; next poll in {delay_label}",
            delay,
        )
        return
    wave.status = "RESTORED"
    wave.first_restore_available_virtual_at = wave.first_restore_available_virtual_at or availability_virtual_at
    wave.last_restore_available_virtual_at = availability_virtual_at
    event(
        session,
        "SIMULATED_RESTORE_AVAILABLE",
        "SIMULATED: all wave objects are available for transfer",
        source_id=source.id,
        wave_id=wave.id,
    )
    # Keep a second hold beyond the polling context.  It is released only by
    # the transfer worker, preventing a 3,600x virtual clock from expiring a
    # freshly available object in the durable-queue hand-off.
    enqueue_available_transfer_objects(session, wave, availability_virtual_at)
    ensure_transfer_task(session, wave, settings)
    succeed(session, task)


def poll_restore(session, task: Task, settings) -> None:
    wave, source = session.get(Wave, task.wave_id), session.get(Wave, task.wave_id).source
    if restore_reapproval_is_required(wave):
        finish_superseded_restore_poll(session, task, wave)
        return
    if runtime_context.is_simulation:
        with simulation_phase_clock_hold(source, "restore availability polling"):
            poll_restore_simulated(session, task, settings, wave, source)
        return
    # Keep the control-bucket layout in sync with submission.  The completion
    # report lives under the source-specific Raijin prefix, not the legacy
    # s3-oci-control prefix.
    operation = aws_operation_config(source, settings)
    s3, s3control, account_id = aws_clients(settings, source.aws_region, source)
    if wave.batch_job_id:
        attempt = restore_attempt_for_job(session, wave, source)
        # The Batch completion report is immutable evidence.  Once imported,
        # do not call DescribeJob or re-list/re-read its report on every
        # availability poll.  That was the source of repeated events and
        # avoidable charged control-bucket requests in the first archive test.
        if attempt.completed_at is None or attempt.report_manifest_key is None:
            attempt.batch_describe_requests = int(attempt.batch_describe_requests or 0) + 1
            job = s3control.describe_job(AccountId=account_id, JobId=wave.batch_job_id)["Job"]
            attempt.job_status = wave.batch_job_status = job.get("Status")
            if job["Status"] not in {"Complete", "Completed"}:
                if job["Status"] in {"Failed", "Cancelled", "Canceled"}:
                    fail_restore_attempt(session, task, wave, attempt, f"Batch job {wave.batch_job_id} ended as {job['Status']}")
                    return
                wave.status = "RESTORING"
                wave.last_poll_at, wave.poll_count = utcnow(), wave.poll_count + 1
                session.commit()
                retry(session, task, f"Batch job status is {job['Status']}", min(1800, 300 + wave.poll_count * 60))
                return
            progress = job.get("ProgressSummary", {}) or {}
            attempt.succeeded_objects = int(progress.get("NumberOfTasksSucceeded") or 0)
            attempt.failed_objects = int(progress.get("NumberOfTasksFailed") or 0)
            objects = list(session.scalars(select(ObjectRecord).where(ObjectRecord.wave_id == wave.id)))
            try:
                report_available, report_metrics = import_completion_report(session, s3, operation, attempt, objects)
                attempt.completion_report_list_requests = int(attempt.completion_report_list_requests or 0) + report_metrics["list_requests"]
                attempt.completion_report_get_requests = int(attempt.completion_report_get_requests or 0) + report_metrics["get_requests"]
            except Exception as error:
                _, summary = classify_task_error(error)
                raise RuntimeError(f"S3 Batch completion-report processing failed: {summary}") from error
            if not report_available:
                wave.status = "RESTORE_REQUESTED"
                wave.last_poll_at, wave.poll_count = utcnow(), wave.poll_count + 1
                session.commit()
                retry(session, task, "Batch job is complete; waiting for per-object completion report evidence", 60)
                return
            diagnosis = restore_result_diagnostics(session, attempt.id)
            if diagnosis["action_required"]:
                reason = diagnosis["reasons"][0] if diagnosis["reasons"] else {"code": "UNKNOWN", "count": diagnosis["unexpected_failed"]}
                message = (f"S3 Batch restore requires operator action: {reason['count']} object(s) reported "
                           f"{reason['code']}; {diagnosis['effective_accepted']}/{attempt.expected_objects} "
                           "requests are accepted or already in progress")
                fail_restore_attempt(session, task, wave, attempt, message)
                return
            attempt.completed_at, attempt.failure_summary = utcnow(), None
            for obj in objects:
                if obj.storage_class in ARCHIVE_CLASSES and obj.state in {ObjectState.RESTORE_REQUESTED, ObjectState.RESTORING}:
                    obj.state = ObjectState.RESTORE_REQUEST_ACCEPTED
            wave.status = "RESTORE_REQUEST_ACCEPTED"
            event(session, "RESTORE_REQUEST_ACCEPTED",
                  f"Batch job {wave.batch_job_id}: {diagnosis['raw_succeeded']} succeeded and "
                  f"{diagnosis['accepted_equivalent']} already in progress; all {attempt.expected_objects} "
                  "archive objects accepted with per-object evidence",
                  source_id=source.id, wave_id=wave.id)
            session.commit()
    objects = list(session.scalars(select(ObjectRecord).where(ObjectRecord.wave_id == wave.id)))
    pending_archives = [obj for obj in objects if obj.storage_class in ARCHIVE_CLASSES and obj.state in {
        ObjectState.RESTORE_REQUESTED, ObjectState.RESTORE_REQUEST_ACCEPTED, ObjectState.RESTORING,
    }]
    # The availability/expiry values are collected from the same targeted
    # polling response needed to decide readiness. No inferred restore time is
    # stored: restored_at remains the Raijin observation timestamp only.
    poll_method = "HeadObject (pending wave objects)"
    checkpointed = {"requests": 0, "throttle_retries": 0, "elapsed_seconds": 0.0}

    def persist_poll_progress(metrics: dict[str, float | int], ready: dict[int, datetime | None]) -> None:
        """Checkpoint a bounded polling slice without double-counting it."""
        delta = {
            "requests": int(metrics.get("requests", 0) or 0) - int(checkpointed["requests"]),
            "throttle_retries": int(metrics.get("throttle_retries", 0) or 0) - int(checkpointed["throttle_retries"]),
            "elapsed_seconds": float(metrics.get("elapsed_seconds", 0) or 0) - float(checkpointed["elapsed_seconds"]),
        }
        if any(value > 0 for value in delta.values()):
            persist_restore_poll_metrics(wave, delta)
        wave.last_availability_poll_objects = int(metrics.get("requests", 0) or 0)
        wave.last_availability_poll_seconds = float(metrics.get("elapsed_seconds", 0) or 0)
        if ready:
            observed_at = utcnow()
            for restored_object in session.scalars(select(ObjectRecord).where(
                ObjectRecord.id.in_(list(ready)),
                ObjectRecord.wave_id == wave.id,
            )):
                if restored_object.state in {
                    ObjectState.RESTORE_REQUESTED, ObjectState.RESTORE_REQUEST_ACCEPTED, ObjectState.RESTORING,
                }:
                    restored_object.state = ObjectState.RESTORED
                    restored_object.restored_at = restored_object.restored_at or observed_at
                    restored_object.restore_expires_at = ready.get(restored_object.id)
        checkpointed.update(metrics)
        session.commit()

    try:
        with task_lease_heartbeat(task.id, settings.task_lease_seconds):
            ready_expiries, poll_metrics = restored_pending_archives_from_head(
                s3, source, pending_archives, progress_callback=persist_poll_progress
            )
        persist_poll_progress(poll_metrics, ready_expiries)
    except Exception as error:
        persist_poll_progress(getattr(error, "raijin_restore_poll_metrics", {}), {})
        wave.last_poll_at, wave.poll_count = utcnow(), wave.poll_count + 1
        session.commit()
        raise
    # Progress commits expire ORM instances. Refresh the wave inventory in one
    # query before final reconciliation instead of triggering one lazy SELECT
    # per object on large waves.
    objects = list(session.scalars(select(ObjectRecord).where(ObjectRecord.wave_id == wave.id)))
    availability_observed_at = utcnow()
    for obj in objects:
        if obj.storage_class not in ARCHIVE_CLASSES or obj.id in ready_expiries:
            if obj.state in {ObjectState.RESTORE_REQUESTED, ObjectState.RESTORE_REQUEST_ACCEPTED, ObjectState.RESTORING, ObjectState.WAVE_ASSIGNED}:
                obj.state, obj.restored_at = ObjectState.RESTORED, obj.restored_at or availability_observed_at
                if obj.storage_class in ARCHIVE_CLASSES:
                    obj.restore_expires_at = ready_expiries.get(obj.id)
        elif obj.state in {ObjectState.RESTORE_REQUESTED, ObjectState.RESTORE_REQUEST_ACCEPTED}:
            obj.state = ObjectState.RESTORING
    pending = session.scalar(select(func.count(ObjectRecord.id)).where(ObjectRecord.wave_id == wave.id, ObjectRecord.state.in_([ObjectState.RESTORE_REQUESTED, ObjectState.RESTORE_REQUEST_ACCEPTED, ObjectState.RESTORING]))) or 0
    wave.last_poll_at, wave.poll_count = utcnow(), wave.poll_count + 1
    if pending:
        wave.status = "RESTORING"
        ready_for_transfer = session.scalar(select(func.count(ObjectRecord.id)).where(
            ObjectRecord.wave_id == wave.id,
            ObjectRecord.state == ObjectState.RESTORED,
        )) or 0
        released = (
            enqueue_available_transfer_objects(session, wave, availability_observed_at)
            if wave.transfer_release_policy == "AS_OBJECTS_AVAILABLE" else 0
        )
        if released:
            ensure_transfer_task(session, wave, settings)
            event(session, "RESTORE_OBJECTS_RELEASED", f"{released} object(s) released immediately to the continuous transfer lane while {pending} remain unavailable", source_id=source.id, wave_id=wave.id)
        session.commit()
        delay = restore_availability_poll_delay_seconds(
            attempt.completed_at if wave.batch_job_id else None,
            utcnow(),
            wave.restore_tier,
            partial_availability=bool(ready_for_transfer),
            transfer_strategy=wave.transfer_release_policy,
            pending_objects=int(pending),
        )
        retry(session, task, f"{pending} object(s) still unavailable after restore; checked with {poll_method}; next availability poll in {delay // 60} minutes", delay)
        return
    wave.status = "RESTORED"
    expiries = [obj.restore_expires_at for obj in objects if obj.restore_expires_at]
    expiry_text = f"; earliest temporary-copy expiry {min(expiries).isoformat()}" if expiries else ""
    event(session, "RESTORE_AVAILABLE", f"All wave objects are available for transfer; readiness checked with {poll_method}{expiry_text}", source_id=source.id, wave_id=wave.id)
    enqueue_available_transfer_objects(session, wave, availability_observed_at)
    ensure_transfer_task(session, wave, settings)
    succeed(session, task)


DIRECT_SHA_LIMIT = 16 * 1024 * 1024
DEFAULT_MULTIPART_PART_SIZE = 64 * 1024 * 1024
MAX_MULTIPART_PARTS = 10_000
COPY_CHUNK_SIZE = 8 * 1024 * 1024


def sha256_b64(data: bytes) -> str:
    return base64.b64encode(hashlib.sha256(data).digest()).decode("ascii")


def expected_part_size(total_size: int, part_number: int, part_size: int) -> int:
    """Return the exact size of a 1-based OCI multipart part."""
    start = (part_number - 1) * part_size
    return max(0, min(part_size, total_size - start))


def multipart_parts_on_oci(client, namespace: str, bucket: str, key: str, upload_id: str) -> dict[int, dict]:
    """List already accepted parts, including pagination, without reading data."""
    result: dict[int, dict] = {}
    page = None
    while True:
        kwargs = {"limit": 1000}
        if page:
            kwargs["page"] = page
        response = client.list_multipart_upload_parts(namespace, bucket, key, upload_id, **kwargs)
        # The OCI SDK returns a bare list for this operation (unlike several
        # list APIs that wrap rows in a `.parts` attribute).
        for part in getattr(response.data, "parts", response.data):
            part_number = getattr(part, "part_number", getattr(part, "part_num", None))
            result[int(part_number)] = {"etag": part.etag, "size": int(part.size)}
        page = getattr(response, "headers", {}).get("opc-next-page")
        if not page:
            return result


def reusable_multipart_part(remote: dict | None, evidence: dict, expected_size: int) -> bool:
    """A part is reusable only when OCI and local SHA evidence agree it exists."""
    return bool(remote and remote.get("size") == expected_size and evidence.get("sha256"))


def effective_multipart_part_size(total_size: int, configured_size: int) -> int:
    """Honor the configured size while keeping OCI's 10,000-part limit."""
    required_for_limit = (total_size + MAX_MULTIPART_PARTS - 1) // MAX_MULTIPART_PARTS
    return max(configured_size, required_for_limit, DIRECT_SHA_LIMIT)


def multipart_audit_matches(part_evidence: dict, destination_digests: list[bytes]) -> bool:
    """Compare OCI reread part digests with the durable source-part evidence."""
    if len(part_evidence) != len(destination_digests):
        return False
    for number, digest in enumerate(destination_digests, start=1):
        if part_evidence.get(str(number), {}).get("sha256") != base64.b64encode(digest).decode("ascii"):
            return False
    return True


def transfer_object(s3, namespace: str, source_bucket: str, destination_bucket: str,
                    object_id: int, rate_bytes_per_second: float, preserve_s3_tags: bool,
                    configured_multipart_part_size: int = DEFAULT_MULTIPART_PART_SIZE) -> None:
    """Copy one object using an independent database session.

    A SQLAlchemy session is never shared between file workers. AWS credentials
    are assumed once per wave, while the OCI client is short lived per object
    worker to keep its HTTP state isolated too.
    """
    with SessionLocal() as worker_session:
        obj = worker_session.get(ObjectRecord, object_id)
        if not obj or obj.state not in {ObjectState.RESTORED, ObjectState.TRANSFERRING}:
            return
        # Keep an accumulated active-copy duration.  A stopped worker may
        # resume this object later; downtime is intentionally not counted.
        if obj.state != ObjectState.TRANSFERRING:
            obj.transfer_elapsed_seconds = 0
        obj.state, obj.transfer_started_at = ObjectState.TRANSFERRING, utcnow()
        obj.transfer_progress_bytes, obj.transfer_progress_at, obj.transfer_rate_mbps = 0, utcnow(), 0
        worker_session.commit()

        args = {"Bucket": source_bucket, "Key": obj.object_key}
        if obj.version_id:
            args["VersionId"] = obj.version_id
        # GetObject already returns the metadata and content headers that were
        # formerly fetched with a separate HeadObject request.
        response = s3.get_object(**args)
        tags = s3.get_object_tagging(**args).get("TagSet", []) if preserve_s3_tags else []
        body = response["Body"]
        # OCI metadata is preserved where supported. S3 object tags have no
        # equivalent OCI tag API, so their complete evidence remains in the
        # control database and a bounded JSON copy is stored as metadata.
        metadata = {str(k).lower(): str(v) for k, v in response.get("Metadata", {}).items()}
        # These immutable source facts let a later OCI-only reconciliation
        # prove that the object is associated with this discovered S3 version
        # without downloading the payload again.
        if obj.etag:
            metadata["s3-oci-source-etag"] = obj.etag
        if obj.last_modified:
            metadata["s3-oci-source-last-modified"] = obj.last_modified.isoformat()
        tags_json = json.dumps({tag["Key"]: tag["Value"] for tag in tags}, separators=(",", ":"), ensure_ascii=False)
        if preserve_s3_tags:
            metadata["s3-oci-tags-json"] = tags_json[:1800]
        progress_bytes, progress_baseline, progress_baseline_at, last_persist, persisted_once = 0, 0, time.monotonic(), time.monotonic(), False
        elapsed_baseline = time.monotonic()

        def record_progress(size: int) -> None:
            nonlocal progress_bytes, progress_baseline, progress_baseline_at, last_persist, persisted_once, elapsed_baseline
            progress_bytes += size
            now = time.monotonic()
            # Persist at most once every two seconds per active object. This
            # keeps a restart-safe live rate without turning each read chunk
            # into a PostgreSQL write.
            if persisted_once and now - last_persist < 2:
                return
            obj.transfer_progress_bytes = progress_bytes
            obj.transfer_progress_at = utcnow()
            obj.transfer_elapsed_seconds += max(0, now - elapsed_baseline)
            elapsed_baseline = now
            if persisted_once:
                elapsed = max(now - progress_baseline_at, 0.001)
                obj.transfer_rate_mbps = round(((progress_bytes - progress_baseline) * 8) / elapsed / 1_000_000, 2)
            else:
                obj.transfer_rate_mbps = 0
            worker_session.commit()
            progress_baseline, progress_baseline_at, last_persist, persisted_once = progress_bytes, now, now, True

        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        oci_client = oci.object_storage.ObjectStorageClient({}, signer=signer)
        full_digest = hashlib.sha256()

        def read_part(limit: int) -> bytes:
            chunks, remaining = [], limit
            while remaining:
                chunk = body.read(min(COPY_CHUNK_SIZE, remaining))
                if not chunk:
                    break
                chunks.append(chunk)
                full_digest.update(chunk)
                remaining -= len(chunk)
            return b"".join(chunks)

        def throttle_uploaded(size: int, started: float) -> None:
            if rate_bytes_per_second <= 0:
                return
            remaining = (size / rate_bytes_per_second) - (time.monotonic() - started)
            if remaining > 0:
                time.sleep(remaining)

        if obj.size_bytes <= DIRECT_SHA_LIMIT:
            payload = read_part(obj.size_bytes)
            if len(payload) != obj.size_bytes:
                raise RuntimeError(f"S3 object ended early: expected {obj.size_bytes} bytes, received {len(payload)}")
            checksum_b64 = sha256_b64(payload)
            upload_started = time.monotonic()
            oci_client.put_object(
                namespace, destination_bucket, obj.object_key, payload,
                content_length=obj.size_bytes, content_type=response.get("ContentType"), opc_meta=metadata,
                opc_checksum_algorithm="SHA256", opc_content_sha256=checksum_b64,
            )
            throttle_uploaded(len(payload), upload_started)
            record_progress(len(payload))
            delivery_algorithm, delivery_checksum = "SHA256", checksum_b64
        else:
            # A multipart upload is a durable transaction.  Do not abort it on
            # a transient failure: its upload id and accepted-part evidence are
            # checkpointed in PostgreSQL and a subsequent worker resumes it.
            upload_id = obj.multipart_upload_id
            persisted_parts = json.loads(obj.multipart_parts_json or "{}")
            remote_parts: dict[int, dict] = {}
            if upload_id:
                try:
                    remote_parts = multipart_parts_on_oci(oci_client, namespace, destination_bucket, obj.object_key, upload_id)
                except Exception:
                    # OCI may have expired or explicitly removed an incomplete
                    # upload. Clear only the local checkpoint and start a fresh
                    # transaction; completed destination objects are never removed.
                    upload_id, persisted_parts, remote_parts = None, {}, {}
                    obj.multipart_upload_id, obj.multipart_parts_json = None, "{}"
                    worker_session.commit()
            if not upload_id:
                create = oci_client.create_multipart_upload(
                    namespace, destination_bucket,
                    oci.object_storage.models.CreateMultipartUploadDetails(
                        object=obj.object_key, content_type=response.get("ContentType"),
                        # PutObject receives opc_meta without the wire prefix,
                        # while CreateMultipartUploadDetails expects the exact
                        # OCI metadata header names.
                        metadata={f"opc-meta-{key}": value for key, value in metadata.items()},
                    ),
                    opc_checksum_algorithm="SHA256",
                )
                upload_id = create.data.upload_id
                obj.multipart_upload_id = upload_id
                obj.multipart_part_size = effective_multipart_part_size(obj.size_bytes, configured_multipart_part_size)
                obj.multipart_parts_json, obj.multipart_updated_at = "{}", utcnow()
                worker_session.commit()

            part_size = int(obj.multipart_part_size or effective_multipart_part_size(obj.size_bytes, configured_multipart_part_size))
            total_parts = (obj.size_bytes + part_size - 1) // part_size
            completed_bytes = 0
            parts = []
            part_digests: list[bytes] = []
            for part_number in range(1, total_parts + 1):
                expected_size = expected_part_size(obj.size_bytes, part_number, part_size)
                remote = remote_parts.get(part_number)
                evidence = persisted_parts.get(str(part_number), {})
                if reusable_multipart_part(remote, evidence, expected_size):
                    completed_bytes += expected_size
                    part_digests.append(base64.b64decode(evidence["sha256"]))
                    parts.append(oci.object_storage.models.CommitMultipartUploadPartDetails(part_num=part_number, etag=remote["etag"]))
                    continue

                start = (part_number - 1) * part_size
                part_args = dict(args)
                part_args["Range"] = f"bytes={start}-{start + expected_size - 1}"
                part_response = s3.get_object(**part_args)
                payload = part_response["Body"].read()
                if len(payload) != expected_size:
                    raise RuntimeError(f"S3 range ended early for multipart part {part_number}: expected {expected_size}, received {len(payload)}")
                full_digest.update(payload)
                digest_b64 = sha256_b64(payload)
                upload_started = time.monotonic()
                uploaded = oci_client.upload_part(
                    namespace, destination_bucket, obj.object_key, upload_id, part_number, payload,
                    content_length=len(payload), opc_checksum_algorithm="SHA256", opc_content_sha256=digest_b64,
                )
                completed_bytes += len(payload)
                persisted_parts[str(part_number)] = {"etag": uploaded.headers["etag"], "size": len(payload), "sha256": digest_b64}
                obj.multipart_parts_json = json.dumps(persisted_parts, separators=(",", ":"))
                obj.multipart_updated_at = utcnow()
                worker_session.commit()
                # Persist acceptance before applying the optional throughput
                # delay. A power loss during that delay must still resume this
                # exact OCI part rather than upload it again.
                throttle_uploaded(len(payload), upload_started)
                progress_bytes = completed_bytes
                record_progress(0)
                parts.append(oci.object_storage.models.CommitMultipartUploadPartDetails(part_num=part_number, etag=uploaded.headers["etag"]))
                part_digests.append(base64.b64decode(digest_b64))
            if len(parts) != total_parts:
                raise RuntimeError("Multipart upload has incomplete part evidence")
            oci_client.commit_multipart_upload(
                namespace, destination_bucket, obj.object_key, upload_id,
                oci.object_storage.models.CommitMultipartUploadDetails(parts_to_commit=parts),
            )
            obj.multipart_upload_id, obj.multipart_updated_at = None, utcnow()
            delivery_algorithm = "SHA256_MULTIPART"
            delivery_checksum = base64.b64encode(hashlib.sha256(b"".join(part_digests)).digest()).decode("ascii")
        # A resumed multipart transfer may deliberately skip already-accepted
        # source ranges, so a linear source SHA-256 is unavailable in that run.
        # Per-part SHA-256 evidence and OCI acceptance remain the normal
        # cryptographic control; a deep audit can request a full reread later.
        checksum = full_digest.hexdigest() if obj.size_bytes <= DIRECT_SHA_LIMIT or not remote_parts else None
        obj.metadata_json, obj.tags_json = json.dumps(response.get("Metadata", {})), tags_json
        obj.source_checksum, obj.destination_checksum = checksum, None
        # OCI independently verifies the submitted SHA-256 before accepting a
        # direct object or every multipart part. This is the normal integrity
        # control; a full destination reread remains an explicit deep audit.
        obj.checksum_algorithm, obj.integrity_verified_at, obj.integrity_error = ("SHA256" if checksum else "SHA256_MULTIPART_PARTS"), None, None
        obj.delivery_integrity_algorithm = delivery_algorithm
        obj.delivery_integrity_checksum = delivery_checksum
        obj.delivery_integrity_status, obj.delivery_integrity_verified_at = "OCI_ACCEPTED", utcnow()
        obj.transfer_elapsed_seconds += max(0, time.monotonic() - elapsed_baseline)
        obj.state, obj.transferred_at = ObjectState.TRANSFERRED, utcnow()
        obj.transfer_progress_bytes, obj.transfer_progress_at, obj.transfer_rate_mbps = obj.size_bytes, utcnow(), 0
        worker_session.commit()


def transfer_object_simulated(
    object_id: int,
    rate_bytes_per_second: float,
    active_workers: int,
    configured_multipart_part_size: int = DEFAULT_MULTIPART_PART_SIZE,
) -> None:
    """Run the production object state machine against typed simulator ports.

    The main database remains the authority for leases and checkpoints.  The
    simulator receives the same byte ranges the real worker would upload, then
    independently regenerates the expected source bytes, validates them and
    discards the payload.  No simulated object body is persisted.
    """
    with SessionLocal() as worker_session:
        obj = worker_session.get(ObjectRecord, object_id)
        if not obj or obj.state not in {ObjectState.RESTORED, ObjectState.TRANSFERRING}:
            return
        source = obj.wave.source
        context = simulation_execution_context(source)
        source_port = SimulatedSourcePort(runtime_context.simulator_base_url)
        destination_port = SimulatedDestinationPort(runtime_context.simulator_base_url)
        if obj.state != ObjectState.TRANSFERRING:
            obj.transfer_elapsed_seconds = 0
        obj.state, obj.transfer_started_at = ObjectState.TRANSFERRING, utcnow()
        obj.transfer_progress_bytes, obj.transfer_progress_at, obj.transfer_rate_mbps = (
            0,
            utcnow(),
            0,
        )
        worker_session.commit()
        elapsed_baseline = time.monotonic()
        progress_bytes = 0

        def throttle(size: int, started: float) -> None:
            if rate_bytes_per_second <= 0:
                return
            delay = size / rate_bytes_per_second - (time.monotonic() - started)
            if delay > 0:
                time.sleep(delay)

        def persist_progress(completed: int) -> None:
            nonlocal progress_bytes, elapsed_baseline
            now = time.monotonic()
            elapsed = max(now - elapsed_baseline, 0)
            delta = max(0, completed - progress_bytes)
            obj.transfer_elapsed_seconds += elapsed
            obj.transfer_progress_bytes = completed
            obj.transfer_progress_at = utcnow()
            obj.transfer_rate_mbps = (
                round(delta * 8 / max(elapsed, 0.001) / 1_000_000, 2) if delta else 0
            )
            progress_bytes, elapsed_baseline = completed, now
            worker_session.commit()

        identity = ObjectIdentity(
            bucket=source.s3_bucket,
            key=obj.object_key,
            version_id=obj.version_id,
        )
        if source.simulation_fidelity == "CONTROL":
            result = destination_port.transfer_logically(
                LogicalTransferRequest(
                    context=context,
                    source=identity,
                    destination_bucket=source.destination_bucket,
                    size_bytes=obj.size_bytes,
                    allocated_rate_mbps=rate_bytes_per_second * 8 / 1_000_000,
                    active_workers=active_workers,
                    network_operation_key=f"wave:{obj.wave_id}",
                    idempotency_key=uuid5(
                        NAMESPACE_URL,
                        f"raijin:simulation:logical-transfer:{context.execution_id}:{obj.id}",
                    ),
                )
            )
            obj.transfer_elapsed_seconds += result.simulated_elapsed_seconds
            obj.transfer_progress_bytes = obj.size_bytes
            obj.transfer_progress_at = utcnow()
            obj.transfer_rate_mbps = (
                round(obj.size_bytes * 8 / result.simulated_elapsed_seconds / 1_000_000, 2)
                if result.simulated_elapsed_seconds > 0
                else 0
            )
            obj.source_checksum = None
            obj.destination_checksum = None
            obj.checksum_algorithm = "SIMULATED_LOGICAL_EVIDENCE"
            obj.delivery_integrity_algorithm = "SIMULATED_LOGICAL_EVIDENCE"
            obj.delivery_integrity_checksum = result.evidence.checksum_sha256
            obj.delivery_integrity_status = "OCI_ACCEPTED"
            obj.delivery_integrity_verified_at = utcnow()
            obj.state, obj.transferred_at = ObjectState.TRANSFERRED, utcnow()
            worker_session.commit()
            return

        def read_range(offset: int, length: int) -> bytes:
            if length == 0:
                return b""
            payload = b"".join(
                source_port.read_range(
                    ReadRangeRequest(
                        context=context,
                        object=identity,
                        offset=offset,
                        length=length,
                    )
                )
            )
            if len(payload) != length:
                raise RuntimeError(
                    f"Simulated source range ended early: expected {length}, received {len(payload)}"
                )
            return payload

        descriptor = ObjectDescriptor(
            bucket=source.destination_bucket,
            key=obj.object_key,
            version_id=obj.version_id,
            size_bytes=obj.size_bytes,
            storage_class="STANDARD",
            etag=obj.etag,
            last_modified=obj.last_modified,
            metadata=json.loads(obj.metadata_json or "{}"),
            tags=json.loads(obj.tags_json or "{}"),
        )
        full_digest = hashlib.sha256()
        resumed = False
        if obj.size_bytes <= DIRECT_SHA_LIMIT:
            payload = read_range(0, obj.size_bytes)
            full_digest.update(payload)
            checksum = base64.b64encode(full_digest.digest()).decode("ascii")
            started = time.monotonic()
            evidence = destination_port.put_object(
                PutObjectRequest(
                    context=context,
                    object=descriptor,
                    source_checksum_sha256=checksum,
                    idempotency_key=uuid5(
                        NAMESPACE_URL,
                        f"raijin:simulation:put:{context.execution_id}:{obj.id}",
                    ),
                ),
                iter((payload,)),
            )
            throttle(len(payload), started)
            persist_progress(len(payload))
            delivery_algorithm, delivery_checksum = "SHA256", evidence.checksum_sha256
            checksum_algorithm, source_checksum = "SHA256", checksum
        else:
            part_size = int(
                obj.multipart_part_size
                or effective_multipart_part_size(
                    obj.size_bytes, configured_multipart_part_size
                )
            )
            upload_id = obj.multipart_upload_id
            persisted_parts = json.loads(obj.multipart_parts_json or "{}")
            if not upload_id:
                upload_id = destination_port.create_multipart(
                    MultipartCreateRequest(
                        context=context,
                        object=descriptor,
                        part_size_bytes=part_size,
                        idempotency_key=uuid5(
                            NAMESPACE_URL,
                            f"raijin:simulation:multipart:{context.execution_id}:{obj.id}",
                        ),
                    )
                )
                obj.multipart_upload_id = upload_id
                obj.multipart_part_size = part_size
                obj.multipart_parts_json = "{}"
                obj.multipart_updated_at = utcnow()
                worker_session.commit()
            total_parts = (obj.size_bytes + part_size - 1) // part_size
            completed_bytes = 0
            parts: list[MultipartPartEvidence] = []
            for part_number in range(1, total_parts + 1):
                expected_size = expected_part_size(obj.size_bytes, part_number, part_size)
                prior = persisted_parts.get(str(part_number))
                if prior and prior.get("size") == expected_size and prior.get("sha256"):
                    resumed = True
                    completed_bytes += expected_size
                    parts.append(
                        MultipartPartEvidence(
                            part_number=part_number,
                            size_bytes=expected_size,
                            checksum_sha256=prior["sha256"],
                            etag=prior["etag"],
                        )
                    )
                    continue
                offset = (part_number - 1) * part_size
                payload = read_range(offset, expected_size)
                digest = sha256_b64(payload)
                full_digest.update(payload)
                started = time.monotonic()
                part = destination_port.upload_part(
                    MultipartPartRequest(
                        context=context,
                        upload_id=upload_id,
                        object=identity,
                        part_number=part_number,
                        size_bytes=expected_size,
                        checksum_sha256=digest,
                        idempotency_key=uuid5(
                            NAMESPACE_URL,
                            f"raijin:simulation:part:{upload_id}:{part_number}",
                        ),
                    ),
                    iter((payload,)),
                )
                throttle(len(payload), started)
                completed_bytes += len(payload)
                parts.append(part)
                persisted_parts[str(part_number)] = {
                    "etag": part.etag,
                    "size": part.size_bytes,
                    "sha256": part.checksum_sha256,
                }
                obj.multipart_parts_json = json.dumps(
                    persisted_parts, separators=(",", ":")
                )
                obj.multipart_updated_at = utcnow()
                worker_session.commit()
                persist_progress(completed_bytes)
            evidence = destination_port.commit_multipart(
                MultipartCommitRequest(
                    context=context,
                    upload_id=upload_id,
                    object=identity,
                    parts=parts,
                    full_checksum_sha256=(
                        None
                        if resumed
                        else base64.b64encode(full_digest.digest()).decode("ascii")
                    ),
                    idempotency_key=uuid5(
                        NAMESPACE_URL,
                        f"raijin:simulation:commit:{upload_id}",
                    ),
                )
            )
            obj.multipart_upload_id, obj.multipart_updated_at = None, utcnow()
            persist_progress(obj.size_bytes)
            delivery_algorithm = "SHA256_MULTIPART_PARTS"
            delivery_checksum = evidence.checksum_sha256
            checksum_algorithm = "SHA256_MULTIPART_PARTS"
            source_checksum = (
                None if resumed else base64.b64encode(full_digest.digest()).decode("ascii")
            )

        obj.source_checksum, obj.destination_checksum = source_checksum, None
        obj.checksum_algorithm = checksum_algorithm
        obj.integrity_verified_at, obj.integrity_error = None, None
        obj.delivery_integrity_algorithm = delivery_algorithm
        obj.delivery_integrity_checksum = delivery_checksum
        obj.delivery_integrity_status = "OCI_ACCEPTED"
        obj.delivery_integrity_verified_at = utcnow()
        obj.transfer_elapsed_seconds += max(0, time.monotonic() - elapsed_baseline)
        obj.state, obj.transferred_at = ObjectState.TRANSFERRED, utcnow()
        obj.transfer_progress_bytes, obj.transfer_progress_at, obj.transfer_rate_mbps = (
            obj.size_bytes,
            utcnow(),
            0,
        )
        worker_session.commit()


def _continuous_source_now(source: Source) -> datetime:
    """Use the source-owned clock when a Fujin execution owns the source."""
    if runtime_context.is_simulation and source.backend_kind == "SIMULATED":
        return simulation_virtual_now(source)
    return utcnow()


def _raiju_host_capacity_factor() -> tuple[float, str]:
    """Return a conservative local capacity factor for lane autoscaling.

    The transfer limit remains authoritative.  CPU and memory only prevent
    Raikou from adding workers when the VM is already under pressure; they
    never reduce an active lane below the configured Raiju floor.
    """
    cpu_count = max(1, os.cpu_count() or 1)
    try:
        load_ratio = os.getloadavg()[0] / cpu_count
    except (AttributeError, OSError):
        load_ratio = 0.0
    mem_total = mem_available = 0
    try:
        with open("/proc/meminfo", encoding="utf-8") as meminfo:
            values = dict(
                line.split(":", 1) for line in meminfo if ":" in line
            )
        mem_total = int(values.get("MemTotal", "0 kB").split()[0] or 0)
        mem_available = int(values.get("MemAvailable", "0 kB").split()[0] or 0)
    except (OSError, ValueError):
        pass
    memory_ratio = (mem_available / mem_total) if mem_total else 1.0
    if load_ratio >= .95 or memory_ratio <= .10:
        return .50, "host pressure: preserving transfer floor"
    if load_ratio >= .75 or memory_ratio <= .20:
        return .75, "host pressure: limiting Raiju expansion"
    return 1.0, "host capacity available"


def _continuous_raiju_worker_count(session, source: Source, available_objects: int,
                                   max_throughput_mbps: int) -> int:
    """Allocate Raiju concurrency from source evidence, never below the floor.

    The continuous lane is deliberately source-scoped today.  Its queue rows
    already carry ``project_id`` so the same calculation can become
    project-scoped without changing item identity or recovery semantics.
    """
    if available_objects <= 0:
        return 0
    floor = min(RAIJU_MIN_WORKERS, available_objects)
    samples = list(session.scalars(
        select(ObjectRecord)
        .where(
            ObjectRecord.source_id == source.id,
            ObjectRecord.transfer_elapsed_seconds > 0,
            ObjectRecord.state.in_([ObjectState.TRANSFERRED, ObjectState.VERIFIED]),
        )
        .order_by(ObjectRecord.transferred_at.desc())
        .limit(100)
    ))
    rates = [
        obj.size_bytes * 8 / max(.001, float(obj.transfer_elapsed_seconds)) / 1_000_000
        for obj in samples if obj.size_bytes > 0
    ]
    if not rates:
        return floor
    observed_per_raiju = max(.1, sum(rates) / len(rates))
    requested = math.ceil(max_throughput_mbps / observed_per_raiju)
    capacity_factor, capacity_reason = _raiju_host_capacity_factor()
    capped_request = max(floor, math.floor(requested * capacity_factor))
    chosen = min(RAIJU_MAX_WORKERS, available_objects, capped_request)
    if capacity_factor < 1:
        event(
            session,
            "RAIJU_AUTOSCALE_HOST_GUARD",
            f"Raikou limited Raiju expansion to {chosen}/{requested}: {capacity_reason}",
            source_id=source.id,
        )
    return chosen


def _continuous_item_query(source_id: int, now: datetime,
                           minimum_priority: int | None = None):
    query = (
        select(TransferQueueItem)
        .join(Wave, Wave.id == TransferQueueItem.wave_id)
        .where(
            TransferQueueItem.source_id == source_id,
            TransferQueueItem.state.in_([
                TransferQueueState.READY, TransferQueueState.RETRY_WAIT,
                TransferQueueState.MULTIPART_RESUME,
            ]),
            or_(TransferQueueItem.retry_at.is_(None), TransferQueueItem.retry_at <= now),
            Wave.status != "PAUSED",
        )
    )
    if minimum_priority is not None:
        query = query.where(TransferQueueItem.priority_score >= int(minimum_priority))
    # The expiration is the stable database-side urgency order. Scores are
    # refreshed on a bounded candidate page immediately before this query,
    # avoiding a full source scan for every free worker slot.
    return query.order_by(
            TransferQueueItem.restore_expires_at.is_(None),
            TransferQueueItem.restore_expires_at,
            TransferQueueItem.priority_score.desc(),
            TransferQueueItem.available_at,
            TransferQueueItem.id,
        )


def _continuous_ready_item_count(session, source_id: int, now: datetime) -> int:
    """Return dispatchable backlog without loading the objects into memory."""
    return int(session.scalar(
        select(func.count(TransferQueueItem.id)).join(Wave, Wave.id == TransferQueueItem.wave_id).where(
            TransferQueueItem.source_id == source_id,
            TransferQueueItem.state.in_([
                TransferQueueState.READY, TransferQueueState.RETRY_WAIT,
                TransferQueueState.MULTIPART_RESUME,
            ]),
            or_(TransferQueueItem.retry_at.is_(None), TransferQueueItem.retry_at <= now),
            Wave.status != "PAUSED",
        )
    ) or 0)


def recover_expired_continuous_item_leases(session, source: Source) -> int:
    """Return abandoned object claims after a Raiju/VM interruption.

    Task recovery alone is insufficient once claim ownership moved below the
    wave level.  The item lease is the exact idempotency fence: an expired
    lease becomes READY and retains any multipart checkpoint on the object.
    """
    now = utcnow()
    # Recovery is invoked on every claim. Bound it just like admission and
    # priority refresh; repeated dispatch cycles deterministically drain an
    # outage with millions of expired leases without creating one huge ORM
    # materialization or long transaction.
    items = list(session.scalars(
        select(TransferQueueItem).where(
            TransferQueueItem.source_id == source.id,
            TransferQueueItem.state == TransferQueueState.LEASED,
            TransferQueueItem.lease_expires_at.is_not(None),
            TransferQueueItem.lease_expires_at < now,
        ).order_by(TransferQueueItem.lease_expires_at, TransferQueueItem.id)
        .with_for_update(skip_locked=True)
        .limit(TRANSFER_LANE_CLAIM_CANDIDATE_PAGE_SIZE)
    ))
    for item in items:
        item.state = TransferQueueState.MULTIPART_RESUME
        item.lease_token = item.lease_owner = None
        item.lease_expires_at = None
        item.decision_reason = "Raiju lease expired; resumable object returned to the continuous lane"
        obj = session.get(ObjectRecord, item.object_id)
        if obj and obj.state == ObjectState.TRANSFERRING:
            obj.state = ObjectState.RESTORED
    if items:
        event(session, "CONTINUOUS_TRANSFER_LEASES_RECOVERED",
              f"Raikou recovered {len(items)} expired object lease(s)", source_id=source.id)
    # A reservation is valid only while both the running normal object and its
    # critical successor are leased by this dispatcher. Clear abandoned links
    # so a later critical object can request a fresh safe handoff.
    stale_reservations = list(session.scalars(
        select(TransferQueueItem).where(
            TransferQueueItem.source_id == source.id,
            TransferQueueItem.state == TransferQueueState.LEASED,
            TransferQueueItem.preemption_successor_item_id.is_not(None),
        ).limit(TRANSFER_LANE_CLAIM_CANDIDATE_PAGE_SIZE)
    ))
    for target in stale_reservations:
        successor = session.get(TransferQueueItem, target.preemption_successor_item_id)
        if successor is None or successor.state != TransferQueueState.LEASED or (
            successor.lease_expires_at is not None and successor.lease_expires_at < now
        ):
            target.preemption_successor_item_id = None
            target.preemption_requested_at = None
            target.decision_reason = "cooperative handoff reservation expired; normal dispatch resumed"
    return len(items)


def claim_continuous_transfer_batch(session, source: Source, settings, task: Task,
                                    max_items: int | None = None,
                                    minimum_priority: int | None = None) -> list[TransferQueueItem]:
    """Claim a small source-local batch without ever splitting an object.

    A critical batch is intentionally smaller.  This is the preemption
    mechanism: Raiju only commits the currently assigned objects to a thread
    pool; unstarted claimed entries are returned immediately, allowing a
    newly released critical object to take the next slot safely.
    """
    # Priority/expiry belongs to the source timeline; task retries and leases
    # are deliberately real-time so a virtual date can never make an item
    # permanently ineligible for retry.
    recover_expired_continuous_item_leases(session, source)
    priority_now = _continuous_source_now(source)
    claim_now = utcnow()
    refresh_transfer_queue_priorities(session, source.id, now=priority_now)
    candidates = list(session.scalars(
        _continuous_item_query(source.id, claim_now, minimum_priority)
        .with_for_update(skip_locked=True)
        .limit(TRANSFER_LANE_CLAIM_CANDIDATE_PAGE_SIZE)
    ))
    if not candidates:
        return []
    critical = candidates[0].priority_score >= int(settings.continuous_transfer_critical_priority)
    object_limit = int(
        settings.continuous_transfer_critical_batch_max_objects
        if critical else settings.continuous_transfer_batch_max_objects
    )
    if max_items is not None:
        object_limit = min(object_limit, max(1, int(max_items)))
    byte_limit = int(
        settings.continuous_transfer_critical_batch_max_bytes
        if critical else settings.continuous_transfer_batch_max_bytes
    )
    selected: list[TransferQueueItem] = []
    total_bytes = 0
    for item in candidates:
        is_critical = item.priority_score >= int(settings.continuous_transfer_critical_priority)
        # Never mix an urgent microbatch with normal backlog.  A normal batch
        # may include elevated work, preserving deadline order.
        if critical != is_critical:
            break
        if selected and (len(selected) >= object_limit or total_bytes + item.size_bytes > byte_limit):
            break
        selected.append(item)
        total_bytes += int(item.size_bytes)
        if len(selected) >= object_limit:
            break
    lease_token = str(uuid4())
    lease_expires = utcnow() + timedelta(seconds=int(settings.task_lease_seconds))
    lead_priority_reason = selected[0].decision_reason or "priority refresh"
    dispatch_batch = TransferDispatchBatch(
        source_id=source.id,
        wave_id=selected[0].wave_id,
        task_id=task.id,
        priority_band="CRITICAL" if critical else selected[0].priority_band,
        priority_score=int(selected[0].priority_score or 0),
        object_limit=object_limit,
        byte_limit=byte_limit,
        object_count=len(selected),
        bytes_planned=total_bytes,
        reason=lead_priority_reason,
    )
    session.add(dispatch_batch)
    session.flush()
    for item in selected:
        item.state = TransferQueueState.LEASED
        item.lease_token = lease_token
        item.lease_owner = WORKER_ID
        item.lease_expires_at = lease_expires
        item.attempts = int(item.attempts or 0) + 1
        item.dispatch_batch_id = dispatch_batch.id
        item.decision_reason = (
            f"claimed by Raiju as {'critical' if critical else 'normal'} batch "
            f"({len(selected)} object(s), {total_bytes} bytes)"
        )
        obj = session.get(ObjectRecord, item.object_id)
        if obj and obj.state == ObjectState.RESTORED:
            obj.state = ObjectState.TRANSFERRING
    lead = selected[0]
    event(
        session,
        "CONTINUOUS_TRANSFER_DISPATCH_DECISION",
        (
            f"Raikou selected {len(selected)} {'critical' if critical else 'normal'} lane item(s) "
            f"({total_bytes} bytes); lead score={lead.priority_score}, band={lead.priority_band}; "
            f"reason={lead_priority_reason}"
        ),
        source_id=source.id,
        wave_id=lead.wave_id,
    )
    task.lease_expires_at = lease_expires
    session.flush()
    return selected


def _return_unstarted_queue_items(session, items: list[TransferQueueItem], reason: str) -> None:
    for item in items:
        item.state = TransferQueueState.READY
        item.lease_token = item.lease_owner = None
        item.lease_expires_at = None
        item.decision_reason = reason
        obj = session.get(ObjectRecord, item.object_id)
        if obj and obj.state == ObjectState.TRANSFERRING:
            obj.state = ObjectState.RESTORED


def choose_cooperative_preemption_target(session, source_id: int,
                                         running_item_ids: list[int],
                                         critical_item: TransferQueueItem,
                                         critical_priority: int,
                                         now: datetime | None = None) -> TransferQueueItem | None:
    """Choose one normal Raiju for a safe handoff at the next free slot.

    A Raiju is never interrupted in the middle of an object.  The selected
    normal item is therefore an auditable *handoff target*: it finishes its
    current object, after which the critical item wins the released slot by
    normal priority ordering.  The cooldown is written on that running normal
    item and is checked before another handoff event can be emitted.  It does
    not hide or delay the urgent item itself.
    """
    if not running_item_ids or int(critical_item.priority_score or 0) < critical_priority:
        return None
    reference = now or utcnow()
    rows = list(session.scalars(
        select(TransferQueueItem)
        .where(
            TransferQueueItem.source_id == source_id,
            TransferQueueItem.id.in_(running_item_ids),
            TransferQueueItem.state == TransferQueueState.LEASED,
            TransferQueueItem.priority_score < critical_priority,
        )
        .with_for_update(skip_locked=True)
    ))
    if not rows:
        return None
    # A source-wide handoff cooldown is represented by the running normal
    # claims.  Do not generate competing preemption decisions while any prior
    # target is still inside its fence.
    def cooldown_is_active(item: TransferQueueItem) -> bool:
        value = item.preemption_cooldown_until
        if value is None:
            return False
        if value.tzinfo is None and reference.tzinfo is not None:
            value = value.replace(tzinfo=reference.tzinfo)
        return value > reference

    if any(cooldown_is_active(item) for item in rows):
        return None

    # Keep the historical row visible to the cooldown check above, then
    # exclude it from a new reservation. This prevents a second critical item
    # from attaching to the same normal object after the short fence expires.
    rows = [item for item in rows if item.preemption_successor_item_id is None]
    if not rows:
        return None

    def remaining_seconds(item: TransferQueueItem) -> float:
        obj = session.get(ObjectRecord, item.object_id)
        remaining = max(0, int(item.size_bytes or 0) - int(getattr(obj, "transfer_progress_bytes", 0) or 0))
        rate_mbps = max(.1, float(getattr(obj, "transfer_rate_mbps", 0) or 0))
        return remaining * 8 / (rate_mbps * 1_000_000)

    target = min(rows, key=lambda item: (remaining_seconds(item), item.id))
    target.preemption_count = int(target.preemption_count or 0) + 1
    target.preemption_cooldown_until = reference + timedelta(seconds=COOPERATIVE_PREEMPTION_COOLDOWN_SECONDS)
    target.preemption_successor_item_id = critical_item.id
    target.preemption_requested_at = reference
    target.decision_reason = (
        "cooperative handoff target; finishes current object before critical "
        f"item {critical_item.id} is admitted"
    )
    return target


def reconcile_continuous_source_waves(session, source: Source) -> None:
    """Derive wave status from object truth and durable lane entries."""
    for wave in session.scalars(select(Wave).where(Wave.source_id == source.id)):
        if wave.status == "PAUSED":
            continue
        reapproval = session.scalar(select(func.count(TransferQueueItem.id)).where(
            TransferQueueItem.wave_id == wave.id,
            TransferQueueItem.state == TransferQueueState.REAPPROVAL_REQUIRED,
        )) or 0
        pending_restore = session.scalar(select(func.count(ObjectRecord.id)).where(
            ObjectRecord.wave_id == wave.id,
            ObjectRecord.state.in_([
                ObjectState.RESTORE_REQUESTED, ObjectState.RESTORE_REQUEST_ACCEPTED,
                ObjectState.RESTORING,
            ]),
        )) or 0
        outstanding = session.scalar(select(func.count(TransferQueueItem.id)).where(
            TransferQueueItem.wave_id == wave.id,
            TransferQueueItem.state.in_([
                TransferQueueState.READY, TransferQueueState.LEASED,
                TransferQueueState.RETRY_WAIT, TransferQueueState.MULTIPART_RESUME,
            ]),
        )) or 0
        remaining = session.scalar(select(func.count(ObjectRecord.id)).where(
            ObjectRecord.wave_id == wave.id,
            ObjectRecord.state.notin_([ObjectState.TRANSFERRED, ObjectState.VERIFIED]),
        )) or 0
        if reapproval:
            wave.status = "RESTORE_REAPPROVAL_REQUIRED"
        elif not remaining:
            if wave.status != "COMPLETED":
                wave.status = "COMPLETED"
                if runtime_context.is_simulation:
                    wave.transfer_completed_virtual_at = _continuous_source_now(source)
                event(session, "CONTINUOUS_WAVE_COMPLETED", "All wave objects reached OCI through the continuous lane.", source_id=source.id, wave_id=wave.id)
        elif pending_restore:
            wave.status = "RESTORE_DRAINING" if outstanding else "RESTORING"
        elif outstanding:
            wave.status = "TRANSFER_DRAINING"

    # Fujin executions are source-scoped.  The previous wave-exclusive
    # executor closed the execution itself; preserve that lifecycle contract
    # after moving delivery ownership to the continuous lane.
    if runtime_context.is_simulation and source.simulation_execution_id:
        total, unfinished = session.execute(
            select(
                func.count(ObjectRecord.id),
                func.count(ObjectRecord.id).filter(
                    ObjectRecord.state.notin_([ObjectState.TRANSFERRED, ObjectState.VERIFIED])
                ),
            ).where(ObjectRecord.source_id == source.id)
        ).one()
        if total and not unfinished:
            SimulatorAdminClient(runtime_context.simulator_base_url).set_execution_state(
                source.simulation_execution_id, "SUCCEEDED"
            )
            event(
                session,
                "SIMULATION_EXECUTION_SUCCEEDED",
                f"Fujin execution completed after all {total} source object(s) reached the simulated destination.",
                source_id=source.id,
            )


def transfer_continuous(session, task: Task, settings) -> None:
    """Move one durable, deadline-aware batch from the continuous lane.

    Waves are intentionally *not* the transfer lock.  A task uses its anchor
    wave only for auditable history; queue entries determine the actual
    objects and may belong to any currently eligible wave of that source.
    """
    anchor = session.get(Wave, task.wave_id)
    if not anchor:
        task.state, task.lease_expires_at = TaskState.CANCELLED, None
        task.error = "Continuous lane anchor wave no longer exists"
        session.commit()
        return
    source = anchor.source
    if source.archived_at:
        task.state, task.lease_expires_at = TaskState.CANCELLED, None
        task.error = "Source is no longer eligible"
        session.commit()
        return
    with simulation_phase_clock_hold(source, "continuous-transfer"):
        initial = claim_continuous_transfer_batch(session, source, settings, task)
        if not initial:
            reconcile_continuous_source_waves(session, source)
            succeed(session, task)
            ensure_transfer_task(session, anchor, settings)
            session.commit()
            return

        # A batch is a durable selection boundary, not a barrier.  Only the
        # Raiju slots that can start immediately retain a lease; every other
        # selected object goes back to READY so a newly-restored critical item
        # can take the next free slot without interrupting active I/O.
        worker_ceiling = _continuous_raiju_worker_count(
            session, source, len(initial), int(settings.max_throughput_mbps)
        )
        for batch_id in {item.dispatch_batch_id for item in initial if item.dispatch_batch_id}:
            batch = session.get(TransferDispatchBatch, batch_id)
            if batch:
                batch.worker_target = worker_ceiling
        active, deferred = initial[:worker_ceiling], initial[worker_ceiling:]
        _return_unstarted_queue_items(session, deferred, "returned before execution; next priority evaluation")
        multipart_part_size = int(settings.multipart_part_size_mib) * 1024 * 1024
        if runtime_context.is_real:
            s3, _, _ = aws_clients(settings, source.aws_region, source)
            namespace = read_oci_runtime_config().get("object_storage_namespace", "").strip()
            if not namespace:
                raise RuntimeError("OCI Object Storage namespace is absent from runtime configuration")
        else:
            s3, namespace = None, "simulated"

        # Future metadata carries the rate allocated when it was dispatched.
        # This permits adaptive Raiju expansion at the next free slot while
        # keeping the aggregate stream rate at or below the configured link.
        futures: dict[object, tuple[int, int, int, float]] = {}
        segments: dict[int, TransferLaneSegment] = {}
        active_wave_counts: dict[int, int] = {}
        # target item id -> (reserved critical item id, Raiju slot).  The
        # successor is leased durably but is not submitted to the executor
        # until the selected normal Raiju reaches its safe object boundary.
        pending_handoffs: dict[int, tuple[int, int]] = {}

        def mark_wave_worker(wave_id: int, delta: int) -> None:
            active_wave_counts[wave_id] = max(0, active_wave_counts.get(wave_id, 0) + delta)
            wave = session.get(Wave, wave_id)
            if wave:
                wave.active_transfer_workers = active_wave_counts[wave_id]
                if delta > 0 and runtime_context.is_simulation:
                    wave.transfer_started_virtual_at = wave.transfer_started_virtual_at or _continuous_source_now(source)

        def submit(executor, item: TransferQueueItem, slot: int, rate: float, active_workers: int) -> None:
            item.last_dispatched_at = utcnow()
            segment = TransferLaneSegment(
                source_id=source.id, wave_id=item.wave_id, queue_item_id=item.id,
                worker_slot=slot, started_at=_continuous_source_now(source),
                entry_reason=item.decision_reason or "continuous lane dispatch",
                nearest_expiry_at=item.restore_expires_at,
            )
            session.add(segment)
            segments[item.id] = segment
            mark_wave_worker(item.wave_id, 1)
            if runtime_context.is_simulation:
                future = executor.submit(transfer_object_simulated, item.object_id, rate, active_workers, multipart_part_size)
            else:
                future = executor.submit(
                    transfer_object, s3, namespace, source.s3_bucket, source.destination_bucket,
                    item.object_id, rate, settings.preserve_s3_tags, multipart_part_size,
                )
            futures[future] = (item.id, item.wave_id, slot, rate)

        def settle(item_id: int, error: Exception | None) -> None:
            session.expire_all()
            item = session.get(TransferQueueItem, item_id)
            if not item:
                return
            obj = session.get(ObjectRecord, item.object_id)
            item.lease_token = item.lease_owner = None
            item.lease_expires_at = None
            segment = segments.get(item_id)
            if segment:
                # CONTROL transfer calls return a logical duration while the
                # worker itself completes in milliseconds.  Persist that
                # duration on the lane segment instead of writing a zero-width
                # interval; the board and adaptive scheduler must see the
                # period the Raiju actually occupied in the virtual world.
                logical_elapsed = float(obj.transfer_elapsed_seconds or 0) if obj else 0
                if runtime_context.is_simulation and logical_elapsed > 0:
                    segment.completed_at = segment.started_at + timedelta(seconds=logical_elapsed)
                else:
                    segment.completed_at = _continuous_source_now(source)
                segment.bytes_transferred = int(obj.size_bytes if error is None and obj else 0)
            if error is None and obj and obj.state == ObjectState.TRANSFERRED:
                item.state, item.transferred_at = TransferQueueState.TRANSFERRED, utcnow()
                item.decision_reason = "OCI delivery accepted with cryptographic evidence"
                if segment:
                    segment.exit_reason = "OCI delivery accepted with cryptographic evidence"
            elif error and restored_object_is_unavailable(error):
                wave = session.get(Wave, item.wave_id)
                if runtime_context.is_simulation and wave and not simulation_restore_expiry_confirmed(session, wave, source):
                    # A simulator 409 before the locally observed expiry is a
                    # contract violation, not a paid restore situation. Keep
                    # the durable item retryable and expose the exact defect.
                    item.state = TransferQueueState.RETRY_WAIT
                    item.retry_at = utcnow() + timedelta(seconds=60)
                    item.decision_reason = "SIMULATOR_RESTORE_STATE_MISMATCH: simulated object unavailable before observed restore expiry"
                    if obj and obj.state == ObjectState.TRANSFERRING:
                        obj.state = ObjectState.RESTORED
                    if segment:
                        segment.exit_reason = "simulator restore-state mismatch; retained for retry after backend correction"
                    event(
                        session,
                        "SIMULATOR_RESTORE_STATE_MISMATCH",
                        "Simulator reported an archived object unavailable before durable restore expiry; no new restore approval was requested.",
                        source_id=source.id,
                        wave_id=item.wave_id,
                    )
                    if item.dispatch_batch_id:
                        batch = session.get(TransferDispatchBatch, item.dispatch_batch_id)
                        if batch:
                            batch.state, batch.completed_at = "COMPLETED", utcnow()
                    return
                item.state = TransferQueueState.REAPPROVAL_REQUIRED
                item.decision_reason = f"restored copy unavailable: {type(error).__name__}: {error}"[:8000]
                if obj:
                    obj.state = ObjectState.RESTORE_REQUESTED
                if wave:
                    wave.restore_reapproval_required = True
                    wave.restore_reapproval_reason = item.decision_reason
                    wave.restore_reapproval_detected_at = utcnow()
                    wave.status = "RESTORE_REAPPROVAL_REQUIRED"
                if segment:
                    segment.exit_reason = "restored copy unavailable; explicit new restore approval required"
            else:
                item.state = TransferQueueState.RETRY_WAIT
                item.retry_at = utcnow() + timedelta(seconds=min(1800, 60 * max(1, int(item.attempts))))
                item.decision_reason = f"retry after transfer error: {type(error).__name__ if error else 'incomplete'}: {error or ''}"[:8000]
                if obj and obj.state == ObjectState.TRANSFERRING:
                    obj.state = ObjectState.RESTORED
                if segment:
                    segment.exit_reason = "transient transfer error; returned to retry queue"
            if item.dispatch_batch_id:
                batch = session.get(TransferDispatchBatch, item.dispatch_batch_id)
                if batch:
                    remaining = session.scalar(select(func.count(TransferQueueItem.id)).where(
                        TransferQueueItem.dispatch_batch_id == batch.id,
                        TransferQueueItem.state == TransferQueueState.LEASED,
                    )) or 0
                    if not remaining:
                        batch.state, batch.completed_at = "COMPLETED", utcnow()

        def renew_lane_leases() -> None:
            """Keep active and reserved handoff items recoverable, not stale."""
            expiry = utcnow() + timedelta(seconds=int(settings.task_lease_seconds))
            protected = [item_id for item_id, _, _, _ in futures.values()]
            protected.extend(successor_id for successor_id, _slot in pending_handoffs.values())
            if protected:
                session.execute(
                    update(TransferQueueItem)
                    .where(
                        TransferQueueItem.id.in_(protected),
                        TransferQueueItem.state == TransferQueueState.LEASED,
                    )
                    .values(lease_expires_at=expiry)
                )
            task.lease_expires_at = expiry

        def reserve_critical_handoff() -> bool:
            """Reserve a critical item for the shortest safe running Raiju.

            This is invoked while every active slot is occupied.  It is the
            durable part of cooperative preemption: the normal object is not
            interrupted, but its next slot is committed to the critical item.
            """
            if pending_handoffs or not futures:
                return False
            critical_priority = int(settings.continuous_transfer_critical_priority)
            reserved = claim_continuous_transfer_batch(
                session, source, settings, task, max_items=1,
                minimum_priority=critical_priority,
            )
            if not reserved:
                return False
            critical_item = reserved[0]
            target = choose_cooperative_preemption_target(
                session,
                source.id,
                [item_id for item_id, _, _, _ in futures.values()],
                critical_item,
                critical_priority,
            )
            if target is None:
                _return_unstarted_queue_items(
                    session, reserved,
                    "critical item remains ready; no safe Raiju handoff target is available",
                )
                return False
            slot = next(
                entry[2] for entry in futures.values() if entry[0] == target.id
            )
            # Claiming deliberately marks the object TRANSFERRING. It is not
            # streaming yet, so return its inventory state to RESTORED while
            # retaining the queue lease as the durable reservation fence.
            critical_object = session.get(ObjectRecord, critical_item.object_id)
            if critical_object and critical_object.state == ObjectState.TRANSFERRING:
                critical_object.state = ObjectState.RESTORED
            critical_item.decision_reason = (
                f"critical cooperative handoff reserved for Raiju slot {slot} "
                f"after normal item {target.id} reaches an object boundary"
            )
            pending_handoffs[target.id] = (critical_item.id, slot)
            batch = session.get(TransferDispatchBatch, critical_item.dispatch_batch_id)
            if batch:
                batch.preempted_batch_id = target.dispatch_batch_id
            event(
                session,
                "CONTINUOUS_TRANSFER_COOPERATIVE_PREEMPTION_RESERVED",
                (
                    f"Raikou reserved critical item {critical_item.id} for Raiju slot {slot}; "
                    f"normal item {target.id} will finish without interruption before handoff."
                ),
                source_id=source.id,
                wave_id=critical_item.wave_id,
            )
            return True

        with ThreadPoolExecutor(max_workers=RAIJU_MAX_WORKERS, thread_name_prefix="raiju-lane") as executor:
            for slot, item in enumerate(active, start=1):
                submit(executor, item, slot, float(settings.max_throughput_mbps) * 125000 / max(1, len(active)), len(active))
            session.commit()
            while futures:
                # Do not wait indefinitely for a large object: newly restored
                # critical work must be able to reserve a safe future slot and
                # spare Raiju capacity must be filled without waiting for an
                # unrelated transfer to finish.
                done, _ = wait(tuple(futures), timeout=5, return_when=FIRST_COMPLETED)
                if not done:
                    refresh_transfer_queue_priorities(session, source.id, now=_continuous_source_now(source))
                    ready_count = _continuous_ready_item_count(session, source.id, utcnow())
                    desired_workers = _continuous_raiju_worker_count(
                        session, source, len(futures) + ready_count, int(settings.max_throughput_mbps)
                    )
                    occupied_slots = {entry[2] for entry in futures.values()}
                    free_slots = [slot for slot in range(1, desired_workers + 1) if slot not in occupied_slots]
                    # Prefer ordinary immediate admission when capacity exists.
                    for slot in free_slots:
                        replacement = claim_continuous_transfer_batch(session, source, settings, task, max_items=1)
                        if not replacement:
                            break
                        item = replacement[0]
                        occupied_rate = sum(entry[3] for entry in futures.values())
                        rate = max(1.0, float(settings.max_throughput_mbps) * 125000 - occupied_rate)
                        submit(executor, item, slot, rate, max(desired_workers, len(futures) + 1))
                    if not free_slots:
                        reserve_critical_handoff()
                    renew_lane_leases()
                    session.commit()
                    continue
                released_slots: list[int] = []
                handoff_starts: list[tuple[int, int]] = []
                for future in done:
                    item_id, wave_id, slot, _rate = futures.pop(future)
                    try:
                        error = None
                        future.result()
                    except Exception as caught:
                        error = caught
                    settle(item_id, error)
                    mark_wave_worker(wave_id, -1)
                    released_slots.append(slot)
                    successor = pending_handoffs.pop(item_id, None)
                    if successor is not None:
                        handoff_starts.append(successor)
                refresh_transfer_queue_priorities(session, source.id, now=_continuous_source_now(source))
                renew_lane_leases()
                session.commit()

                # Re-evaluate Raiju capacity after every completed object.
                # Existing streams retain their budget; only the remainder of
                # the configured link is allocated to newly admitted slots.
                # That makes expansion safe without interrupting an object or
                # a multipart part already in progress.
                ready_count = _continuous_ready_item_count(session, source.id, utcnow())
                desired_workers = _continuous_raiju_worker_count(
                    session, source, len(futures) + ready_count, int(settings.max_throughput_mbps)
                )
                # A reserved critical successor owns the selected normal
                # Raiju's newly released slot. Start it first; this is the
                # actual handoff rather than an advisory preemption event.
                occupied_handoff_slots: set[int] = set()
                for successor_id, slot in handoff_starts:
                    successor = session.get(TransferQueueItem, successor_id)
                    if successor is None or successor.state != TransferQueueState.LEASED:
                        continue
                    batch = session.get(TransferDispatchBatch, successor.dispatch_batch_id)
                    if batch:
                        batch.worker_target = max(desired_workers, len(futures) + 1)
                    occupied_rate = sum(entry[3] for entry in futures.values())
                    rate = max(1.0, float(settings.max_throughput_mbps) * 125000 - occupied_rate)
                    submit(executor, successor, slot, rate, max(desired_workers, len(futures) + 1))
                    occupied_handoff_slots.add(slot)
                    event(
                        session,
                        "CONTINUOUS_TRANSFER_COOPERATIVE_PREEMPTION_EXECUTED",
                        f"Raiju slot {slot} accepted reserved critical item {successor.id} after safe object boundary.",
                        source_id=source.id,
                        wave_id=successor.wave_id,
                    )
                free_slots = [slot for slot in released_slots if slot not in occupied_handoff_slots] + [
                    slot for slot in range(1, RAIJU_MAX_WORKERS + 1)
                    if slot not in {entry[2] for entry in futures.values()} and slot not in released_slots
                ]
                starts = min(len(free_slots), max(0, desired_workers - len(futures)), ready_count)
                for slot in free_slots[:starts]:
                    replacement = claim_continuous_transfer_batch(session, source, settings, task, max_items=1)
                    if not replacement:
                        continue
                    item = replacement[0]
                    batch = session.get(TransferDispatchBatch, item.dispatch_batch_id)
                    if batch:
                        batch.worker_target = desired_workers
                    occupied_rate = sum(entry[3] for entry in futures.values())
                    remaining_starts = max(1, starts - free_slots.index(slot))
                    rate = max(1.0, (float(settings.max_throughput_mbps) * 125000 - occupied_rate) / remaining_starts)
                    submit(executor, item, slot, rate, desired_workers)
                    session.commit()
        for wave_id in list(active_wave_counts):
            wave = session.get(Wave, wave_id)
            if wave:
                wave.active_transfer_workers = 0
        reconcile_continuous_source_waves(session, source)
        refresh_transfer_queue_priorities(session, source.id, now=_continuous_source_now(source))
        session.commit()
    # Mark this dispatch cycle complete *before* asking Raikou for the next
    # one; a new durable task is then immediately eligible if backlog exists.
    succeed(session, task)
    ensure_transfer_task(session, anchor, settings)
    session.commit()


def verify_wave(session, task: Task) -> None:
    """Read OCI objects and compare them to the SHA-256 evidence from S3."""
    wave = session.get(Wave, task.wave_id)
    source = wave.source
    if runtime_context.is_simulation:
        verify_wave_simulated(session, task, wave, source)
        return
    namespace = read_oci_runtime_config().get("object_storage_namespace", "").strip()
    if not namespace:
        raise RuntimeError("OCI Object Storage namespace is absent from runtime configuration")
    signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
    oci_client = oci.object_storage.ObjectStorageClient({}, signer=signer)
    objects = list(session.scalars(select(ObjectRecord).where(
        ObjectRecord.wave_id == wave.id, ObjectRecord.state == ObjectState.TRANSFERRED
    ).order_by(ObjectRecord.id)))
    failed = 0
    for obj in objects:
        multipart_evidence = json.loads(obj.multipart_parts_json or "{}")
        has_multipart_evidence = obj.checksum_algorithm == "SHA256_MULTIPART_PARTS" and bool(multipart_evidence)
        if not obj.source_checksum and not has_multipart_evidence:
            obj.integrity_error, obj.state = "SHA-256 source evidence is missing after transfer", ObjectState.FAILED
            obj.audit_progress_at, obj.audit_rate_mbps = utcnow(), 0
            session.commit()
            failed += 1
            continue
        try:
            destination_body = oci_client.get_object(namespace, source.destination_bucket, obj.object_key).data.raw
            destination_digest = hashlib.sha256()
            obj.audit_started_at, obj.audit_progress_bytes = utcnow(), 0
            obj.audit_progress_at, obj.audit_rate_mbps = utcnow(), 0
            session.commit()
            progress, baseline, baseline_at, last_persist, persisted_once = 0, 0, time.monotonic(), time.monotonic(), False
            part_digests: list[bytes] = []
            part_size = int(obj.multipart_part_size or DEFAULT_MULTIPART_PART_SIZE)
            parts_total = (obj.size_bytes + part_size - 1) // part_size if has_multipart_evidence else 1
            for part_number in range(1, parts_total + 1):
                digest = hashlib.sha256()
                remaining = expected_part_size(obj.size_bytes, part_number, part_size) if has_multipart_evidence else obj.size_bytes
                while remaining:
                    chunk = destination_body.read(min(COPY_CHUNK_SIZE, remaining))
                    if not chunk:
                        raise RuntimeError("OCI object ended early during audit")
                    destination_digest.update(chunk)
                    digest.update(chunk)
                    remaining -= len(chunk)
                    progress += len(chunk)
                    now = time.monotonic()
                    if not persisted_once or now - last_persist >= 2:
                        obj.audit_progress_bytes, obj.audit_progress_at = progress, utcnow()
                        if persisted_once:
                            elapsed = max(now - baseline_at, 0.001)
                            obj.audit_rate_mbps = round(((progress - baseline) * 8) / elapsed / 1_000_000, 2)
                        session.commit()
                        baseline, baseline_at, last_persist, persisted_once = progress, now, now, True
                if has_multipart_evidence:
                    part_digests.append(digest.digest())
            obj.destination_checksum = destination_digest.hexdigest() if obj.source_checksum else base64.b64encode(hashlib.sha256(b"".join(part_digests)).digest()).decode("ascii")
        except Exception as error:
            obj.integrity_error, obj.state = f"Unable to read OCI destination: {type(error).__name__}: {error}", ObjectState.FAILED
            obj.audit_progress_at, obj.audit_rate_mbps = utcnow(), 0
            session.commit()
            failed += 1
            continue
        if (has_multipart_evidence and not multipart_audit_matches(multipart_evidence, part_digests)) or (obj.source_checksum and obj.source_checksum != obj.destination_checksum):
            obj.integrity_error, obj.state = "SHA-256 source/destination mismatch", ObjectState.FAILED
            failed += 1
        else:
            obj.integrity_error, obj.integrity_verified_at, obj.state = None, utcnow(), ObjectState.VERIFIED
        obj.audit_progress_bytes, obj.audit_progress_at, obj.audit_rate_mbps = obj.size_bytes, utcnow(), 0
        session.commit()
    remaining = session.scalar(select(func.count(ObjectRecord.id)).where(
        ObjectRecord.wave_id == wave.id, ObjectRecord.state != ObjectState.VERIFIED
    )) or 0
    wave.status = "VERIFIED" if not remaining else "VERIFICATION_FAILED"
    event(session, "INTEGRITY_VERIFICATION_COMPLETED", f"Operator-requested integrity verification completed; {remaining} object(s) failed or remain pending", source_id=source.id, wave_id=wave.id)
    succeed(session, task)


def verify_wave_simulated(session, task: Task, wave: Wave, source: Source) -> None:
    """Deep-audit discarded destination payload using deterministic replay."""
    if source.simulation_fidelity != "DATA":
        raise RuntimeError(
            "Deep SHA-256 audit requires a DATA simulation; CONTROL validates logical state only"
        )
    context = simulation_execution_context(source)
    source_port = SimulatedSourcePort(runtime_context.simulator_base_url)
    destination_port = SimulatedDestinationPort(runtime_context.simulator_base_url)
    objects = list(
        session.scalars(
            select(ObjectRecord)
            .where(
                ObjectRecord.wave_id == wave.id,
                ObjectRecord.state == ObjectState.TRANSFERRED,
            )
            .order_by(ObjectRecord.id)
        )
    )
    failed = 0
    for obj in objects:
        obj.audit_started_at, obj.audit_progress_bytes = utcnow(), 0
        obj.audit_progress_at, obj.audit_rate_mbps = utcnow(), 0
        session.commit()
        source_digest, destination_digest = hashlib.sha256(), hashlib.sha256()
        try:
            for offset in range(0, obj.size_bytes, COPY_CHUNK_SIZE):
                length = min(COPY_CHUNK_SIZE, obj.size_bytes - offset)
                source_request = ReadRangeRequest(
                    context=context,
                    object=ObjectIdentity(
                        bucket=source.s3_bucket,
                        key=obj.object_key,
                        version_id=obj.version_id,
                    ),
                    offset=offset,
                    length=length,
                    allow_archived_for_audit=True,
                )
                destination_request = ReadRangeRequest(
                    context=context,
                    object=ObjectIdentity(
                        bucket=source.destination_bucket,
                        key=obj.object_key,
                        version_id=obj.version_id,
                    ),
                    offset=offset,
                    length=length,
                )
                source_payload = b"".join(source_port.read_range(source_request))
                destination_payload = b"".join(
                    destination_port.read_range(destination_request)
                )
                if len(source_payload) != length or len(destination_payload) != length:
                    raise RuntimeError("Simulated audit range ended early")
                source_digest.update(source_payload)
                destination_digest.update(destination_payload)
                obj.audit_progress_bytes = offset + length
                obj.audit_progress_at = utcnow()
                session.commit()
            obj.source_checksum = base64.b64encode(source_digest.digest()).decode("ascii")
            obj.destination_checksum = base64.b64encode(
                destination_digest.digest()
            ).decode("ascii")
            if obj.source_checksum != obj.destination_checksum:
                raise RuntimeError("SHA-256 source/destination mismatch")
            obj.integrity_error = None
            obj.integrity_verified_at = utcnow()
            obj.state = ObjectState.VERIFIED
        except Exception as error:
            obj.integrity_error = f"Simulated deep audit failed: {type(error).__name__}: {error}"
            obj.state = ObjectState.FAILED
            failed += 1
        obj.audit_progress_at, obj.audit_rate_mbps = utcnow(), 0
        session.commit()
    remaining = int(
        session.scalar(
            select(func.count(ObjectRecord.id)).where(
                ObjectRecord.wave_id == wave.id,
                ObjectRecord.state != ObjectState.VERIFIED,
            )
        )
        or 0
    )
    wave.status = "VERIFIED" if not remaining else "VERIFICATION_FAILED"
    event(
        session,
        "SIMULATED_INTEGRITY_VERIFICATION_COMPLETED",
        f"SIMULATED deep audit completed; {failed} failed and {remaining} remain pending",
        source_id=source.id,
        wave_id=wave.id,
    )
    succeed(session, task)


def task_kinds_for_role(role: str) -> frozenset[str] | None:
    if role in {"governance", "raikou"}:
        return GOVERNANCE_TASK_KINDS
    if role in {"transfer", "raiju"}:
        return TRANSFER_TASK_KINDS
    if role == "all":
        return None
    raise RuntimeError("RAIJIN_WORKER_ROLE must be raikou/governance, raiju/transfer, or all")


def run_once(role: str = WORKER_ROLE) -> None:
    from app.runtime_context import mode_switch_requested
    if mode_switch_requested():
        return
    if runtime_context.is_simulation:
        # Fail before touching the isolated control-plane schema unless the
        # simulator contract and all currently advertised operations are live.
        cloud_backend.readiness(require_operations=True)
    with SessionLocal() as session:
        settings = runtime_settings(session)
        allowed_task_kinds = task_kinds_for_role(role)
        if role in {"governance", "raikou", "all"}:
            if runtime_context.is_simulation:
                # Stop any clock created by an earlier release before planner
                # work can observe it. New releases create paused clocks, but
                # this safely migrates active scenarios without resetting
                # their virtual timeline.
                synchronize_simulation_source_clocks(session)
                session.commit()
            # Sources archived by an earlier release can still have READY
            # polling tasks. Reconcile them before any planner/claim action.
            if reconcile_archived_source_work(session):
                session.commit()
            if runtime_context.is_real:
                refresh_due_global_aws_pricing(session)
            replan_dynamic_pipeline(session, settings)
            for run in session.scalars(select(DynamicPipelineRun).where(
                DynamicPipelineRun.scheduled_restores.is_(True),
                DynamicPipelineRun.status.not_in(["COMPLETED", "HISTORICAL", "NEEDS_ATTENTION"]),
            )):
                materialize_dynamic_pipeline_horizon(session, settings, run)
            release_dynamic_restore_horizon(session, settings)
            # A completed restore may have been held behind an earlier
            # transfer at the exact moment its polling task finished.
            # Revisit durable RESTORED waves on every governance cycle so
            # the next one advances as soon as the lane is free.
            reconcile_restored_transfer_lane(session, settings)
            session.commit()
        # There is exactly one real worker per VM.  If it was interrupted while
        # discovery was running, its page checkpoint is already committed. Make
        # the source eligible again instead of leaving it permanently stuck in
        # DISCOVERING.  The unfinished active slice is intentionally not added
        # to elapsed time because a power loss makes its exact end unknowable.
        interrupted = list(session.scalars(select(Source).where(Source.status == "DISCOVERING"))) if role in {"governance", "raikou", "all"} else []
        for pending_source in interrupted:
            pending_source.status = "DISCOVERY_QUEUED"
            pending_source.discovery_started_at = None
            pending_job = session.scalar(select(DiscoveryJob).where(
                DiscoveryJob.source_id == pending_source.id,
                DiscoveryJob.state == TaskState.RUNNING,
            ).order_by(DiscoveryJob.id.desc()).limit(1))
            if pending_job:
                pending_job.state, pending_job.worker_id, pending_job.lease_expires_at = TaskState.READY, None, None
                pending_job.available_at = utcnow()
            else:
                session.add(DiscoveryJob(source_id=pending_source.id))
            event(session, "DISCOVERY_RECOVERED", "Discovery worker interruption recovered from durable page checkpoint", source_id=pending_source.id)
        if interrupted:
            session.commit()
        # Upgrade compatibility: sources queued by releases before the
        # discovery queue existed become visible jobs on the next worker loop.
        legacy_queued = list(session.scalars(select(Source).where(Source.status == "DISCOVERY_QUEUED"))) if role in {"governance", "raikou", "all"} else []
        for queued_source in legacy_queued:
            exists = session.scalar(select(DiscoveryJob.id).where(
                DiscoveryJob.source_id == queued_source.id,
                DiscoveryJob.state.in_([TaskState.READY, TaskState.RUNNING]),
            ).limit(1))
            if not exists:
                session.add(DiscoveryJob(source_id=queued_source.id))
        if legacy_queued:
            session.commit()
        discovery_job = claim_discovery_job(session, settings.task_lease_seconds) if role in {"governance", "raikou", "all"} else None
        if discovery_job:
            source = session.get(Source, discovery_job.source_id)
            if not source or source.status != "DISCOVERY_QUEUED":
                discovery_job.state, discovery_job.error, discovery_job.lease_expires_at, discovery_job.completed_at = TaskState.FAILED, "Source is no longer eligible for remote discovery", None, utcnow()
                session.commit()
                return
            try:
                if discovery_job.mode == "REMOTE_LIST":
                    discover(session, source, settings, discovery_job)
                elif discovery_job.mode == "S3_INVENTORY_MANIFEST":
                    import_s3_inventory_manifest(session, source, settings, discovery_job)
                else:
                    raise RuntimeError(f"Unsupported discovery job mode {discovery_job.mode}")
            except Exception as error:
                finished_at = utcnow()
                if source.discovery_started_at:
                    source.discovery_elapsed_seconds = float(source.discovery_elapsed_seconds or 0) + max(0, (finished_at - source.discovery_started_at).total_seconds())
                source.discovery_started_at = None
                source.status, source.discovery_error = "DISCOVERY_FAILED", str(error)[:8000]
                discovery_job.state, discovery_job.error, discovery_job.lease_expires_at, discovery_job.completed_at = TaskState.FAILED, source.discovery_error, None, finished_at
                event(session, "DISCOVERY_FAILED", source.discovery_error, source_id=source.id); session.commit()
            return
        if runtime_context.is_simulation:
            simulation_kinds = frozenset(
                {"SUBMIT_BATCH_RESTORE", "POLL_RESTORE", "TRANSFER_CONTINUOUS", "VERIFY_WAVE"}
            )
            allowed_task_kinds = (
                simulation_kinds
                if allowed_task_kinds is None
                else frozenset(allowed_task_kinds & simulation_kinds)
            )
        if runtime_context.is_simulation:
            synchronize_simulation_source_clocks(session)
            session.commit()
        task = claim_task(session, settings.task_lease_seconds, allowed_task_kinds)
        if not task:
            if runtime_context.is_simulation:
                synchronize_simulation_source_clocks(session)
                session.commit()
            return
        try:
            if task.kind == "SUBMIT_BATCH_RESTORE": submit_restore(session, task, settings)
            elif task.kind == "POLL_RESTORE": poll_restore(session, task, settings)
            elif task.kind == "TRANSFER_CONTINUOUS": transfer_continuous(session, task, settings)
            elif task.kind == "VERIFY_WAVE": verify_wave(session, task)
            else: raise RuntimeError(f"Unsupported real worker task {task.kind}")
        except Exception as error:
            disposition, summary = classify_task_error(error)
            if disposition == "retry":
                retry(
                    session,
                    task,
                    summary,
                    seconds=1 if isinstance(error, SimulatedNetworkRecoveryPending) else None,
                )
            else:
                fail_permanently(session, task, summary)
        finally:
            if runtime_context.is_simulation:
                synchronize_simulation_source_clocks(session)
                session.commit()


if __name__ == "__main__":
    failure_delay = 5
    while True:
        try:
            run_once()
            failure_delay = 5
        except Exception as error:
            # A transient database/network failure must not kill the durable
            # service. Task-level errors are persisted by run_once; this is a
            # last-resort guard for failures before a task can be claimed.
            print(f"real worker loop error: {type(error).__name__}: {error}", flush=True)
            time.sleep(failure_delay)
            failure_delay = min(60, failure_delay * 2)
            continue
        time.sleep(5)
