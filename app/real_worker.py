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
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import quote, unquote, urlparse

import boto3
import oci
from botocore.config import Config
from botocore.exceptions import ClientError
from sqlalchemy import func, or_, select

from app.main import (
    AwsConnection, DiscoveryJob, Event, ObjectRecord, ObjectState, RestoreAttempt, RestoreObjectResult, SessionLocal, Source, Task, TaskState,
    Wave, parse_aws_connection_payload, read_oci_runtime_config, refresh_due_global_aws_pricing, restore_availability_poll_delay_seconds, runtime_settings, utcnow,
)

# The bootstrap supplies a stable identity for the one real worker on the VM.
# Keep the hostname fallback for local development/tests that do not use it.
WORKER_ID = os.getenv("RAIJIN_WORKER_ID", f"aws-oci-worker-{socket.gethostname()}")
# The two production workers share PostgreSQL's durable queue but never claim
# each other's responsibility. ``all`` preserves a simple local invocation.
WORKER_ROLE = os.getenv("RAIJIN_WORKER_ROLE", "all").strip().lower()
GOVERNANCE_TASK_KINDS = frozenset({"SUBMIT_BATCH_RESTORE", "POLL_RESTORE", "VERIFY_WAVE"})
TRANSFER_TASK_KINDS = frozenset({"TRANSFER_WAVE"})
ARCHIVE_CLASSES = {"GLACIER", "DEEP_ARCHIVE", "INTELLIGENT_TIERING_ARCHIVE_ACCESS", "INTELLIGENT_TIERING_DEEP_ARCHIVE_ACCESS"}
# A ListObjectsV2 page contains up to 1,000 objects.  A LIST request is
# usually about 12.5x the unit price of a HEAD/GET request, so use targeted
# HeadObject checks only when the archive subset is materially smaller than
# the source scan.  The conservative factor of 10 also minimizes requests.
RESTORE_POLL_HEAD_TO_LIST_RATIO = 10
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
    if code in TRANSIENT_AWS_CODES or status in {429, 500, 502, 503, 504}:
        return "retry", summary
    if code in PERMANENT_AWS_CODES or (isinstance(status, int) and 400 <= status < 500):
        return "failed", summary
    if name in {"EndpointConnectionError", "ConnectTimeoutError", "ReadTimeoutError", "ConnectionClosedError"}:
        return "retry", summary
    return "failed", summary


def event(session, kind: str, message: str, source_id: int | None = None, wave_id: int | None = None) -> None:
    session.add(Event(kind=kind, message=message, source_id=source_id, wave_id=wave_id))


def connection_values(connection: AwsConnection) -> dict[str, str]:
    """Read the current connection payload; credentials never enter PostgreSQL."""
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
    # A wave owns one durable transfer task, but a process restart can leave a
    # still-valid lease behind. Never claim another transfer task while one is
    # live, even if other task kinds remain eligible in the queue.
    live_transfer = session.scalar(select(Task.id).where(
        Task.kind == "TRANSFER_WAVE", Task.state == TaskState.RUNNING,
        Task.lease_expires_at >= now,
    ).limit(1))
    query = select(Task).join(Wave).where(available, Task.available_at <= now, Wave.status != "PAUSED")
    if allowed_kinds is not None:
        query = query.where(Task.kind.in_(allowed_kinds))
    if live_transfer:
        query = query.where(Task.kind != "TRANSFER_WAVE")
    task = session.scalar(query.order_by(Task.available_at, Task.id).with_for_update(skip_locked=True).limit(1))
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


def ensure_transfer_task(session, wave: Wave) -> bool:
    """Ensure one eligible transfer task exists while restore polling continues."""
    existing = session.scalar(select(Task.id).where(
        Task.wave_id == wave.id,
        Task.kind == "TRANSFER_WAVE",
        Task.state.in_([TaskState.READY, TaskState.RUNNING]),
    ).limit(1))
    if existing:
        return False
    session.add(Task(wave_id=wave.id, kind="TRANSFER_WAVE"))
    event(session, "TRANSFER_RELEASED", "Transfer task released for restored objects", source_id=wave.source_id, wave_id=wave.id)
    return True


def retry(session, task: Task, error: Exception | str, seconds: int | None = None) -> None:
    delay = seconds if seconds is not None else min(1800, max(60, task.attempts * 60))
    task.state, task.lease_expires_at = TaskState.READY, None
    task.available_at = utcnow() + timedelta(seconds=delay)
    task.error = str(error)[:8000]
    if task.kind == "TRANSFER_WAVE":
        wave = session.get(Wave, task.wave_id)
        if wave and wave.status == "TRANSFERRING":
            wave.status = "RESTORED"
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
        if attempt and attempt.job_id:
            attempt.failure_summary = (
                "Raijin polling/report processing failed after AWS accepted "
                f"Batch job {attempt.job_id}: {task.error}"
            )[:8000]
    if wave and wave.status not in {"PAUSED", "VERIFIED", "RESTORE_REQUEST_FAILED"}:
        wave.status = "FAILED"
    event(session, "TASK_FAILED_PERMANENTLY", f"{task.kind} requires operator action: {task.error}", wave_id=task.wave_id)
    session.commit()


def discover(session, source: Source, settings, job: DiscoveryJob | None = None) -> None:
    source.status, source.discovery_error = "DISCOVERING", None
    source.discovery_started_at = utcnow()
    session.commit()
    remote_discover(session, source, settings, job)


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
                if key and (not source.s3_prefix or key.startswith(source.s3_prefix)):
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
    job.state, job.error, job.lease_expires_at, job.completed_at = TaskState.SUCCEEDED, None, None, finished_at
    event(session, "INVENTORY_MANIFEST_IMPORTED", f"S3 Inventory manifest imported {inserted} object(s) in this run; {source.discovery_objects_inserted} total object(s) across {source.discovery_pages_completed} shard(s)", source_id=source.id)
    session.commit()


def remote_discover(session, source: Source, settings, job: DiscoveryJob | None = None) -> None:
    s3, _, _ = aws_clients(settings, source.aws_region, source)
    inserted = 0
    continuation_token = source.discovery_continuation_token
    pending_rows: list[dict] = []
    pending_pages = 0
    pending_token = continuation_token
    next_request_at = 0.0

    def checkpoint_batch() -> None:
        """Atomically persist a bounded discovery batch and its S3 cursor."""
        nonlocal inserted, pending_pages
        if not pending_pages:
            return
        if pending_rows:
            # Mappings bypass the ORM identity map, keeping memory bounded even
            # when a bucket contains tens of millions of objects.
            session.bulk_insert_mappings(ObjectRecord, pending_rows)
        inserted += len(pending_rows)
        source.discovery_pages_completed += pending_pages
        source.discovery_objects_inserted += len(pending_rows)
        source.discovery_continuation_token = pending_token
        if job:
            job.lease_expires_at = utcnow() + timedelta(seconds=max(settings.task_lease_seconds, 300))
        session.commit()
        pending_rows.clear()
        pending_pages = 0

    while True:
        request = {"Bucket": source.s3_bucket, "Prefix": source.s3_prefix, "MaxKeys": DISCOVERY_MAX_KEYS}
        if continuation_token:
            request["ContinuationToken"] = continuation_token
        throttle_attempt = 0
        while True:
            wait = next_request_at - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            try:
                page = s3.list_objects_v2(**request)
                next_request_at = time.monotonic() + DISCOVERY_REQUEST_INTERVAL_SECONDS
                break
            except ClientError as error:
                code = (error.response.get("Error") or {}).get("Code")
                if code not in {"SlowDown", "Throttling", "ThrottlingException", "RequestLimitExceeded"} or throttle_attempt >= DISCOVERY_MAX_THROTTLE_RETRIES:
                    raise
                throttle_attempt += 1
                # Do not checkpoint ahead of S3. The next request keeps the
                # same continuation token, so this retry cannot skip keys.
                time.sleep(min(30, 2 ** throttle_attempt))
                next_request_at = time.monotonic() + DISCOVERY_REQUEST_INTERVAL_SECONDS
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
        pending_token = continuation_token
        if pending_pages >= DISCOVERY_CHECKPOINT_PAGES or not continuation_token:
            checkpoint_batch()
        if not continuation_token:
            break
    finished_at = utcnow()
    if source.discovery_started_at:
        source.discovery_elapsed_seconds = float(source.discovery_elapsed_seconds or 0) + max(0, (finished_at - source.discovery_started_at).total_seconds())
    source.discovery_started_at = None
    source.status, source.discovery_completed_at = "DISCOVERED", finished_at
    if job:
        job.state, job.error, job.lease_expires_at, job.completed_at = TaskState.SUCCEEDED, None, None, finished_at
    event(session, "DISCOVERY_COMPLETED", f"Discovery inserted {inserted} object(s) in this run; {source.discovery_objects_inserted} total object(s) across {source.discovery_pages_completed} ListObjectsV2 page(s)", source_id=source.id)
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
    """Choose the cheaper, lower-request restore readiness strategy."""
    list_pages = max(1, math.ceil(max(0, source_objects) / 1000))
    return wave_archive_objects <= list_pages * RESTORE_POLL_HEAD_TO_LIST_RATIO


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


def import_completion_report(session, s3, operation: dict[str, str], attempt: RestoreAttempt, objects: list[ObjectRecord]) -> bool:
    """Import per-object S3 Batch evidence. False means AWS has not published it yet."""
    prefix = f"{operation['control_prefix'].rstrip('/')}/reports/wave-{attempt.wave_id}/attempt-{attempt.id}/"
    keys = [entry["Key"] for page in s3.get_paginator("list_objects_v2").paginate(Bucket=operation["control_bucket"], Prefix=prefix) for entry in page.get("Contents", [])]
    manifest_key = next((key for key in keys if key.endswith("manifest.json")), None)
    if not manifest_key:
        return False
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
        report = s3.get_object(Bucket=result.get("Bucket", operation["control_bucket"]), Key=report_key)
        report_etag = report.get("ETag", "").strip('"') or None
        for row in csv.reader(io.StringIO(report["Body"].read().decode("utf-8", "replace"))):
            if len(row) < 7:
                continue
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
    return True


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
        succeed(session, task, "TRANSFER_WAVE")
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
            ClientRequestToken=f"s3-oci-wave-{wave.id}",
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


def poll_restore(session, task: Task, settings) -> None:
    wave, source = session.get(Wave, task.wave_id), session.get(Wave, task.wave_id).source
    # Keep the control-bucket layout in sync with submission.  The completion
    # report lives under the source-specific Raijin prefix, not the legacy
    # s3-oci-control prefix.
    operation = aws_operation_config(source, settings)
    s3, s3control, account_id = aws_clients(settings, source.aws_region, source)
    if wave.batch_job_id:
        attempt = restore_attempt_for_job(session, wave, source)
        job = s3control.describe_job(AccountId=account_id, JobId=wave.batch_job_id)["Job"]
        attempt.job_status = wave.batch_job_status = job.get("Status")
        wave.last_poll_at, wave.poll_count = utcnow(), wave.poll_count + 1
        if job["Status"] not in {"Complete", "Completed"}:
            if job["Status"] in {"Failed", "Cancelled", "Canceled"}:
                fail_restore_attempt(session, task, wave, attempt, f"Batch job {wave.batch_job_id} ended as {job['Status']}")
                return
            wave.status = "RESTORING"
            session.commit()
            retry(session, task, f"Batch job status is {job['Status']}", min(1800, 300 + wave.poll_count * 60))
            return
        progress = job.get("ProgressSummary", {}) or {}
        attempt.succeeded_objects = int(progress.get("NumberOfTasksSucceeded") or 0)
        attempt.failed_objects = int(progress.get("NumberOfTasksFailed") or 0)
        total = int(progress.get("TotalNumberOfTasks") or 0)
        if total != attempt.expected_objects or attempt.failed_objects or attempt.succeeded_objects != attempt.expected_objects:
            message = f"Batch job completed with {attempt.succeeded_objects}/{attempt.expected_objects} succeeded and {attempt.failed_objects} failed"
            if attempt.failed_objects == attempt.expected_objects:
                for result in session.scalars(select(RestoreObjectResult).where(RestoreObjectResult.attempt_id == attempt.id)):
                    result.task_status, result.error_message = "FAILED", "Batch job completed with no successful restore requests; completion report unavailable"
            fail_restore_attempt(session, task, wave, attempt, message)
            return
        objects = list(session.scalars(select(ObjectRecord).where(ObjectRecord.wave_id == wave.id)))
        try:
            report_available = import_completion_report(session, s3, operation, attempt, objects)
        except Exception as error:
            _, summary = classify_task_error(error)
            raise RuntimeError(f"S3 Batch completion-report processing failed: {summary}") from error
        if not report_available:
            wave.status = "RESTORE_REQUESTED"
            session.commit()
            retry(session, task, "Batch job accepted all objects; waiting for completion report evidence", 60)
            return
        failed_results = session.scalar(select(func.count(RestoreObjectResult.id)).where(RestoreObjectResult.attempt_id == attempt.id, RestoreObjectResult.task_status != "SUCCEEDED")) or 0
        if failed_results:
            fail_restore_attempt(session, task, wave, attempt, f"Completion report contains {failed_results} failed or unknown restore request(s)")
            return
        attempt.completed_at = utcnow()
        for obj in objects:
            if obj.storage_class in ARCHIVE_CLASSES and obj.state in {ObjectState.RESTORE_REQUESTED, ObjectState.RESTORING}:
                obj.state = ObjectState.RESTORE_REQUEST_ACCEPTED
        wave.status = "RESTORE_REQUEST_ACCEPTED"
        event(session, "RESTORE_REQUEST_ACCEPTED", f"Batch job {wave.batch_job_id} accepted all {attempt.expected_objects} archive restore request(s) with per-object evidence", source_id=source.id, wave_id=wave.id)
        session.commit()
    objects = list(session.scalars(select(ObjectRecord).where(ObjectRecord.wave_id == wave.id)))
    archives = [obj for obj in objects if obj.storage_class in ARCHIVE_CLASSES]
    source_object_count = session.scalar(select(func.count(ObjectRecord.id)).where(ObjectRecord.source_id == source.id)) or 0
    # The availability/expiry values are collected from the same polling
    # response already needed to decide readiness.  This adds no AWS requests.
    ready_expiries: dict[str, object] = {}
    if should_poll_restore_with_head(len(archives), int(source_object_count)):
        poll_method = "HeadObject"
        for obj in archives:
            arguments = {"Bucket": source.s3_bucket, "Key": obj.object_key}
            if obj.version_id:
                arguments["VersionId"] = obj.version_id
            response = s3.head_object(**arguments)
            if restored_from_head_response(response):
                ready_expiries[obj.object_key] = restore_expiry_from_head_response(response)
    else:
        poll_method = "ListObjectsV2"
        for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=source.s3_bucket, Prefix=source.s3_prefix, OptionalObjectAttributes=["RestoreStatus"]
        ):
            for item in page.get("Contents", []):
                restore = item.get("RestoreStatus", {})
                if restore and not restore.get("IsRestoreInProgress") and restore.get("RestoreExpiryDate"):
                    expiry = restore["RestoreExpiryDate"]
                    ready_expiries[item["Key"]] = expiry.astimezone(timezone.utc) if getattr(expiry, "tzinfo", None) else expiry.replace(tzinfo=timezone.utc)
    for obj in objects:
        if obj.storage_class not in ARCHIVE_CLASSES or obj.object_key in ready_expiries:
            if obj.state in {ObjectState.RESTORE_REQUESTED, ObjectState.RESTORE_REQUEST_ACCEPTED, ObjectState.RESTORING, ObjectState.WAVE_ASSIGNED}:
                obj.state, obj.restored_at = ObjectState.RESTORED, obj.restored_at or utcnow()
                if obj.storage_class in ARCHIVE_CLASSES:
                    obj.restore_expires_at = ready_expiries.get(obj.object_key)
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
        if source.transfer_strategy == "AS_OBJECTS_AVAILABLE" and ready_for_transfer:
            ensure_transfer_task(session, wave)
            event(session, "RESTORE_PARTIALLY_AVAILABLE", f"{ready_for_transfer} restored object(s) released for transfer while {pending} remain unavailable", source_id=source.id, wave_id=wave.id)
        session.commit()
        delay = restore_availability_poll_delay_seconds(
            attempt.completed_at if wave.batch_job_id else None,
            utcnow(),
            wave.restore_tier,
            partial_availability=bool(ready_for_transfer),
        )
        retry(session, task, f"{pending} object(s) still unavailable after restore; checked with {poll_method}; next availability poll in {delay // 60} minutes", delay)
        return
    wave.status = "RESTORED"
    expiries = [expiry for expiry in ready_expiries.values() if expiry]
    expiry_text = f"; earliest temporary-copy expiry {min(expiries).isoformat()}" if expiries else ""
    event(session, "RESTORE_AVAILABLE", f"All wave objects are available for transfer; readiness checked with {poll_method}{expiry_text}", source_id=source.id, wave_id=wave.id)
    ensure_transfer_task(session, wave)
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


def transfer_wave(session, task: Task, settings) -> None:
    wave, source = session.get(Wave, task.wave_id), session.get(Wave, task.wave_id).source
    # Reflect the claimed transfer immediately.  Establishing AWS clients can
    # take a few seconds (or retry credentials), and leaving the wave as
    # READY_FOR_RESTORE during that interval is misleading for Standard data,
    # which never needs a restore request.
    wave.status = "TRANSFERRING"
    session.commit()
    s3, _, _ = aws_clients(settings, source.aws_region, source)
    namespace = read_oci_runtime_config().get("object_storage_namespace", "").strip()
    if not namespace:
        raise RuntimeError("OCI Object Storage namespace is absent from runtime configuration")

    # The wave task remains exclusive: file workers only parallelize objects
    # inside it. Settings are reloaded before each batch, so changing workers
    # or the aggregate throughput in the UI takes effect without a restart.
    while True:
        session.expire_all()
        live_settings = runtime_settings(session)
        worker_count = max(1, live_settings.transfer_workers)
        object_ids = list(session.scalars(select(ObjectRecord.id).where(
            ObjectRecord.wave_id == wave.id,
            ObjectRecord.state.in_([ObjectState.RESTORED, ObjectState.TRANSFERRING]),
        ).order_by(ObjectRecord.id).limit(worker_count)))
        if not object_ids:
            break
        task.lease_expires_at = utcnow() + timedelta(seconds=live_settings.task_lease_seconds)
        session.commit()
        rate = live_settings.max_throughput_mbps * 125000 / worker_count
        multipart_part_size = live_settings.multipart_part_size_mib * 1024 * 1024
        errors: list[str] = []
        with ThreadPoolExecutor(max_workers=len(object_ids), thread_name_prefix="s3-oci-transfer") as executor:
            futures = [executor.submit(transfer_object, s3, namespace, source.s3_bucket,
                                       source.destination_bucket, object_id, rate, live_settings.preserve_s3_tags, multipart_part_size)
                       for object_id in object_ids]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as error:
                    errors.append(f"{type(error).__name__}: {error}")
        if errors:
            raise RuntimeError(f"{len(errors)} object transfer(s) failed in parallel batch: {errors[0]}")

    session.expire_all()
    remaining = session.scalar(select(func.count(ObjectRecord.id)).where(
        ObjectRecord.wave_id == wave.id, ObjectRecord.state != ObjectState.TRANSFERRED
    )) or 0
    wave = session.get(Wave, task.wave_id)
    delivery_pending = session.scalar(select(func.count(ObjectRecord.id)).where(
        ObjectRecord.wave_id == wave.id,
        or_(ObjectRecord.delivery_integrity_status.is_(None), ObjectRecord.delivery_integrity_status != "OCI_ACCEPTED"),
    )) or 0
    waiting_restore = session.scalar(select(func.count(ObjectRecord.id)).where(
        ObjectRecord.wave_id == wave.id,
        ObjectRecord.state.in_([ObjectState.RESTORE_REQUESTED, ObjectState.RESTORE_REQUEST_ACCEPTED, ObjectState.RESTORING]),
    )) or 0
    if source.transfer_strategy == "AS_OBJECTS_AVAILABLE" and waiting_restore:
        wave.status = "RESTORING"
        event(session, "PARTIAL_TRANSFER_COMPLETED", f"Transferred currently available objects; {waiting_restore} object(s) still await restore.", source_id=source.id, wave_id=wave.id)
    else:
        wave.status = "COMPLETED" if not remaining and not delivery_pending else "TRANSFERRED_WITH_ERRORS"
        event(session, "TRANSFER_COMPLETED", f"Wave transfer completed; {remaining} object(s) pending or failed and {delivery_pending} object(s) without OCI cryptographic delivery evidence.", source_id=source.id, wave_id=wave.id)
    succeed(session, task)


def verify_wave(session, task: Task) -> None:
    """Read OCI objects and compare them to the SHA-256 evidence from S3."""
    wave = session.get(Wave, task.wave_id)
    source = wave.source
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


def task_kinds_for_role(role: str) -> frozenset[str] | None:
    if role == "governance":
        return GOVERNANCE_TASK_KINDS
    if role == "transfer":
        return TRANSFER_TASK_KINDS
    if role == "all":
        return None
    raise RuntimeError("RAIJIN_WORKER_ROLE must be governance, transfer, or all")


def run_once(role: str = WORKER_ROLE) -> None:
    with SessionLocal() as session:
        settings = runtime_settings(session)
        if not settings.real_worker_enabled:
            return
        allowed_task_kinds = task_kinds_for_role(role)
        if role in {"governance", "all"}:
            refresh_due_global_aws_pricing(session)
        # There is exactly one real worker per VM.  If it was interrupted while
        # discovery was running, its page checkpoint is already committed. Make
        # the source eligible again instead of leaving it permanently stuck in
        # DISCOVERING.  The unfinished active slice is intentionally not added
        # to elapsed time because a power loss makes its exact end unknowable.
        interrupted = list(session.scalars(select(Source).where(Source.status == "DISCOVERING"))) if role in {"governance", "all"} else []
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
        legacy_queued = list(session.scalars(select(Source).where(Source.status == "DISCOVERY_QUEUED"))) if role in {"governance", "all"} else []
        for queued_source in legacy_queued:
            exists = session.scalar(select(DiscoveryJob.id).where(
                DiscoveryJob.source_id == queued_source.id,
                DiscoveryJob.state.in_([TaskState.READY, TaskState.RUNNING]),
            ).limit(1))
            if not exists:
                session.add(DiscoveryJob(source_id=queued_source.id))
        if legacy_queued:
            session.commit()
        discovery_job = claim_discovery_job(session, settings.task_lease_seconds) if role in {"governance", "all"} else None
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
        task = claim_task(session, settings.task_lease_seconds, allowed_task_kinds)
        if not task: return
        try:
            if task.kind == "SUBMIT_BATCH_RESTORE": submit_restore(session, task, settings)
            elif task.kind == "POLL_RESTORE": poll_restore(session, task, settings)
            elif task.kind == "TRANSFER_WAVE": transfer_wave(session, task, settings)
            elif task.kind == "VERIFY_WAVE": verify_wave(session, task)
            else: raise RuntimeError(f"Unsupported real worker task {task.kind}")
        except Exception as error:
            disposition, summary = classify_task_error(error)
            if disposition == "retry":
                retry(session, task, summary)
            else:
                fail_permanently(session, task, summary)


if __name__ == "__main__":
    while True:
        try: run_once()
        except Exception as error:
            # A transient database/network failure must not kill the durable
            # service. Task-level errors are persisted by run_once; this is a
            # last-resort guard for failures before a task can be claimed.
            print(f"real worker loop error: {type(error).__name__}: {error}", flush=True)
        time.sleep(5)
