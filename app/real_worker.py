"""Durable AWS S3 to OCI Object Storage worker.

It is deliberately separate from the API process. All external calls happen
only after durable state is committed, and all progress is represented by a
task lease or source discovery state so a VM restart is recoverable.
"""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import json
import os
import socket
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
from urllib.parse import quote

import boto3
import oci
from sqlalchemy import func, or_, select

from app.main import (
    Event, ObjectRecord, ObjectState, SessionLocal, Source, Task, TaskState,
    Wave, read_oci_runtime_config, runtime_settings, utcnow,
)

# The bootstrap supplies a stable identity for the one real worker on the VM.
# Keep the hostname fallback for local development/tests that do not use it.
WORKER_ID = os.getenv("RAIJIN_WORKER_ID", f"aws-oci-worker-{socket.gethostname()}")
ARCHIVE_CLASSES = {"GLACIER", "DEEP_ARCHIVE", "INTELLIGENT_TIERING_ARCHIVE_ACCESS", "INTELLIGENT_TIERING_DEEP_ARCHIVE_ACCESS"}


def event(session, kind: str, message: str, source_id: int | None = None, wave_id: int | None = None) -> None:
    session.add(Event(kind=kind, message=message, source_id=source_id, wave_id=wave_id))


def secret_values() -> dict[str, str]:
    config = read_oci_runtime_config()
    signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
    client = oci.secrets.SecretsClient({}, signer=signer)
    values = {}
    for name in ("aws_access_key_id", "aws_secret_access_key"):
        bundle = client.get_secret_bundle(config["secret_ocids"][name]).data
        values[name] = base64.b64decode(bundle.secret_bundle_content.content).decode().strip()
    return values


def aws_clients(settings, region: str):
    values = secret_values()
    bootstrap = boto3.Session(
        aws_access_key_id=values["aws_access_key_id"],
        aws_secret_access_key=values["aws_secret_access_key"],
        region_name=region,
    )
    assumed = bootstrap.client("sts").assume_role(
        RoleArn=settings.aws_migration_role_arn,
        RoleSessionName="s3-oci-migration-worker",
        DurationSeconds=3600,
    )["Credentials"]
    session = boto3.Session(
        aws_access_key_id=assumed["AccessKeyId"],
        aws_secret_access_key=assumed["SecretAccessKey"],
        aws_session_token=assumed["SessionToken"],
        region_name=region,
    )
    return session.client("s3"), session.client("s3control"), session.client("sts").get_caller_identity()["Account"]


def worker_can_reclaim_lease(task_worker_id: str | None, lease_expires_at, now) -> bool:
    """Whether this single-VM worker may recover an interrupted task now."""
    return task_worker_id == WORKER_ID or not lease_expires_at or lease_expires_at < now


def claim_task(session, lease_seconds: int) -> Task | None:
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
    if live_transfer:
        query = query.where(Task.kind != "TRANSFER_WAVE")
    task = session.scalar(query.order_by(Task.available_at, Task.id).with_for_update(skip_locked=True).limit(1))
    if not task:
        return None
    task.state, task.worker_id = TaskState.RUNNING, WORKER_ID
    task.attempts += 1
    task.lease_expires_at = now + timedelta(seconds=lease_seconds)
    event(session, "TASK_CLAIMED", f"Task {task.id} claimed by real worker", wave_id=task.wave_id)
    session.commit()
    return task


def succeed(session, task: Task, next_kind: str | None = None) -> None:
    if next_kind:
        session.add(Task(wave_id=task.wave_id, kind=next_kind))
    task.state, task.lease_expires_at, task.error = TaskState.SUCCEEDED, None, None
    event(session, "TASK_SUCCEEDED", f"Real worker completed {task.kind}", wave_id=task.wave_id)
    session.commit()


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


def discover(session, source: Source, settings) -> None:
    source.status, source.discovery_error = "DISCOVERING", None
    session.commit()
    s3, _, _ = aws_clients(settings, source.aws_region)
    paginator = s3.get_paginator("list_objects_v2")
    inserted = 0
    for page in paginator.paginate(Bucket=source.s3_bucket, Prefix=source.s3_prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            exists = session.scalar(select(ObjectRecord.id).where(ObjectRecord.source_id == source.id, ObjectRecord.object_key == key, ObjectRecord.version_id.is_(None)))
            if exists:
                continue
            session.add(ObjectRecord(source_id=source.id, object_key=key, size_bytes=item["Size"], etag=item.get("ETag", "").strip('"') or None, storage_class=item.get("StorageClass"), last_modified=item.get("LastModified")))
            inserted += 1
        session.commit()
    source.status, source.discovery_completed_at = "DISCOVERED", utcnow()
    event(session, "DISCOVERY_COMPLETED", f"Discovery inserted {inserted} object(s) using paginated ListObjectsV2 only", source_id=source.id)
    session.commit()


def archive_objects(session, wave_id: int) -> list[ObjectRecord]:
    return list(session.scalars(select(ObjectRecord).where(ObjectRecord.wave_id == wave_id, ObjectRecord.storage_class.in_(ARCHIVE_CLASSES), ObjectRecord.state == ObjectState.WAVE_ASSIGNED).order_by(ObjectRecord.object_key)))


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
    if not settings.aws_control_bucket or not settings.aws_batch_role_arn:
        raise RuntimeError("AWS control bucket and Batch Operations role ARN must be configured")
    s3, s3control, account_id = aws_clients(settings, source.aws_region)
    has_versions = any(obj.version_id for obj in archives)
    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    for obj in archives:
        row = [source.s3_bucket, quote(obj.object_key, safe="/")]
        if has_versions:
            row.append(obj.version_id or "")
        writer.writerow(row)
    manifest_key = f"{settings.aws_control_prefix.rstrip('/')}/manifests/wave-{wave.id}-{int(time.time())}.csv"
    response = s3.put_object(Bucket=settings.aws_control_bucket, Key=manifest_key, Body=output.getvalue().encode("utf-8"), ContentType="text/csv")
    fields = ["S3Bucket", "S3Key"] + (["S3VersionId"] if has_versions else [])
    job = s3control.create_job(
        AccountId=account_id, ConfirmationRequired=False, Priority=10, RoleArn=settings.aws_batch_role_arn,
        Operation={"S3InitiateRestoreObject": {"ExpirationInDays": wave.restore_days, "GlacierJobTier": wave.restore_tier}},
        Manifest={"Spec": {"Format": "S3BatchOperations_CSV_20180820", "Fields": fields}, "Location": {"ObjectArn": f"arn:aws:s3:::{settings.aws_control_bucket}/{manifest_key}", "ETag": response["ETag"].strip('"')}},
        Report={"Bucket": f"arn:aws:s3:::{settings.aws_control_bucket}", "Prefix": f"{settings.aws_control_prefix.rstrip('/')}/reports/wave-{wave.id}/", "Format": "Report_CSV_20180820", "Enabled": True, "ReportScope": "AllTasks"},
        Description=f"S3 to OCI restore wave {wave.id}",
        ClientRequestToken=f"s3-oci-wave-{wave.id}",
    )
    wave.batch_job_id, wave.manifest_key, wave.manifest_etag, wave.status = job["JobId"], manifest_key, response["ETag"].strip('"'), "RESTORE_REQUESTED"
    for obj in archives:
        obj.state, obj.restore_requested_at = ObjectState.RESTORE_REQUESTED, utcnow()
    event(session, "BATCH_RESTORE_SUBMITTED", f"Batch job {wave.batch_job_id} submitted for {len(archives)} archive object(s)", source_id=source.id, wave_id=wave.id)
    succeed(session, task, "POLL_RESTORE")


def poll_restore(session, task: Task, settings) -> None:
    wave, source = session.get(Wave, task.wave_id), session.get(Wave, task.wave_id).source
    s3, s3control, account_id = aws_clients(settings, source.aws_region)
    if wave.batch_job_id:
        job = s3control.describe_job(AccountId=account_id, JobId=wave.batch_job_id)["Job"]
        wave.last_poll_at, wave.poll_count = utcnow(), wave.poll_count + 1
        if job["Status"] not in {"Complete", "Completed"}:
            wave.status = "RESTORING"
            session.commit()
            retry(session, task, f"Batch job status is {job['Status']}", min(1800, 300 + wave.poll_count * 60))
            return
    ready_keys: set[str] = set()
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=source.s3_bucket, Prefix=source.s3_prefix, OptionalObjectAttributes=["RestoreStatus"]):
        for item in page.get("Contents", []):
            restore = item.get("RestoreStatus", {})
            if restore and not restore.get("IsRestoreInProgress") and restore.get("RestoreExpiryDate"):
                ready_keys.add(item["Key"])
    objects = list(session.scalars(select(ObjectRecord).where(ObjectRecord.wave_id == wave.id)))
    for obj in objects:
        if obj.storage_class not in ARCHIVE_CLASSES or obj.object_key in ready_keys:
            if obj.state in {ObjectState.RESTORE_REQUESTED, ObjectState.RESTORING, ObjectState.WAVE_ASSIGNED}:
                obj.state, obj.restored_at = ObjectState.RESTORED, obj.restored_at or utcnow()
        elif obj.state == ObjectState.RESTORE_REQUESTED:
            obj.state = ObjectState.RESTORING
    pending = session.scalar(select(func.count(ObjectRecord.id)).where(ObjectRecord.wave_id == wave.id, ObjectRecord.state.in_([ObjectState.RESTORE_REQUESTED, ObjectState.RESTORING]))) or 0
    wave.last_poll_at, wave.poll_count = utcnow(), wave.poll_count + 1
    if pending:
        wave.status = "RESTORING"
        session.commit()
        retry(session, task, f"{pending} object(s) still unavailable after restore", min(1800, 300 + wave.poll_count * 60))
        return
    wave.status = "RESTORED"
    event(session, "RESTORE_AVAILABLE", "All wave objects are available for transfer", source_id=source.id, wave_id=wave.id)
    succeed(session, task, "TRANSFER_WAVE")


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
    s3, _, _ = aws_clients(settings, source.aws_region)
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


def run_once() -> None:
    with SessionLocal() as session:
        settings = runtime_settings(session)
        if not settings.real_worker_enabled:
            return
        source = session.scalar(select(Source).where(Source.status == "DISCOVERY_QUEUED").order_by(Source.discovery_requested_at).with_for_update(skip_locked=True).limit(1))
        if source:
            try: discover(session, source, settings)
            except Exception as error:
                source.status, source.discovery_error = "DISCOVERY_FAILED", str(error)[:8000]; event(session, "DISCOVERY_FAILED", source.discovery_error, source_id=source.id); session.commit()
            return
        task = claim_task(session, settings.task_lease_seconds)
        if not task: return
        try:
            if task.kind == "SUBMIT_BATCH_RESTORE": submit_restore(session, task, settings)
            elif task.kind == "POLL_RESTORE": poll_restore(session, task, settings)
            elif task.kind == "TRANSFER_WAVE": transfer_wave(session, task, settings)
            elif task.kind == "VERIFY_WAVE": verify_wave(session, task)
            else: raise RuntimeError(f"Unsupported real worker task {task.kind}")
        except Exception as error: retry(session, task, error)


if __name__ == "__main__":
    while True:
        try: run_once()
        except Exception as error:
            # A transient database/network failure must not kill the durable
            # service. Task-level errors are persisted by run_once; this is a
            # last-resort guard for failures before a task can be claimed.
            print(f"real worker loop error: {type(error).__name__}: {error}", flush=True)
        time.sleep(5)
