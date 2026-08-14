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
import socket
import time
from datetime import timedelta
from urllib.parse import quote

import boto3
import oci
from sqlalchemy import func, select

from app.main import (
    Event, ObjectRecord, ObjectState, SessionLocal, Source, Task, TaskState,
    Wave, read_oci_runtime_config, runtime_settings, utcnow,
)

WORKER_ID = f"aws-oci-worker-{socket.gethostname()}"
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


def claim_task(session, lease_seconds: int) -> Task | None:
    now = utcnow()
    available = (Task.state == TaskState.READY) | ((Task.state == TaskState.RUNNING) & (Task.lease_expires_at < now))
    task = session.scalar(select(Task).join(Wave).where(available, Task.available_at <= now, Wave.status != "PAUSED").order_by(Task.available_at, Task.id).with_for_update(skip_locked=True).limit(1))
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
        obj.state = ObjectState.RESTORE_REQUESTED
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


class HashingStream:
    def __init__(self, body, rate_bytes_per_second: float): self.body, self.digest, self.rate = body, hashlib.sha256(), rate_bytes_per_second
    def read(self, amount=-1):
        started = time.monotonic(); data = self.body.read(amount)
        if data:
            self.digest.update(data)
            if self.rate > 0:
                elapsed = time.monotonic() - started; minimum = len(data) / self.rate
                if minimum > elapsed: time.sleep(minimum - elapsed)
        return data


def transfer_wave(session, task: Task, settings) -> None:
    wave, source = session.get(Wave, task.wave_id), session.get(Wave, task.wave_id).source
    s3, _, _ = aws_clients(settings, source.aws_region)
    signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
    oci_client = oci.object_storage.ObjectStorageClient({}, signer=signer)
    namespace = read_oci_runtime_config().get("object_storage_namespace", "").strip()
    if not namespace:
        raise RuntimeError("OCI Object Storage namespace is absent from runtime configuration")
    rate = settings.max_throughput_mbps * 125000 / max(1, settings.transfer_workers)
    objects = list(session.scalars(select(ObjectRecord).where(ObjectRecord.wave_id == wave.id, ObjectRecord.state.in_([ObjectState.RESTORED, ObjectState.TRANSFERRING])).order_by(ObjectRecord.id)))
    for obj in objects:
        task.lease_expires_at = utcnow() + timedelta(seconds=settings.task_lease_seconds); session.commit()
        obj.state = ObjectState.TRANSFERRING; session.commit()
        args = {"Bucket": source.s3_bucket, "Key": obj.object_key}
        if obj.version_id: args["VersionId"] = obj.version_id
        head, tags = s3.head_object(**args), s3.get_object_tagging(**args).get("TagSet", [])
        body = s3.get_object(**args)["Body"]
        # OCI metadata is preserved where supported. S3 object tags have no
        # equivalent OCI tag API, so their complete evidence remains in the
        # control database and a bounded JSON copy is stored as metadata.
        metadata = {str(k).lower(): str(v) for k, v in head.get("Metadata", {}).items()}
        tags_json = json.dumps({tag["Key"]: tag["Value"] for tag in tags}, separators=(",", ":"), ensure_ascii=False)
        metadata["s3-oci-tags-json"] = tags_json[:1800]
        stream = HashingStream(body, rate)
        oci_client.put_object(namespace, source.destination_bucket, obj.object_key, stream, content_length=obj.size_bytes, content_type=head.get("ContentType"), opc_meta=metadata)
        checksum = stream.digest.hexdigest()
        destination_body = oci_client.get_object(namespace, source.destination_bucket, obj.object_key).data.raw
        destination_digest = hashlib.sha256()
        for chunk in iter(lambda: destination_body.read(8 * 1024 * 1024), b""):
            destination_digest.update(chunk)
        destination_checksum = destination_digest.hexdigest()
        obj.metadata_json, obj.tags_json = json.dumps(head.get("Metadata", {})), tags_json
        obj.source_checksum, obj.destination_checksum = checksum, destination_checksum
        # Transfer collects immutable checksum evidence, but does not compare it
        # or declare the object verified. That is an explicit operator action.
        obj.checksum_algorithm, obj.integrity_verified_at, obj.integrity_error, obj.state, obj.transferred_at = "SHA256", None, None, ObjectState.TRANSFERRED, utcnow()
        session.commit()
    remaining = session.scalar(select(func.count(ObjectRecord.id)).where(ObjectRecord.wave_id == wave.id, ObjectRecord.state != ObjectState.TRANSFERRED)) or 0
    wave.status = "TRANSFERRED" if not remaining else "TRANSFERRED_WITH_ERRORS"
    event(session, "TRANSFER_COMPLETED", f"Wave transfer completed; {remaining} object(s) pending or failed. Integrity verification awaits operator request.", source_id=source.id, wave_id=wave.id)
    succeed(session, task)


def verify_wave(session, task: Task) -> None:
    """Compare transfer evidence only after the operator has queued verification."""
    wave = session.get(Wave, task.wave_id)
    source = wave.source
    objects = list(session.scalars(select(ObjectRecord).where(
        ObjectRecord.wave_id == wave.id, ObjectRecord.state == ObjectState.TRANSFERRED
    ).order_by(ObjectRecord.id)))
    failed = 0
    for obj in objects:
        if not obj.source_checksum or not obj.destination_checksum:
            obj.integrity_error, obj.state = "SHA-256 evidence is missing after transfer", ObjectState.FAILED
            failed += 1
        elif obj.source_checksum != obj.destination_checksum:
            obj.integrity_error, obj.state = "SHA-256 source/destination mismatch", ObjectState.FAILED
            failed += 1
        else:
            obj.integrity_error, obj.integrity_verified_at, obj.state = None, utcnow(), ObjectState.VERIFIED
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
