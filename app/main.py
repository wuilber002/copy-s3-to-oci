from __future__ import annotations

"""Local control plane for a durable S3-to-OCI migration.

AWS and OCI transfer adapters are deliberately not invoked by API requests.
They are scheduled workers; this keeps configuration, inventory, waves and
leases durable even if a VM is restarted during a long restore.
"""

from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import Generator
import os
import csv
import io
import json
import base64
import shutil

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from sqlalchemy import BigInteger, DateTime, ForeignKey, Integer, String, Text, case, create_engine, func, inspect, select, text
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship, sessionmaker


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


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
    RESTORING = "RESTORING"
    RESTORED = "RESTORED"
    TRANSFERRING = "TRANSFERRING"
    VERIFIED = "VERIFIED"
    FAILED = "FAILED"


class TaskState(StrEnum):
    READY = "READY"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class Source(Base):
    __tablename__ = "sources"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(128), unique=True)
    s3_bucket: Mapped[str] = mapped_column(String(255))
    s3_prefix: Mapped[str] = mapped_column(String(1024), default="")
    aws_region: Mapped[str] = mapped_column(String(64))
    destination_bucket: Mapped[str] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(32), default="CONFIGURED")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    objects: Mapped[list[ObjectRecord]] = relationship(back_populates="source")
    waves: Mapped[list[Wave]] = relationship(back_populates="source")


class ObjectRecord(Base):
    __tablename__ = "objects"

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
    integrity_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    integrity_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    state: Mapped[str] = mapped_column(String(32), default=ObjectState.DISCOVERED)
    wave_id: Mapped[int | None] = mapped_column(ForeignKey("waves.id"), nullable=True, index=True)
    source: Mapped[Source] = relationship(back_populates="objects")
    wave: Mapped[Wave | None] = relationship(back_populates="objects")


class Wave(Base):
    __tablename__ = "waves"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"), index=True)
    name: Mapped[str] = mapped_column(String(128))
    max_bytes: Mapped[int] = mapped_column(BigInteger)
    restore_days: Mapped[int] = mapped_column(Integer)
    restore_tier: Mapped[str] = mapped_column(String(16))
    status: Mapped[str] = mapped_column(String(32), default="DRAFT")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    source: Mapped[Source] = relationship(back_populates="waves")
    objects: Mapped[list[ObjectRecord]] = relationship(back_populates="wave")
    tasks: Mapped[list[Task]] = relationship(back_populates="wave")


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
    default_wave_size_bytes: Mapped[int] = mapped_column(BigInteger, default=10 * 1024**4)
    default_restore_days: Mapped[int] = mapped_column(Integer, default=7)
    default_restore_tier: Mapped[str] = mapped_column(String(16), default="BULK")
    task_lease_seconds: Mapped[int] = mapped_column(Integer, default=300)
    simulation_enabled: Mapped[bool] = mapped_column(default=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class SourceCreate(BaseModel):
    name: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{1,127}$")
    s3_bucket: str
    s3_prefix: str = ""
    aws_region: str
    destination_bucket: str


class SourceUpdate(SourceCreate):
    pass


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
    default_wave_size_bytes: int = Field(gt=0, le=10 * 1024**4)
    default_restore_days: int = Field(ge=1, le=30)
    default_restore_tier: str = Field(pattern="^(BULK|STANDARD)$")
    task_lease_seconds: int = Field(ge=30, le=3600)
    simulation_enabled: bool = False


class SimulationTaskUpdate(BaseModel):
    worker_id: str = Field(min_length=1, max_length=128)


class WaveAction(BaseModel):
    reason: str = Field(min_length=3, max_length=1000)


class IntegrityEvidence(BaseModel):
    source_checksum: str | None = Field(default=None, max_length=256)
    destination_checksum: str | None = Field(default=None, max_length=256)
    checksum_algorithm: str = Field(pattern="^(SHA256|MD5)$")
    verified: bool
    error: str | None = Field(default=None, max_length=4000)


app = FastAPI(title="S3 to OCI Migration", version="0.3.0")
app.mount("/static", StaticFiles(directory="app/static"), name="static")


@app.on_event("startup")
def create_schema() -> None:
    Base.metadata.create_all(engine)
    # Lightweight additive migrations keep the single-VM PoC upgradeable. No
    # destructive schema operation is performed automatically.
    expected_columns = {
        "source_checksum": "VARCHAR(256)",
        "destination_checksum": "VARCHAR(256)",
        "checksum_algorithm": "VARCHAR(32)",
        "integrity_verified_at": "TIMESTAMP WITH TIME ZONE",
        "integrity_error": "TEXT",
    }
    existing_columns = {column["name"] for column in inspect(engine).get_columns("objects")}
    with engine.begin() as connection:
        for column, sql_type in expected_columns.items():
            if column not in existing_columns:
                connection.execute(text(f"ALTER TABLE objects ADD COLUMN {column} {sql_type}"))


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
            "default_wave_size_bytes": settings.default_wave_size_bytes, "default_restore_days": settings.default_restore_days,
            "default_restore_tier": settings.default_restore_tier, "task_lease_seconds": settings.task_lease_seconds,
            "simulation_enabled": settings.simulation_enabled, "updated_at": settings.updated_at}


def read_oci_runtime_config() -> dict:
    with open(oci_runtime_config_file, encoding="utf-8") as config_file:
        return json.load(config_file)


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


@app.get("/api/readiness")
def oci_readiness(session: Session = Depends(get_session)) -> dict:
    """Explicit OCI pre-check. It returns only readiness states, never secret values."""
    checks: list[dict] = []
    try:
        runtime_config = read_oci_runtime_config()
        secret_ocids = runtime_config.get("secret_ocids", {})
    except (FileNotFoundError, json.JSONDecodeError, OSError) as error:
        return {"ready": False, "checks": [{"name": "Configuração OCI", "status": "NOT_CONFIGURED", "detail": type(error).__name__}]}
    try:
        import oci
        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        secrets_client = oci.secrets.SecretsClient({}, signer=signer)
        object_storage_client = oci.object_storage.ObjectStorageClient({}, signer=signer)
    except Exception as error:  # SDK exposes several auth-specific exception types.
        return {"ready": False, "checks": [{"name": "Identidade dinâmica OCI", "status": "FAILED", "detail": type(error).__name__}]}

    expected_secrets = ["aws_access_key_id", "aws_secret_access_key", "aws_role_arn", "postgres_password"]
    for secret_name in expected_secrets:
        secret_ocid = secret_ocids.get(secret_name)
        if not secret_ocid:
            checks.append({"name": f"Secret {secret_name}", "status": "NOT_CONFIGURED", "detail": "OCID ausente"})
            continue
        try:
            bundle = secrets_client.get_secret_bundle(secret_ocid).data
            encoded_content = bundle.secret_bundle_content.content
            content = base64.b64decode(encoded_content).decode("utf-8").strip()
            status = "PLACEHOLDER" if content.startswith("REPLACE_THIS_PLACEHOLDER") else "READY"
            checks.append({"name": f"Secret {secret_name}", "status": status, "detail": "valor presente" if status == "READY" else "placeholder ainda não substituído"})
        except Exception as error:
            checks.append({"name": f"Secret {secret_name}", "status": "FAILED", "detail": type(error).__name__})

    try:
        namespace = object_storage_client.get_namespace().data
        checks.append({"name": "Namespace OCI Object Storage", "status": "READY", "detail": namespace})
        for bucket_name in sorted({source.destination_bucket for source in session.scalars(select(Source))}):
            try:
                object_storage_client.list_objects(namespace, bucket_name, limit=1)
                checks.append({"name": f"Bucket OCI {bucket_name}", "status": "READY", "detail": "leitura autorizada"})
            except Exception as error:
                checks.append({"name": f"Bucket OCI {bucket_name}", "status": "FAILED", "detail": type(error).__name__})
    except Exception as error:
        checks.append({"name": "OCI Object Storage", "status": "FAILED", "detail": type(error).__name__})
    ready = all(check["status"] == "READY" for check in checks)
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
    volume = shutil.disk_usage("/")
    return {
        "status": "ok",
        "time": utcnow(),
        "sources": source_count,
        "objects": object_count,
        "bytes": bytes_total,
        "tasks": task_counts,
        "disk": {"total": volume.total, "used": volume.used, "free": volume.free},
    }


@app.get("/api/settings")
def get_settings(session: Session = Depends(get_session)) -> dict:
    return settings_dict(runtime_settings(session))


@app.put("/api/settings")
def update_settings(payload: RuntimeSettingsUpdate, session: Session = Depends(get_session)) -> dict:
    settings = runtime_settings(session)
    for field, value in payload.model_dump().items():
        setattr(settings, field, value)
    record_event(session, "SETTINGS_UPDATED", "Operational transfer settings updated")
    session.commit()
    return settings_dict(settings)


@app.get("/api/sources")
def list_sources(session: Session = Depends(get_session)) -> list[dict]:
    return [{"id": s.id, "name": s.name, "s3_bucket": s.s3_bucket, "s3_prefix": s.s3_prefix,
             "aws_region": s.aws_region, "destination_bucket": s.destination_bucket, "status": s.status}
            for s in session.scalars(select(Source).order_by(Source.id))]


@app.post("/api/sources", status_code=201)
def create_source(payload: SourceCreate, session: Session = Depends(get_session)) -> dict:
    if session.scalar(select(Source).where(Source.name == payload.name)):
        raise HTTPException(status_code=409, detail="Source name already exists")
    source = Source(**payload.model_dump())
    session.add(source)
    session.flush()
    record_event(session, "SOURCE_CREATED", f"Source '{source.name}' configured", source_id=source.id)
    session.commit()
    return {"id": source.id, "name": source.name, "status": source.status}


@app.put("/api/sources/{source_id}")
def update_source(source_id: int, payload: SourceUpdate, session: Session = Depends(get_session)) -> dict:
    source = source_or_404(session, source_id)
    objects = session.scalar(select(func.count(ObjectRecord.id)).where(ObjectRecord.source_id == source_id)) or 0
    waves = session.scalar(select(func.count(Wave.id)).where(Wave.source_id == source_id)) or 0
    if objects or waves:
        raise HTTPException(status_code=409, detail="A source with inventory or waves is immutable to preserve auditability")
    duplicate = session.scalar(select(Source.id).where(Source.name == payload.name, Source.id != source_id))
    if duplicate:
        raise HTTPException(status_code=409, detail="Source name already exists")
    for field, value in payload.model_dump().items():
        setattr(source, field, value)
    record_event(session, "SOURCE_UPDATED", f"Source '{source.name}' configuration updated", source_id=source.id)
    session.commit()
    return {"id": source.id, "name": source.name, "status": source.status}


@app.post("/api/sources/{source_id}/inventory/import", status_code=201)
def import_inventory(source_id: int, payload: InventoryImport, session: Session = Depends(get_session)) -> dict:
    source_or_404(session, source_id)
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


@app.get("/api/sources/{source_id}/summary")
def source_summary(source_id: int, session: Session = Depends(get_session)) -> dict:
    source_or_404(session, source_id)
    count, bytes_total = session.execute(select(func.count(ObjectRecord.id), func.coalesce(func.sum(ObjectRecord.size_bytes), 0)).where(ObjectRecord.source_id == source_id)).one()
    states = dict(session.execute(
        select(ObjectRecord.state, func.count(ObjectRecord.id))
        .where(ObjectRecord.source_id == source_id)
        .group_by(ObjectRecord.state)
    ).all())
    return {"source_id": source_id, "objects": count, "bytes": bytes_total, "object_states": states}


@app.get("/api/sources/{source_id}/inventory")
def list_inventory(source_id: int, limit: int = 100, offset: int = 0, session: Session = Depends(get_session)) -> dict:
    source_or_404(session, source_id)
    limit = min(max(limit, 1), 1000)
    query = select(ObjectRecord).where(ObjectRecord.source_id == source_id).order_by(ObjectRecord.object_key).offset(offset).limit(limit)
    rows = session.scalars(query)
    total = session.scalar(select(func.count(ObjectRecord.id)).where(ObjectRecord.source_id == source_id)) or 0
    return {"items": [{"id": obj.id, "key": obj.object_key, "version_id": obj.version_id,
                       "size_bytes": obj.size_bytes, "storage_class": obj.storage_class, "state": obj.state,
                       "last_modified": obj.last_modified, "etag": obj.etag, "wave_id": obj.wave_id} for obj in rows],
            "limit": limit, "offset": offset, "total": total}


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
                          "error": obj.integrity_error}}


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


@app.post("/api/sources/{source_id}/waves", status_code=201)
def create_wave(source_id: int, payload: WaveCreate, session: Session = Depends(get_session)) -> dict:
    source_or_404(session, source_id)
    if session.scalar(select(Wave).where(Wave.source_id == source_id, Wave.name == payload.name)):
        raise HTTPException(status_code=409, detail="Wave name already exists for this source")
    wave = Wave(source_id=source_id, **payload.model_dump())
    session.add(wave)
    session.flush()
    remaining = payload.max_bytes
    objects = session.scalars(select(ObjectRecord).where(ObjectRecord.source_id == source_id, ObjectRecord.state == ObjectState.DISCOVERED).order_by(ObjectRecord.object_key).with_for_update(skip_locked=True))
    assigned = 0
    assigned_bytes = 0
    for obj in objects:
        if obj.size_bytes > remaining and assigned:
            break
        if obj.size_bytes > payload.max_bytes:
            continue
        obj.wave_id = wave.id
        obj.state = ObjectState.WAVE_ASSIGNED
        remaining -= obj.size_bytes
        assigned += 1
        assigned_bytes += obj.size_bytes
    if not assigned:
        session.rollback()
        raise HTTPException(status_code=409, detail="No discovered objects fit in this wave")
    wave.status = "READY_FOR_RESTORE"
    session.add(Task(wave_id=wave.id, kind="SUBMIT_BATCH_RESTORE"))
    record_event(session, "WAVE_CREATED", f"Wave '{wave.name}' created with {assigned} object(s) and {assigned_bytes} byte(s)", source_id=source_id, wave_id=wave.id)
    session.commit()
    return {"id": wave.id, "name": wave.name, "objects": assigned, "bytes": assigned_bytes, "status": wave.status}


@app.get("/api/sources/{source_id}/waves")
def list_waves(source_id: int, session: Session = Depends(get_session)) -> list[dict]:
    source_or_404(session, source_id)
    rows = session.execute(
        select(Wave, func.count(ObjectRecord.id), func.coalesce(func.sum(ObjectRecord.size_bytes), 0))
        .outerjoin(ObjectRecord, ObjectRecord.wave_id == Wave.id)
        .where(Wave.source_id == source_id)
        .group_by(Wave.id)
        .order_by(Wave.id)
    )
    return [{"id": wave.id, "name": wave.name, "status": wave.status, "restore_tier": wave.restore_tier,
             "restore_days": wave.restore_days, "objects": count, "bytes": size} for wave, count, size in rows]


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
    return {"wave_id": wave_id, "status": wave.status, "objects": total_objects, "bytes": total_bytes, "object_states": by_state,
            "integrity": {"verified": integrity_verified, "failed": integrity_failed, "pending": total_objects - integrity_verified - integrity_failed},
            "tasks": [{"id": t.id, "kind": t.kind, "state": t.state, "attempts": t.attempts, "error": t.error} for t in wave.tasks]}


@app.post("/api/waves/{wave_id}/pause")
def pause_wave(wave_id: int, payload: WaveAction, session: Session = Depends(get_session)) -> dict:
    wave = wave_or_404(session, wave_id)
    if wave.status == "PAUSED":
        return {"wave_id": wave.id, "status": wave.status}
    wave.status = "PAUSED"
    record_event(session, "WAVE_PAUSED", f"Wave '{wave.name}' paused: {payload.reason}", source_id=wave.source_id, wave_id=wave.id)
    session.commit()
    return {"wave_id": wave.id, "status": wave.status}


@app.post("/api/waves/{wave_id}/resume")
def resume_wave(wave_id: int, payload: WaveAction, session: Session = Depends(get_session)) -> dict:
    wave = wave_or_404(session, wave_id)
    if wave.status != "PAUSED":
        raise HTTPException(status_code=409, detail="Only a paused wave can be resumed")
    wave.status = "READY_FOR_RESTORE"
    queued = session.scalar(select(Task.id).where(Task.wave_id == wave.id, Task.state.in_([TaskState.READY, TaskState.RUNNING])))
    if not queued:
        session.add(Task(wave_id=wave.id, kind="SUBMIT_BATCH_RESTORE"))
    record_event(session, "WAVE_RESUMED", f"Wave '{wave.name}' resumed: {payload.reason}", source_id=wave.source_id, wave_id=wave.id)
    session.commit()
    return {"wave_id": wave.id, "status": wave.status}


@app.post("/api/waves/{wave_id}/reprocess")
def reprocess_wave(wave_id: int, payload: WaveAction, session: Session = Depends(get_session)) -> dict:
    """Queue a new controlled restore submission; no external call is made here."""
    wave = wave_or_404(session, wave_id)
    if wave.status == "PAUSED":
        raise HTTPException(status_code=409, detail="Resume the wave before reprocessing it")
    queued = session.scalar(select(Task.id).where(Task.wave_id == wave.id, Task.state.in_([TaskState.READY, TaskState.RUNNING])))
    if queued:
        raise HTTPException(status_code=409, detail="This wave already has a queued or running task")
    wave.status = "READY_FOR_RESTORE"
    session.add(Task(wave_id=wave.id, kind="SUBMIT_BATCH_RESTORE"))
    record_event(session, "WAVE_REPROCESS_QUEUED", f"New restore submission queued for wave '{wave.name}': {payload.reason}", source_id=wave.source_id, wave_id=wave.id)
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
    query = select(Event)
    if source_id is not None:
        query = query.where(Event.source_id == source_id)
    if wave_id is not None:
        query = query.where(Event.wave_id == wave_id)
    events = session.scalars(query.order_by(Event.created_at.desc(), Event.id.desc()).limit(limit))
    return [{"id": event.id, "kind": event.kind, "message": event.message, "source_id": event.source_id,
             "wave_id": event.wave_id, "created_at": event.created_at} for event in events]


@app.get("/api/events.csv")
def export_events(session: Session = Depends(get_session)) -> StreamingResponse:
    content = io.StringIO()
    writer = csv.writer(content, lineterminator="\n")
    writer.writerow(["id", "created_at", "kind", "source_id", "wave_id", "message"])
    for event in session.scalars(select(Event).order_by(Event.created_at.desc(), Event.id.desc())):
        writer.writerow([event.id, event.created_at.isoformat(), event.kind, event.source_id or "", event.wave_id or "", event.message])
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
    """Advance a task without any AWS/OCI call; restricted to explicit PoC mode."""
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
        wave.status = "RESTORED"
        next_kind = "TRANSFER_WAVE"
    elif task.kind == "TRANSFER_WAVE":
        for obj in objects:
            if obj.state == ObjectState.RESTORED:
                obj.state = ObjectState.VERIFIED
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
