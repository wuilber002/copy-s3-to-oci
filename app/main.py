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
import re
import shutil

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from sqlalchemy import BigInteger, DateTime, Float, ForeignKey, Integer, String, Text, and_, case, create_engine, func, inspect, or_, select, text
from sqlalchemy.engine import URL
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, aliased, mapped_column, relationship, sessionmaker


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


class Source(Base):
    __tablename__ = "sources"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(128), unique=True)
    s3_bucket: Mapped[str] = mapped_column(String(255))
    s3_prefix: Mapped[str] = mapped_column(String(1024), default="")
    aws_region: Mapped[str] = mapped_column(String(64))
    destination_bucket: Mapped[str] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(32), default="CONFIGURED")
    discovery_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    discovery_completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    discovery_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    archived_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    destination_validation_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    destination_validation_status: Mapped[str | None] = mapped_column(String(32), nullable=True)
    destination_missing_count: Mapped[int] = mapped_column(Integer, default=0)
    destination_size_mismatch_count: Mapped[int] = mapped_column(Integer, default=0)
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
    restored_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    transferred_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    transfer_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    transfer_progress_bytes: Mapped[int] = mapped_column(BigInteger, default=0)
    transfer_progress_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    transfer_rate_mbps: Mapped[float] = mapped_column(Float, default=0)
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
    manifest_key: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    manifest_etag: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_poll_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    poll_count: Mapped[int] = mapped_column(Integer, default=0)
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
    aws_migration_role_arn: Mapped[str] = mapped_column(String(2048), default="")
    aws_batch_role_arn: Mapped[str] = mapped_column(String(2048), default="")
    aws_control_bucket: Mapped[str] = mapped_column(String(255), default="")
    aws_control_prefix: Mapped[str] = mapped_column(String(1024), default="s3-oci-control/")
    preserve_s3_tags: Mapped[bool] = mapped_column(default=True)
    real_worker_enabled: Mapped[bool] = mapped_column(default=False)
    activity_auto_refresh_enabled: Mapped[bool] = mapped_column(default=True)
    activity_refresh_seconds: Mapped[int] = mapped_column(Integer, default=15)
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


class AutomaticWaveCreate(BaseModel):
    max_bytes: int = Field(gt=0, le=10 * 1024**4)
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
    transfer_workers: int = Field(ge=1, le=64)
    max_throughput_mbps: int = Field(ge=1, le=1200)
    default_wave_size_bytes: int = Field(gt=0, le=10 * 1024**4)
    default_restore_days: int = Field(ge=1, le=30)
    default_restore_tier: str = Field(pattern="^(BULK|STANDARD)$")
    task_lease_seconds: int = Field(ge=30, le=3600)
    simulation_enabled: bool = False
    aws_migration_role_arn: str = Field(default="", max_length=2048, pattern=r"^$|^arn:(aws|aws-us-gov|aws-cn):iam::[0-9]{12}:role/.+")
    aws_batch_role_arn: str = Field(default="", max_length=2048, pattern=r"^$|^arn:(aws|aws-us-gov|aws-cn):iam::[0-9]{12}:role/.+")
    aws_control_bucket: str = Field(default="", max_length=255, pattern=r"^$|^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
    aws_control_prefix: str = Field(default="s3-oci-control/", max_length=1024)
    preserve_s3_tags: bool = True
    real_worker_enabled: bool = False


class ActivityRefreshSettingsUpdate(BaseModel):
    enabled: bool
    seconds: int = Field(ge=5, le=300)


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
        "restored_at": "TIMESTAMP WITH TIME ZONE",
        "transferred_at": "TIMESTAMP WITH TIME ZONE",
        "transfer_started_at": "TIMESTAMP WITH TIME ZONE",
        "transfer_progress_bytes": "BIGINT NOT NULL DEFAULT 0",
        "transfer_progress_at": "TIMESTAMP WITH TIME ZONE",
        "transfer_rate_mbps": "DOUBLE PRECISION NOT NULL DEFAULT 0",
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
        "activity_auto_refresh_enabled": "BOOLEAN NOT NULL DEFAULT TRUE",
        "activity_refresh_seconds": "INTEGER NOT NULL DEFAULT 15",
    }
    source_columns = {"discovery_requested_at": "TIMESTAMP WITH TIME ZONE", "discovery_completed_at": "TIMESTAMP WITH TIME ZONE", "discovery_error": "TEXT"}
    source_columns["archived_at"] = "TIMESTAMP WITH TIME ZONE"
    source_columns.update({"destination_validation_at": "TIMESTAMP WITH TIME ZONE", "destination_validation_status": "VARCHAR(32)", "destination_missing_count": "INTEGER NOT NULL DEFAULT 0", "destination_size_mismatch_count": "INTEGER NOT NULL DEFAULT 0"})
    wave_columns = {"batch_job_id": "VARCHAR(128)", "manifest_key": "VARCHAR(2048)", "manifest_etag": "VARCHAR(128)", "last_poll_at": "TIMESTAMP WITH TIME ZONE", "poll_count": "INTEGER NOT NULL DEFAULT 0"}
    existing_runtime_columns = {column["name"] for column in inspect(engine).get_columns("runtime_settings")}
    existing_source_columns = {column["name"] for column in inspect(engine).get_columns("sources")}
    existing_wave_columns = {column["name"] for column in inspect(engine).get_columns("waves")}
    existing_bucket_columns = {column["name"] for column in inspect(engine).get_columns("oci_bucket_cache")}
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
        for column, sql_type in wave_columns.items():
            if column not in existing_wave_columns:
                connection.execute(text(f"ALTER TABLE waves ADD COLUMN {column} {sql_type}"))
        if "compartment_name" not in existing_bucket_columns:
            connection.execute(text("ALTER TABLE oci_bucket_cache ADD COLUMN compartment_name VARCHAR(255)"))


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
            "simulation_enabled": settings.simulation_enabled, "aws_migration_role_arn": settings.aws_migration_role_arn,
            "aws_batch_role_arn": settings.aws_batch_role_arn, "aws_control_bucket": settings.aws_control_bucket,
            "aws_control_prefix": settings.aws_control_prefix, "real_worker_enabled": settings.real_worker_enabled,
            "preserve_s3_tags": settings.preserve_s3_tags,
            "activity_auto_refresh_enabled": settings.activity_auto_refresh_enabled,
            "activity_refresh_seconds": settings.activity_refresh_seconds, "updated_at": settings.updated_at}


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

    expected_secrets = ["aws_access_key_id", "aws_secret_access_key", "postgres_password"]
    secret_values: dict[str, str] = {}
    for secret_name in expected_secrets:
        secret_ocid = secret_ocids.get(secret_name)
        if not secret_ocid:
            checks.append({"name": f"Secret {secret_name}", "status": "NOT_CONFIGURED", "detail": "OCID ausente"})
            continue
        try:
            bundle = secrets_client.get_secret_bundle(secret_ocid).data
            encoded_content = bundle.secret_bundle_content.content
            content = base64.b64decode(encoded_content).decode("utf-8").strip()
            status = "PLACEHOLDER" if content.startswith("REPLACE_THIS_PLACEHOLDER") else "CONFIGURED"
            if status == "CONFIGURED":
                secret_values[secret_name] = content
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

    settings = runtime_settings(session)
    migration_role_arn = settings.aws_migration_role_arn.strip()
    batch_role_arn = settings.aws_batch_role_arn.strip()
    checks.append({"name": "Configuração role AWS de migração", "status": "CONFIGURED" if migration_role_arn else "NOT_CONFIGURED", "detail": "ARN preenchido na tela Configurações" if migration_role_arn else "preencha o ARN na tela Configurações"})
    checks.append({"name": "Configuração role AWS Batch Operations", "status": "CONFIGURED" if batch_role_arn else "NOT_CONFIGURED", "detail": "ARN preenchido; validação ocorrerá ao criar o primeiro job" if batch_role_arn else "preencha o ARN na tela Configurações"})
    control_bucket = settings.aws_control_bucket.strip()
    checks.append({"name": "Bucket AWS de controle", "status": "CONFIGURED" if control_bucket else "NOT_CONFIGURED", "detail": "bucket configurado; a autorização será testada sem gravar objetos" if control_bucket else "preencha o bucket de manifestos e relatórios"})
    aws_required = ["aws_access_key_id", "aws_secret_access_key"]
    if all(name in secret_values for name in aws_required) and migration_role_arn:
        try:
            import boto3
            aws_region = session.scalar(select(Source.aws_region).order_by(Source.id)) or "us-east-1"
            sts = boto3.client("sts", region_name=aws_region, aws_access_key_id=secret_values["aws_access_key_id"], aws_secret_access_key=secret_values["aws_secret_access_key"])
            sts.get_caller_identity()
            assumed = sts.assume_role(RoleArn=migration_role_arn, RoleSessionName="s3-oci-readiness", DurationSeconds=900)["Credentials"]
            for check in checks:
                if check["name"] in {"Secret aws_access_key_id", "Secret aws_secret_access_key", "Configuração role AWS de migração"}:
                    check["status"] = "VALIDATED"
                    check["detail"] = "validada no teste AWS STS e AssumeRole"
            checks.append({"name": "Credenciais AWS e role de migração", "status": "VALIDATED", "detail": "GetCallerIdentity e AssumeRole concluídos"})
            if control_bucket:
                try:
                    s3 = boto3.client("s3", region_name=aws_region, aws_access_key_id=assumed["AccessKeyId"], aws_secret_access_key=assumed["SecretAccessKey"], aws_session_token=assumed["SessionToken"])
                    s3.head_bucket(Bucket=control_bucket)
                    for check in checks:
                        if check["name"] == "Bucket AWS de controle":
                            check["status"], check["detail"] = "VALIDATED", "HeadBucket concluído; sem escrita"
                except Exception as error:
                    for check in checks:
                        if check["name"] == "Bucket AWS de controle":
                            check["status"], check["detail"] = "CONFIGURED", f"configurado, mas HeadBucket falhou: {type(error).__name__}"
        except Exception as error:
            checks.append({"name": "Credenciais AWS e role de migração", "status": "CONFIGURED", "detail": f"preenchidas, mas teste falhou: {type(error).__name__}"})
    else:
        checks.append({"name": "Credenciais AWS e role de migração", "status": "NOT_CONFIGURED", "detail": "preencha as duas Secrets AWS e o ARN da role de migração"})

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
    transferred_bytes, transferred_files, restored_files = int(transferred_bytes or 0), int(transferred_files or 0), int(restored_files or 0)
    transfer_seconds = min(window_seconds, max(1, (utcnow() - first_transfer).total_seconds())) if first_transfer else window_seconds
    restore_seconds = min(window_seconds, max(1, (utcnow() - first_restore).total_seconds())) if first_restore else window_seconds
    live_transfer_mbps = float(session.scalar(select(func.coalesce(func.sum(ObjectRecord.transfer_rate_mbps), 0)).where(
        ObjectRecord.state == ObjectState.TRANSFERRING,
        ObjectRecord.wave_id.in_(select(Task.wave_id).where(Task.kind == "TRANSFER_WAVE", Task.state == TaskState.RUNNING)),
    )) or 0)
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
            "transfer_mbps": round((transferred_bytes * 8) / transfer_seconds / 1_000_000, 2),
            "transfer_live_mbps": round(live_transfer_mbps, 2),
            "restored_files": restored_files,
            "restored_per_minute": round(restored_files / (restore_seconds / 60), 2),
            "restored_per_hour": round(restored_files / (restore_seconds / 3600), 2),
            "active_transfers": active_transfers,
        },
        "disk": {"total": volume.total, "used": volume.used, "free": volume.free},
    }


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


@app.get("/api/sources")
def list_sources(session: Session = Depends(get_session)) -> list[dict]:
    sources = list(session.scalars(select(Source).where(Source.archived_at.is_(None)).order_by(Source.id)))
    total_rows = session.execute(
        select(ObjectRecord.source_id, func.count(ObjectRecord.id),
               func.coalesce(func.sum(case((ObjectRecord.state == ObjectState.VERIFIED, 1), else_=0)), 0),
               func.coalesce(func.sum(case((ObjectRecord.state.in_([ObjectState.TRANSFERRED, ObjectState.VERIFIED]), 1), else_=0)), 0))
        .where(ObjectRecord.source_id.in_([s.id for s in sources])).group_by(ObjectRecord.source_id)
    ).all() if sources else []
    totals = {source_id: (int(total), int(verified), int(transferred)) for source_id, total, verified, transferred in total_rows}
    def migration_status(source: Source) -> str:
        total, verified, transferred = totals.get(source.id, (0, 0, 0))
        if source.destination_validation_status == "DIFFERENT":
            return "DESTINATION_DIVERGENT"
        if source.status == "DISCOVERED" and total and verified == total:
            return "COMPLETED"
        if source.status == "DISCOVERED" and total and transferred == total:
            return "AWAITING_INTEGRITY_VERIFICATION"
        return "IN_PROGRESS" if total else "NOT_STARTED"
    return [{"id": s.id, "name": s.name, "s3_bucket": s.s3_bucket, "s3_prefix": s.s3_prefix,
             "aws_region": s.aws_region, "destination_bucket": s.destination_bucket, "status": s.status,
             "discovery_requested_at": s.discovery_requested_at, "discovery_completed_at": s.discovery_completed_at,
             "discovery_error": s.discovery_error, "archived_at": s.archived_at,
             "destination_validation": {"status": s.destination_validation_status, "at": s.destination_validation_at,
                                        "missing": s.destination_missing_count, "size_mismatches": s.destination_size_mismatch_count},
             "migration_status": migration_status(s), "can_delete": not source_has_executed_wave(session, s.id)}
            for s in sources]


@app.post("/api/sources", status_code=201)
def create_source(payload: SourceCreate, session: Session = Depends(get_session)) -> dict:
    if session.scalar(select(Source).where(Source.name == payload.name)):
        raise HTTPException(status_code=409, detail="Source name already exists")
    if not session.scalar(select(OciBucketCache.id).where(OciBucketCache.name == payload.destination_bucket)):
        raise HTTPException(status_code=422, detail="Choose a destination bucket from the OCI cache; refresh it in Settings first")
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
    for field, value in payload.model_dump().items():
        setattr(source, field, value)
    record_event(session, "SOURCE_UPDATED", f"Source '{source.name}' configuration updated", source_id=source.id)
    session.commit()
    return {"id": source.id, "name": source.name, "status": source.status}


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
    for wave in session.scalars(select(Wave).where(Wave.source_id == source_id, Wave.status.not_in(["VERIFIED", "TRANSFERRED", "TRANSFERRED_WITH_ERRORS", "VERIFICATION_FAILED"]))):
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


@app.post("/api/sources/{source_id}/discovery")
def request_discovery(source_id: int, session: Session = Depends(get_session)) -> dict:
    source = active_source_or_409(session, source_id)
    if source.status == "DISCOVERING":
        raise HTTPException(status_code=409, detail="Discovery already running for this source")
    if session.scalar(select(func.count(Wave.id)).where(Wave.source_id == source_id)):
        raise HTTPException(status_code=409, detail="Discovery is immutable after waves are created")
    source.status = "DISCOVERY_QUEUED"
    source.discovery_requested_at = utcnow()
    source.discovery_completed_at = None
    source.discovery_error = None
    record_event(session, "DISCOVERY_QUEUED", f"AWS discovery queued for source '{source.name}'", source_id=source.id)
    session.commit()
    return {"source_id": source.id, "status": source.status}


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
    migration_status = "DESTINATION_DIVERGENT" if source.destination_validation_status == "DIFFERENT" else "COMPLETED" if source.status == "DISCOVERED" and count and states.get(ObjectState.VERIFIED, 0) == count else "AWAITING_INTEGRITY_VERIFICATION" if source.status == "DISCOVERED" and count and (states.get(ObjectState.TRANSFERRED, 0) + states.get(ObjectState.VERIFIED, 0)) == count else "IN_PROGRESS" if count else "NOT_STARTED"
    return {"source_id": source_id, "objects": count, "bytes": bytes_total, "object_states": states, "migration_status": migration_status,
            "destination_validation": {"status": source.destination_validation_status, "at": source.destination_validation_at,
                                       "missing": source.destination_missing_count, "size_mismatches": source.destination_size_mismatch_count},
            "discovery": {"status": source.status, "requested_at": source.discovery_requested_at,
                          "completed_at": source.discovery_completed_at, "error": source.discovery_error}}


@app.post("/api/sources/{source_id}/validate-destination")
def validate_destination(source_id: int, session: Session = Depends(get_session)) -> dict:
    """Explicitly reconcile the persisted S3 discovery inventory with OCI listing."""
    source = active_source_or_409(session, source_id)
    expected = {key: int(size) for key, size in session.execute(
        select(ObjectRecord.object_key, ObjectRecord.size_bytes).where(ObjectRecord.source_id == source.id)
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
    mismatched = sorted(key for key, size in expected.items() if key in found and found[key] != size)
    source.destination_validation_at = utcnow()
    source.destination_missing_count, source.destination_size_mismatch_count = len(missing), len(mismatched)
    source.destination_validation_status = "VALID" if not missing and not mismatched else "DIFFERENT"
    affected_wave_counts: dict[int, int] = {}
    divergent_keys = missing + mismatched
    for offset in range(0, len(divergent_keys), 1000):
        objects = list(session.scalars(select(ObjectRecord).where(
            ObjectRecord.source_id == source.id, ObjectRecord.object_key.in_(divergent_keys[offset:offset + 1000])
        )))
        for obj in objects:
            obj.integrity_verified_at, obj.destination_checksum, obj.transferred_at = None, None, None
            obj.integrity_error = "OCI destination validation found object missing or with a different size"
            if obj.wave_id:
                obj.state = ObjectState.WAVE_ASSIGNED
                affected_wave_counts[obj.wave_id] = affected_wave_counts.get(obj.wave_id, 0) + 1
            else:
                obj.state = ObjectState.DISCOVERED
    for wave_id, affected_objects in affected_wave_counts.items():
        wave = session.get(Wave, wave_id)
        wave.status, wave.batch_job_id, wave.manifest_key, wave.manifest_etag = "READY_FOR_RESTORE", None, None, None
        wave.last_poll_at, wave.poll_count = None, 0
        record_event(session, "DESTINATION_DIVERGENCE_REOPENED_WAVE", f"Wave reopened after OCI destination validation; {affected_objects} object(s) require reprocessing", source_id=source.id, wave_id=wave.id)
    record_event(session, "DESTINATION_VALIDATED", f"OCI destination validation: {len(missing)} missing and {len(mismatched)} size mismatch(es)", source_id=source.id)
    session.commit()
    return {"source_id": source.id, "status": source.destination_validation_status, "expected": len(expected), "found": len(found), "missing": len(missing), "size_mismatches": len(mismatched), "missing_examples": missing[:50], "size_mismatch_examples": mismatched[:50], "validated_at": source.destination_validation_at}


@app.get("/api/sources/{source_id}/inventory")
def list_inventory(source_id: int, limit: int = 10, offset: int = 0,
                   search: str = Query(default="", max_length=512),
                   session: Session = Depends(get_session)) -> dict:
    source_or_404(session, source_id)
    limit = min(max(limit, 1), 1000)
    filters = [ObjectRecord.source_id == source_id]
    if search.strip():
        filters.append(ObjectRecord.object_key.ilike(f"%{search.strip()}%"))
    query = select(ObjectRecord).where(*filters).order_by(ObjectRecord.object_key).offset(offset).limit(limit)
    rows = session.scalars(query)
    total = session.scalar(select(func.count(ObjectRecord.id)).where(*filters)) or 0
    return {"items": [{"id": obj.id, "key": obj.object_key, "version_id": obj.version_id,
                       "size_bytes": obj.size_bytes, "storage_class": obj.storage_class, "state": obj.state,
                       "last_modified": obj.last_modified, "etag": obj.etag, "wave_id": obj.wave_id} for obj in rows],
            "limit": limit, "offset": offset, "total": total, "search": search.strip()}


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
                          "error": obj.integrity_error}, "restored_at": obj.restored_at,
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
                restore_tier: str, objects: list[ObjectRecord], oversized: bool = False) -> Wave:
    assigned_bytes = sum(obj.size_bytes for obj in objects)
    wave = Wave(source_id=source_id, name=name, max_bytes=max_bytes, restore_days=restore_days,
                restore_tier=restore_tier, status="PLANNED")
    session.add(wave)
    session.flush()
    for obj in objects:
        obj.wave_id = wave.id
        obj.state = ObjectState.WAVE_ASSIGNED
    suffix = " (contains an object larger than the configured target)" if oversized else ""
    record_event(session, "WAVE_CREATED", f"Wave '{wave.name}' planned with {len(objects)} object(s) and {assigned_bytes} byte(s){suffix}; no task was queued", source_id=source_id, wave_id=wave.id)
    return wave


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
    rows = session.execute(
        select(Wave, func.count(ObjectRecord.id), func.coalesce(func.sum(ObjectRecord.size_bytes), 0))
        .outerjoin(ObjectRecord, ObjectRecord.wave_id == Wave.id)
        .where(Wave.source_id == source_id)
        .group_by(Wave.id)
        .order_by(Wave.id)
    )
    return [{"id": wave.id, "name": wave.name, "status": wave.status, "restore_tier": wave.restore_tier,
             "restore_days": wave.restore_days, "objects": count, "bytes": size, "batch_job_id": wave.batch_job_id,
             "last_poll_at": wave.last_poll_at,
             "can_delete": wave.id not in executed_wave_ids and wave.id not in progressed_wave_ids,
             "is_transferring": wave.id in transferring_wave_ids}
            for wave, count, size in rows]


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
    return {"wave_id": wave_id, "status": wave.status, "objects": total_objects, "bytes": total_bytes, "object_states": by_state,
            "batch": {"job_id": wave.batch_job_id, "manifest_key": wave.manifest_key, "last_poll_at": wave.last_poll_at, "poll_count": wave.poll_count},
            "integrity": {"verified": integrity_verified, "failed": integrity_failed, "pending": total_objects - integrity_verified - integrity_failed},
            "tasks": [{"id": t.id, "kind": t.kind, "state": t.state, "attempts": t.attempts, "error": t.error} for t in wave.tasks]}


@app.post("/api/waves/{wave_id}/verify")
def verify_wave(wave_id: int, session: Session = Depends(get_session)) -> dict:
    """Queue integrity verification only when an operator explicitly requests it."""
    wave = wave_or_404(session, wave_id)
    if wave.status not in {"TRANSFERRED", "TRANSFERRED_WITH_ERRORS", "VERIFICATION_FAILED"}:
        raise HTTPException(status_code=409, detail="Integrity verification can only be requested after transfer completes")
    queued = session.scalar(select(Task.id).where(
        Task.wave_id == wave.id, Task.kind == "VERIFY_WAVE", Task.state.in_([TaskState.READY, TaskState.RUNNING])
    ))
    if queued:
        raise HTTPException(status_code=409, detail="Integrity verification is already queued or running for this wave")
    wave.status = "VERIFICATION_QUEUED"
    session.add(Task(wave_id=wave.id, kind="VERIFY_WAVE"))
    record_event(session, "INTEGRITY_VERIFICATION_QUEUED", f"Integrity verification queued by operator for wave '{wave.name}'", source_id=wave.source_id, wave_id=wave.id)
    session.commit()
    return {"wave_id": wave.id, "status": wave.status, "message": "Integrity verification queued"}


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
    wave.status = "READY_FOR_RESTORE"
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
