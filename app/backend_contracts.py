"""Typed cloud-operation ports shared by real, simulated and future edge runtimes.

These models are intentionally transport-neutral and JSON serializable. Byte
streams remain iterators at the local port boundary; their request metadata is
serializable so the same operation can later be represented by a remote work
order without sending payload through the control plane.
"""

from __future__ import annotations

from datetime import datetime
from typing import Iterable, Protocol
from uuid import UUID

from pydantic import BaseModel, Field

from app.runtime_context import SIMULATOR_CONTRACT_VERSION


class ExecutionContext(BaseModel):
    tenant_id: UUID
    project_id: UUID
    execution_id: UUID
    correlation_id: UUID
    contract_version: str = SIMULATOR_CONTRACT_VERSION


class ObjectIdentity(BaseModel):
    bucket: str = Field(min_length=1, max_length=255)
    key: str = Field(min_length=1, max_length=2048)
    version_id: str | None = Field(default=None, max_length=1024)


class ObjectDescriptor(ObjectIdentity):
    size_bytes: int = Field(ge=0)
    storage_class: str = Field(default="STANDARD", max_length=64)
    etag: str | None = Field(default=None, max_length=256)
    last_modified: datetime | None = None
    checksum_algorithm: str | None = Field(default=None, max_length=32)
    checksum: str | None = Field(default=None, max_length=256)
    metadata: dict[str, str] = Field(default_factory=dict)
    tags: dict[str, str] = Field(default_factory=dict)


class ListObjectsRequest(BaseModel):
    context: ExecutionContext
    bucket: str = Field(min_length=1, max_length=255)
    prefix: str = Field(default="", max_length=2048)
    continuation_token: str | None = None
    max_keys: int = Field(default=1000, ge=1, le=1000)


class ListObjectsPage(BaseModel):
    objects: list[ObjectDescriptor]
    next_continuation_token: str | None = None


class RestoreObjectRequest(BaseModel):
    context: ExecutionContext
    object: ObjectIdentity
    tier: str = Field(pattern="^(BULK|STANDARD)$")
    retention_days: int = Field(ge=1, le=30)
    idempotency_key: UUID


class RestoreObjectResult(BaseModel):
    accepted: bool
    already_in_progress: bool = False
    request_id: str | None = None
    error_code: str | None = None
    error_message: str | None = None


class SubmitRestoreBatchRequest(BaseModel):
    context: ExecutionContext
    bucket: str = Field(min_length=1, max_length=255)
    tier: str = Field(pattern="^(BULK|STANDARD)$")
    retention_days: int = Field(ge=1, le=30)
    object_count: int = Field(gt=0)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    idempotency_key: UUID


class SubmitRestoreBatchResult(BaseModel):
    job_id: str
    status: str
    object_count: int


class DescribeRestoreBatchRequest(BaseModel):
    context: ExecutionContext
    job_id: str = Field(min_length=1, max_length=512)
    continuation_token: str | None = None
    max_results: int = Field(default=1000, ge=1, le=1000)


class RestoreBatchObjectResult(BaseModel):
    key: str
    version_id: str | None = None
    accepted: bool
    already_in_progress: bool = False
    error_code: str | None = None
    error_message: str | None = None


class DescribeRestoreBatchResult(BaseModel):
    job_id: str
    status: str
    object_count: int
    accepted_count: int
    failed_count: int
    results: list[RestoreBatchObjectResult]
    next_continuation_token: str | None = None


class HeadObjectRequest(BaseModel):
    context: ExecutionContext
    object: ObjectIdentity


class HeadObjectResult(BaseModel):
    exists: bool
    descriptor: ObjectDescriptor | None = None
    restore_in_progress: bool = False
    restore_expires_at: datetime | None = None
    simulator_virtual_now: datetime | None = None
    simulator_recommended_real_poll_seconds: float | None = Field(default=None, ge=0)


class ReadRangeRequest(BaseModel):
    context: ExecutionContext
    object: ObjectIdentity
    offset: int = Field(default=0, ge=0)
    length: int = Field(gt=0)
    # Simulator-only deep audit can deterministically regenerate an archived
    # source after its temporary restore window. Real cloud adapters must not
    # interpret this as permission to bypass provider storage semantics.
    allow_archived_for_audit: bool = False


class PutObjectRequest(BaseModel):
    context: ExecutionContext
    object: ObjectDescriptor
    source_checksum_sha256: str
    idempotency_key: UUID


class MultipartCreateRequest(BaseModel):
    context: ExecutionContext
    object: ObjectDescriptor
    part_size_bytes: int = Field(gt=0)
    idempotency_key: UUID


class MultipartPartRequest(BaseModel):
    context: ExecutionContext
    upload_id: str = Field(min_length=1, max_length=512)
    object: ObjectIdentity
    part_number: int = Field(ge=1, le=10000)
    size_bytes: int = Field(gt=0)
    checksum_sha256: str
    idempotency_key: UUID


class MultipartPartEvidence(BaseModel):
    part_number: int
    size_bytes: int
    checksum_sha256: str
    etag: str


class MultipartCommitRequest(BaseModel):
    context: ExecutionContext
    upload_id: str = Field(min_length=1, max_length=512)
    object: ObjectIdentity
    parts: list[MultipartPartEvidence]
    # A resumed multipart copy may deliberately skip source ranges that were
    # already accepted.  In that case a new linear full-object SHA-256 is not
    # available, while every part still has independent cryptographic
    # evidence.  The destination validates the complete ordered part manifest.
    full_checksum_sha256: str | None = None
    idempotency_key: UUID


class WriteEvidence(BaseModel):
    accepted: bool
    request_id: str
    size_bytes: int
    checksum_sha256: str
    etag: str | None = None


class LogicalTransferRequest(BaseModel):
    context: ExecutionContext
    source: ObjectIdentity
    destination_bucket: str = Field(min_length=1, max_length=255)
    size_bytes: int = Field(ge=0)
    # Raiju's allocation is part of the simulated transport contract. CONTROL
    # must model the same aggregate link envelope as DATA/REAL, rather than
    # granting each parallel object the entire scenario throughput.
    allocated_rate_mbps: float = Field(gt=0)
    active_workers: int = Field(ge=1)
    network_operation_key: str = Field(min_length=1, max_length=255)
    idempotency_key: UUID


class LogicalTransferResult(BaseModel):
    evidence: WriteEvidence
    simulated_elapsed_seconds: float = Field(ge=0)


class SourceCloudPort(Protocol):
    def list_objects(self, request: ListObjectsRequest) -> ListObjectsPage: ...
    def head_object(self, request: HeadObjectRequest) -> HeadObjectResult: ...
    def restore_object(self, request: RestoreObjectRequest) -> RestoreObjectResult: ...
    def read_range(self, request: ReadRangeRequest) -> Iterable[bytes]: ...
    def submit_restore_batch(
        self, request: SubmitRestoreBatchRequest, manifest: Iterable[bytes]
    ) -> SubmitRestoreBatchResult: ...
    def describe_restore_batch(
        self, request: DescribeRestoreBatchRequest
    ) -> DescribeRestoreBatchResult: ...


class DestinationCloudPort(Protocol):
    def head_object(self, request: HeadObjectRequest) -> HeadObjectResult: ...
    def read_range(self, request: ReadRangeRequest) -> Iterable[bytes]: ...
    def put_object(self, request: PutObjectRequest, chunks: Iterable[bytes]) -> WriteEvidence: ...
    def create_multipart(self, request: MultipartCreateRequest) -> str: ...
    def upload_part(self, request: MultipartPartRequest, chunks: Iterable[bytes]) -> MultipartPartEvidence: ...
    def commit_multipart(self, request: MultipartCommitRequest) -> WriteEvidence: ...
    def transfer_logically(self, request: LogicalTransferRequest) -> LogicalTransferResult: ...
