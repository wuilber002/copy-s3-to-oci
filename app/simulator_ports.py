"""HTTP adapters implementing the typed cloud ports against raijin-simulator."""

from __future__ import annotations

import base64
import http.client
import json
from typing import Iterable, Iterator, TypeVar
from urllib.parse import urlparse
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from pydantic import BaseModel

from app.backend_contracts import (
    DestinationCloudPort,
    HeadObjectRequest,
    HeadObjectResult,
    ListObjectsPage,
    ListObjectsRequest,
    MultipartCommitRequest,
    MultipartCreateRequest,
    MultipartPartEvidence,
    MultipartPartRequest,
    PutObjectRequest,
    ReadRangeRequest,
    RestoreObjectRequest,
    RestoreObjectResult,
    SourceCloudPort,
    WriteEvidence,
    DescribeRestoreBatchRequest,
    DescribeRestoreBatchResult,
    SubmitRestoreBatchRequest,
    SubmitRestoreBatchResult,
    LogicalTransferRequest,
    LogicalTransferResult,
)


T = TypeVar("T", bound=BaseModel)


class SimulatorTransportError(RuntimeError):
    pass


class SimulatorHttpTransport:
    def __init__(self, base_url: str, timeout_seconds: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.parsed = urlparse(self.base_url)
        if self.parsed.scheme not in {"http", "https"} or not self.parsed.hostname:
            raise ValueError("Simulator URL must be HTTP(S)")

    def json(self, path: str, payload: BaseModel, response_model: type[T]) -> T:
        request = Request(
            f"{self.base_url}{path}",
            data=payload.model_dump_json().encode("utf-8"),
            method="POST",
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                return response_model.model_validate_json(response.read())
        except HTTPError as error:
            # Preserve the simulator's structured reason.  A bare HTTP 500/409
            # in a wave report is not actionable enough for an operator.
            try:
                detail = error.read().decode("utf-8", errors="replace")
            except Exception:
                detail = ""
            raise SimulatorTransportError(
                f"Simulator request failed: {path}: HTTP {error.code}"
                + (f": {detail}" if detail else "")
            ) from error
        except Exception as error:
            raise SimulatorTransportError(f"Simulator request failed: {path}: {error}") from error

    def stream_response(self, path: str, payload: BaseModel) -> Iterator[bytes]:
        request = Request(
            f"{self.base_url}{path}",
            data=payload.model_dump_json().encode("utf-8"),
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            response = urlopen(request, timeout=self.timeout_seconds)
        except Exception as error:
            raise SimulatorTransportError(f"Simulator stream failed: {path}: {error}") from error
        try:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                yield chunk
        finally:
            response.close()

    def upload(
        self, path: str, metadata: BaseModel, chunks: Iterable[bytes], response_model: type[T]
    ) -> T:
        connection_class = (
            http.client.HTTPSConnection if self.parsed.scheme == "https" else http.client.HTTPConnection
        )
        connection = connection_class(
            self.parsed.hostname,
            self.parsed.port,
            timeout=self.timeout_seconds,
        )
        prefix = self.parsed.path.rstrip("/")
        encoded = base64.urlsafe_b64encode(metadata.model_dump_json().encode("utf-8")).decode(
            "ascii"
        )
        try:
            connection.request(
                "PUT",
                f"{prefix}{path}",
                body=chunks,
                headers={
                    "Content-Type": "application/octet-stream",
                    "X-Raijin-Metadata": encoded,
                },
                encode_chunked=True,
            )
            response = connection.getresponse()
            body = response.read()
            if response.status >= 400:
                raise SimulatorTransportError(
                    f"Simulator upload failed: {path}: HTTP {response.status}: "
                    f"{body.decode('utf-8', errors='replace')}"
                )
            return response_model.model_validate_json(body)
        finally:
            connection.close()


class SimulatedSourcePort(SourceCloudPort):
    def __init__(self, base_url: str):
        self.transport = SimulatorHttpTransport(base_url)

    def list_objects(self, request: ListObjectsRequest) -> ListObjectsPage:
        return self.transport.json("/v1/cloud/source/list-objects", request, ListObjectsPage)

    def head_object(self, request: HeadObjectRequest) -> HeadObjectResult:
        return self.transport.json("/v1/cloud/source/head-object", request, HeadObjectResult)

    def restore_object(self, request: RestoreObjectRequest) -> RestoreObjectResult:
        return self.transport.json(
            "/v1/cloud/source/restore-object", request, RestoreObjectResult
        )

    def read_range(self, request: ReadRangeRequest) -> Iterable[bytes]:
        return self.transport.stream_response("/v1/cloud/source/read-range", request)

    def submit_restore_batch(
        self, request: SubmitRestoreBatchRequest, manifest: Iterable[bytes]
    ) -> SubmitRestoreBatchResult:
        return self.transport.upload(
            "/v1/cloud/source/submit-restore-batch",
            request,
            manifest,
            SubmitRestoreBatchResult,
        )

    def describe_restore_batch(
        self, request: DescribeRestoreBatchRequest
    ) -> DescribeRestoreBatchResult:
        return self.transport.json(
            "/v1/cloud/source/describe-restore-batch", request, DescribeRestoreBatchResult
        )


class SimulatedDestinationPort(DestinationCloudPort):
    def __init__(self, base_url: str):
        self.transport = SimulatorHttpTransport(base_url)

    def head_object(self, request: HeadObjectRequest) -> HeadObjectResult:
        return self.transport.json("/v1/cloud/source/head-object", request, HeadObjectResult)

    def read_range(self, request: ReadRangeRequest) -> Iterable[bytes]:
        return self.transport.stream_response("/v1/cloud/destination/read-range", request)

    def put_object(self, request: PutObjectRequest, chunks: Iterable[bytes]) -> WriteEvidence:
        return self.transport.upload(
            "/v1/cloud/destination/put-object", request, chunks, WriteEvidence
        )

    def create_multipart(self, request: MultipartCreateRequest) -> str:
        class UploadResponse(BaseModel):
            upload_id: str

        return self.transport.json(
            "/v1/cloud/destination/create-multipart", request, UploadResponse
        ).upload_id

    def upload_part(
        self, request: MultipartPartRequest, chunks: Iterable[bytes]
    ) -> MultipartPartEvidence:
        return self.transport.upload(
            "/v1/cloud/destination/upload-part", request, chunks, MultipartPartEvidence
        )

    def commit_multipart(self, request: MultipartCommitRequest) -> WriteEvidence:
        return self.transport.json(
            "/v1/cloud/destination/commit-multipart", request, WriteEvidence
        )

    def transfer_logically(self, request: LogicalTransferRequest) -> LogicalTransferResult:
        return self.transport.json(
            "/v1/cloud/destination/logical-transfer", request, LogicalTransferResult
        )
