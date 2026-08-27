"""External, versioned AWS/OCI simulation service.

It never consumes RAIJIN's durable queue. The normal governance and transfer
workers call its typed cloud ports, so simulated executions exercise the same
state machine, leases, checkpoints and integrity controls as REAL mode.
"""

import base64
import asyncio
from contextlib import asynccontextmanager, suppress
from functools import lru_cache
import hashlib
import json

from fastapi import FastAPI, Header, HTTPException, Request
from sqlalchemy.exc import IntegrityError
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.runtime_context import SIMULATOR_CONTRACT_VERSION, SIMULATOR_SERVICE_VERSION
from app.simulator_contract import SimulatorHandshake, SimulatorHealth
from app.simulator_store import (
    ScenarioConflictError,
    ScenarioCreate,
    SimulatorStore,
    TemplateWrite,
)
from app.simulation_engine import SimulationEngine, SimulatedNetworkUnavailable
from app.backend_contracts import (
    HeadObjectRequest,
    ListObjectsRequest,
    ReadRangeRequest,
    RestoreObjectRequest,
    MultipartCommitRequest,
    MultipartCreateRequest,
    MultipartPartRequest,
    PutObjectRequest,
    DescribeRestoreBatchRequest,
    SubmitRestoreBatchRequest,
    LogicalTransferRequest,
)
from app.simulated_data import SimulatedDataIntegrityError, StreamEvidence


async def automatic_housekeeping(stop: asyncio.Event) -> None:
    while True:
        try:
            await asyncio.wait_for(stop.wait(), timeout=24 * 60 * 60)
            return
        except asyncio.TimeoutError:
            store().apply_housekeeping()
            store().apply_generator_housekeeping()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    if operations_ready()[0]:
        store().ensure_generator_release()
        store().ensure_default_templates()
        store().pause_running_clocks_after_startup()
        store().apply_housekeeping()
    stop = asyncio.Event()
    task = asyncio.create_task(automatic_housekeeping(stop))
    try:
        yield
    finally:
        stop.set()
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


app = FastAPI(
    title="RAIJIN Simulator", version=SIMULATOR_SERVICE_VERSION, lifespan=lifespan
)


class ScenarioCreatePayload(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    fidelity: str
    seed: str = Field(min_length=1, max_length=255)
    logical_size_bytes: int = Field(ge=0)
    physical_budget_bytes: int = Field(default=1_000_000_000_000, ge=0)
    clock_acceleration: float = Field(default=3600.0, gt=0)
    retention_days: int = Field(default=60, ge=0, le=3650)
    quarantine_days: int = Field(default=30, ge=0, le=3650)
    template_id: str | None = None
    template_snapshot: dict = Field(default_factory=dict)
    configuration: dict = Field(default_factory=dict)
    fault_rules: list = Field(default_factory=list)


class MaterializePayload(BaseModel):
    source_bucket: str = Field(min_length=1, max_length=255)
    destination_bucket: str = Field(min_length=1, max_length=255)
    region: str = Field(min_length=1, max_length=64)
    object_count: int = Field(gt=0)
    logical_size_bytes: int = Field(ge=0)
    prefixes: list[str] = Field(default_factory=lambda: ["simulation"])
    storage_class: str = Field(default="DEEP_ARCHIVE", max_length=64)


class ClockControlPayload(BaseModel):
    action: str
    advance_seconds: float = Field(default=0, ge=0)


class ExecutionStatePayload(BaseModel):
    state: str


class ExecutionClonePayload(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    fidelity: str | None = None
    physical_budget_bytes: int | None = Field(default=None, ge=0)


class ScenarioPurgePayload(BaseModel):
    reason: str = Field(min_length=10, max_length=2000)


class TemplateWritePayload(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    description: str = Field(default="", max_length=4000)
    fidelity: str
    configuration: dict = Field(default_factory=dict)
    fault_rules: list = Field(default_factory=list)


def scenario_response(item) -> dict:
    return {
        "id": item.id,
        "name": item.name,
        "fidelity": item.fidelity,
        "state": item.state,
        "logical_size_bytes": item.logical_size_bytes,
        "physical_budget_bytes": item.physical_budget_bytes,
        "clock_acceleration": item.clock_acceleration,
        "retention_days": item.retention_days,
        "quarantine_days": item.quarantine_days,
        "contract_version": item.contract_version,
        "generator_version": item.generator_version,
        "created_at": item.created_at,
    }


def template_response(item) -> dict:
    return {
        "id": item.id,
        "name": item.name,
        "description": item.description,
        "fidelity": item.fidelity,
        "configuration": json.loads(item.configuration_json),
        "fault_rules": json.loads(item.fault_rules_json),
        "updated_at": item.updated_at,
    }


@lru_cache(maxsize=1)
def store() -> SimulatorStore:
    return SimulatorStore.from_environ()


def engine() -> SimulationEngine:
    return SimulationEngine(store())


def decode_metadata(value: str, model):
    try:
        raw = base64.urlsafe_b64decode(value.encode("ascii"))
        return model.model_validate_json(raw)
    except Exception as error:
        raise HTTPException(status_code=422, detail="Invalid simulation request metadata") from error


async def consume_request(request: Request, expected_size: int) -> StreamEvidence:
    digest = hashlib.sha256()
    size = 0
    async for chunk in request.stream():
        digest.update(chunk)
        size += len(chunk)
        if size > expected_size:
            raise HTTPException(status_code=422, detail="Stream exceeds declared size")
    if size != expected_size:
        raise HTTPException(
            status_code=422, detail=f"Received {size} bytes; expected {expected_size}"
        )
    return StreamEvidence(
        size_bytes=size,
        checksum_sha256=base64.b64encode(digest.digest()).decode("ascii"),
    )


def operations_ready() -> tuple[bool, str | None]:
    try:
        store().require_current_schema()
        return True, None
    except Exception as error:
        return False, str(error)


@app.get("/healthz", response_model=SimulatorHealth)
def healthcheck() -> SimulatorHealth:
    ready, error = operations_ready()
    return SimulatorHealth(ready=ready, detail=error)


@app.get("/v1/handshake", response_model=SimulatorHandshake)
def handshake() -> SimulatorHandshake:
    ready, _ = operations_ready()
    return SimulatorHandshake(
        contract_version=SIMULATOR_CONTRACT_VERSION,
        service_version=SIMULATOR_SERVICE_VERSION,
        operations_enabled=ready,
        capabilities=(
            [
                "scenario-catalog-v1",
                "immutable-execution-snapshot-v1",
                "virtual-source-v1",
                "discarding-destination-v1",
                "multipart-resume-v1",
                "batch-restore-v1",
                "logical-transfer-v1",
                "deep-audit-replay-v1",
                "virtual-clock-v1",
                "scenario-templates-v1",
                "editable-template-snapshots-v1",
                "network-profile-v1",
                "execution-lifecycle-v1",
                "clone-replay-v1",
                "housekeeping-quarantine-v1",
                "generator-lifecycle-v1",
            ]
            if ready
            else []
        ),
    )


@app.post("/v1/scenarios/{scenario_id}/materialize", status_code=201)
def materialize_scenario(scenario_id: str, payload: MaterializePayload) -> dict:
    try:
        return engine().materialize(scenario_id, **payload.model_dump())
    except LookupError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    except ValueError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    except Exception as error:
        raise HTTPException(status_code=503, detail=str(error)) from error


@app.post("/v1/scenarios", status_code=201)
def create_scenario(payload: ScenarioCreatePayload) -> dict:
    try:
        item = store().create_scenario(ScenarioCreate(**payload.model_dump()))
        return scenario_response(item)
    except ScenarioConflictError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    except (ValueError, IntegrityError) as error:
        raise HTTPException(status_code=422, detail=str(error)) from error
    except Exception as error:
        raise HTTPException(status_code=503, detail=str(error)) from error


@app.get("/v1/scenarios")
def list_scenarios() -> list[dict]:
    try:
        return [scenario_response(item) for item in store().list_scenarios()]
    except Exception as error:
        raise HTTPException(status_code=503, detail=str(error)) from error


@app.get("/v1/templates")
def list_templates() -> list[dict]:
    return [template_response(item) for item in store().list_templates()]


@app.post("/v1/templates", status_code=201)
def create_template(payload: TemplateWritePayload) -> dict:
    try:
        return template_response(store().create_template(TemplateWrite(**payload.model_dump())))
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error


@app.put("/v1/templates/{template_id}")
def update_template(template_id: str, payload: TemplateWritePayload) -> dict:
    try:
        return template_response(store().update_template(template_id, TemplateWrite(**payload.model_dump())))
    except LookupError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error


@app.post("/v1/scenarios/{scenario_id}/executions", status_code=201)
def create_execution(scenario_id: str) -> dict:
    try:
        execution = store().create_execution(scenario_id)
        return {
            "id": execution.id,
            "scenario_id": execution.scenario_id,
            "correlation_id": execution.correlation_id,
            "state": execution.state,
            "physical_budget_bytes": execution.physical_budget_bytes,
            "snapshot": json.loads(execution.immutable_snapshot_json),
            "created_at": execution.created_at,
        }
    except LookupError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    except ValueError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    except Exception as error:
        raise HTTPException(status_code=503, detail=str(error)) from error


@app.get("/v1/executions/{execution_id}/clock")
def execution_clock(execution_id: str) -> dict:
    try:
        return store().clock_status(execution_id)
    except LookupError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error


@app.get("/v1/executions/{execution_id}")
def execution_detail(execution_id: str) -> dict:
    item = store().get_execution(execution_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Simulation execution not found")
    return {
        "id": item.id,
        "scenario_id": item.scenario_id,
        "correlation_id": item.correlation_id,
        "state": item.state,
        "physical_budget_bytes": item.physical_budget_bytes,
        "physical_bytes_processed": item.physical_bytes_processed,
        "real_started_at": item.real_started_at,
        "real_finished_at": item.real_finished_at,
        "virtual_started_at": item.virtual_started_at,
        "virtual_finished_at": item.virtual_finished_at,
        "created_at": item.created_at,
    }


@app.get("/v1/executions/{execution_id}/report")
def execution_report(execution_id: str) -> dict:
    try:
        return store().execution_report(execution_id)
    except LookupError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error


@app.post("/v1/executions/{execution_id}/clone", status_code=201)
def clone_execution(execution_id: str, payload: ExecutionClonePayload) -> dict:
    original = store().get_execution(execution_id)
    if original is None:
        raise HTTPException(status_code=404, detail="Simulation execution not found")
    snapshot = json.loads(original.immutable_snapshot_json)
    materialization = snapshot.get("configuration", {}).get("_materialization")
    if not materialization:
        raise HTTPException(status_code=409, detail="Execution has no reproducible catalog snapshot")
    try:
        fidelity = (payload.fidelity or snapshot["fidelity"]).upper()
        configuration = dict(snapshot.get("configuration", {}))
        configuration.pop("_materialization", None)
        scenario = store().create_scenario(ScenarioCreate(
            name=payload.name,
            fidelity=fidelity,
            seed=snapshot["seed"],
            logical_size_bytes=int(snapshot["logical_size_bytes"]),
            physical_budget_bytes=(payload.physical_budget_bytes if payload.physical_budget_bytes is not None else int(snapshot["physical_budget_bytes"])),
            clock_acceleration=float(snapshot["clock_acceleration"]),
            template_snapshot=snapshot.get("template_snapshot", {}),
            configuration=configuration,
            fault_rules=snapshot.get("fault_rules", []),
        ))
        catalog = engine().materialize(scenario.id, **materialization)
        cloned = store().create_execution(scenario.id)
        return {
            "scenario": scenario_response(scenario),
            "execution": {
                "id": cloned.id,
                "scenario_id": cloned.scenario_id,
                "correlation_id": cloned.correlation_id,
                "state": cloned.state,
            },
            "catalog": catalog,
        }
    except ValueError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@app.post("/v1/executions/{execution_id}/clock")
def control_execution_clock(execution_id: str, payload: ClockControlPayload) -> dict:
    try:
        return store().control_clock(execution_id, payload.action, payload.advance_seconds)
    except LookupError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error


@app.post("/v1/executions/{execution_id}/state")
def set_execution_state(execution_id: str, payload: ExecutionStatePayload) -> dict:
    try:
        item = store().set_execution_state(execution_id, payload.state)
        return {"id": item.id, "scenario_id": item.scenario_id, "state": item.state,
                "real_started_at": item.real_started_at, "real_finished_at": item.real_finished_at,
                "virtual_started_at": item.virtual_started_at, "virtual_finished_at": item.virtual_finished_at}
    except LookupError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    except ValueError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@app.post("/v1/housekeeping")
def apply_housekeeping() -> dict:
    result = store().apply_housekeeping()
    result["generator_lifecycle"] = store().apply_generator_housekeeping()
    return result


@app.get("/v1/generators")
def list_generator_releases() -> list[dict]:
    return store().list_generator_releases()


@app.post("/v1/scenarios/{scenario_id}/restore")
def restore_scenario_lifecycle(scenario_id: str) -> dict:
    try:
        return scenario_response(store().restore_scenario(scenario_id))
    except LookupError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    except ValueError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@app.post("/v1/scenarios/{scenario_id}/purge")
def purge_scenario(scenario_id: str, payload: ScenarioPurgePayload) -> dict:
    try:
        return store().purge_scenario(scenario_id, payload.reason)
    except LookupError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    except ValueError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@app.post("/v1/cloud/source/list-objects")
def source_list_objects(payload: ListObjectsRequest) -> dict:
    try:
        return engine().list_objects(
            str(payload.context.execution_id),
            payload.bucket,
            payload.prefix,
            payload.continuation_token,
            payload.max_keys,
        ).model_dump(mode="json")
    except LookupError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error


@app.post("/v1/cloud/source/head-object")
def source_head_object(payload: HeadObjectRequest) -> dict:
    try:
        return engine().head_object(
            str(payload.context.execution_id), payload.object.bucket, payload.object.key
        ).model_dump(mode="json")
    except LookupError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error


@app.post("/v1/cloud/source/restore-object")
def source_restore_object(payload: RestoreObjectRequest) -> dict:
    try:
        return engine().restore_object(
            str(payload.context.execution_id),
            payload.object.bucket,
            payload.object.key,
            payload.tier,
            payload.retention_days,
            str(payload.idempotency_key),
        ).model_dump(mode="json")
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error


@app.put("/v1/cloud/source/submit-restore-batch", status_code=201)
async def source_submit_restore_batch(
    request: Request, x_raijin_metadata: str = Header(alias="X-Raijin-Metadata")
) -> dict:
    payload = decode_metadata(x_raijin_metadata, SubmitRestoreBatchRequest)
    try:
        job = engine().create_restore_job(
            str(payload.context.execution_id),
            payload.bucket,
            payload.tier,
            payload.retention_days,
            payload.object_count,
            payload.manifest_sha256,
            str(payload.idempotency_key),
        )
        digest = hashlib.sha256()
        buffer = b""
        batch: list[tuple[str, str | None]] = []
        async for chunk in request.stream():
            digest.update(chunk)
            buffer += chunk
            while b"\n" in buffer:
                line, buffer = buffer.split(b"\n", 1)
                if not line:
                    continue
                row = json.loads(line)
                batch.append((str(row["key"]), row.get("version_id")))
                if len(batch) >= 1000:
                    engine().append_restore_job_objects(job.id, batch)
                    batch.clear()
        if buffer.strip():
            row = json.loads(buffer)
            batch.append((str(row["key"]), row.get("version_id")))
        if batch:
            engine().append_restore_job_objects(job.id, batch)
        completed = engine().finalize_restore_job(job.id, digest.hexdigest())
        return {
            "job_id": completed.id,
            "status": completed.state,
            "object_count": completed.object_count,
        }
    except json.JSONDecodeError as error:
        raise HTTPException(status_code=422, detail="Restore manifest is not newline JSON") from error
    except (ValueError, SimulatedDataIntegrityError) as error:
        raise HTTPException(status_code=422, detail=str(error)) from error
    except LookupError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error


@app.post("/v1/cloud/source/describe-restore-batch")
def source_describe_restore_batch(payload: DescribeRestoreBatchRequest) -> dict:
    try:
        return engine().describe_restore_job(
            payload.job_id, payload.continuation_token, payload.max_results
        ).model_dump(mode="json")
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error
    except LookupError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error


@app.post("/v1/cloud/source/read-range")
def source_read_range(payload: ReadRangeRequest) -> StreamingResponse:
    try:
        chunks = engine().read_range(
            str(payload.context.execution_id),
            payload.object.bucket,
            payload.object.key,
            payload.offset,
            payload.length,
            payload.allow_archived_for_audit,
        )
        return StreamingResponse(
            chunks,
            media_type="application/octet-stream",
            headers={"Content-Length": str(payload.length)},
        )
    except LookupError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    except PermissionError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@app.post("/v1/cloud/destination/read-range")
def destination_read_range(payload: ReadRangeRequest) -> StreamingResponse:
    try:
        chunks = engine().read_range(
            str(payload.context.execution_id),
            payload.object.bucket,
            payload.object.key,
            payload.offset,
            payload.length,
        )
        return StreamingResponse(
            chunks,
            media_type="application/octet-stream",
            headers={"Content-Length": str(payload.length)},
        )
    except LookupError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error


@app.put("/v1/cloud/destination/put-object")
async def destination_put_object(
    request: Request, x_raijin_metadata: str = Header(alias="X-Raijin-Metadata")
) -> dict:
    payload = decode_metadata(x_raijin_metadata, PutObjectRequest)
    received = await consume_request(request, payload.object.size_bytes)
    try:
        return engine().put_object_evidence(
            str(payload.context.execution_id),
            payload.object.bucket,
            payload.object.key,
            received,
            payload.source_checksum_sha256,
            str(payload.idempotency_key),
        ).model_dump(mode="json")
    except (ValueError, SimulatedDataIntegrityError) as error:
        raise HTTPException(status_code=422, detail=str(error)) from error
    except LookupError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error


@app.post("/v1/cloud/destination/logical-transfer")
def destination_logical_transfer(payload: LogicalTransferRequest) -> dict:
    try:
        return engine().transfer_logically(
            str(payload.context.execution_id),
            payload.source.bucket,
            payload.destination_bucket,
            payload.source.key,
            payload.size_bytes,
            payload.allocated_rate_mbps,
            payload.active_workers,
            payload.network_operation_key,
            str(payload.idempotency_key),
        ).model_dump(mode="json")
    except SimulatedNetworkUnavailable as error:
        # An outage selected by a scenario is a first-class, retryable
        # simulator outcome.  It must never be collapsed into HTTP 500.
        raise HTTPException(
            status_code=503,
            detail={
                "code": "SIMULATED_NETWORK_UNAVAILABLE",
                "message": str(error),
                "retry_after_virtual_seconds": error.retry_after_virtual_seconds,
            },
        ) from error
    except PermissionError as error:
        # This is an expected operational state, not an internal simulator
        # fault: the temporary restored copy is no longer available.
        raise HTTPException(
            status_code=409,
            detail=f"{error}. A new restore requires explicit operator approval because it may incur AWS charges.",
        ) from error
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error
    except LookupError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error


@app.post("/v1/cloud/destination/create-multipart")
def destination_create_multipart(payload: MultipartCreateRequest) -> dict:
    try:
        upload_id = engine().create_multipart(
            str(payload.context.execution_id),
            payload.object.bucket,
            payload.object.key,
            payload.object.size_bytes,
            payload.part_size_bytes,
            payload.object.checksum,
            str(payload.idempotency_key),
        )
        return {"upload_id": upload_id}
    except (ValueError, SimulatedDataIntegrityError) as error:
        raise HTTPException(status_code=422, detail=str(error)) from error
    except LookupError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error


@app.put("/v1/cloud/destination/upload-part")
async def destination_upload_part(
    request: Request, x_raijin_metadata: str = Header(alias="X-Raijin-Metadata")
) -> dict:
    payload = decode_metadata(x_raijin_metadata, MultipartPartRequest)
    received = await consume_request(request, payload.size_bytes)
    try:
        return engine().upload_part_evidence(
            payload.upload_id,
            payload.part_number,
            received,
            payload.checksum_sha256,
        ).model_dump(mode="json")
    except (ValueError, SimulatedDataIntegrityError) as error:
        raise HTTPException(status_code=422, detail=str(error)) from error
    except LookupError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error


@app.post("/v1/cloud/destination/commit-multipart")
def destination_commit_multipart(payload: MultipartCommitRequest) -> dict:
    try:
        return engine().commit_multipart(
            payload.upload_id,
            payload.parts,
            payload.full_checksum_sha256,
            str(payload.idempotency_key),
        ).model_dump(mode="json")
    except (ValueError, SimulatedDataIntegrityError) as error:
        raise HTTPException(status_code=422, detail=str(error)) from error
    except LookupError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
