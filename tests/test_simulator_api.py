import base64
import json
import socket
import threading
import time
import uuid
from urllib.request import Request, urlopen

from sqlalchemy import create_engine
import uvicorn

from app.backend_contracts import (
    ExecutionContext,
    ListObjectsRequest,
    ObjectDescriptor,
    PutObjectRequest,
    ReadRangeRequest,
    RestoreObjectRequest,
)
from app.simulation_migrations import migrate
from app import simulator
from app.simulated_data import consume_and_discard


def test_simulator_api_exposes_versioned_end_to_end_data_path(tmp_path, monkeypatch):
    database_url = f"sqlite+pysqlite:///{tmp_path / 'simulation.db'}"
    migrate(create_engine(database_url))
    monkeypatch.setenv("RAIJIN_SIMULATOR_DATABASE_URL", database_url)
    simulator.store.cache_clear()
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    port = listener.getsockname()[1]
    listener.close()
    server = uvicorn.Server(
        uvicorn.Config(simulator.app, host="127.0.0.1", port=port, log_level="error")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{port}"
    for _ in range(100):
        if server.started:
            break
        time.sleep(0.01)

    def get(path):
        with urlopen(f"{base_url}{path}") as response:
            return response.status, json.loads(response.read())

    def post(path, payload):
        request = Request(
            f"{base_url}{path}",
            data=json.dumps(payload).encode(),
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urlopen(request) as response:
            body = response.read()
            return response.status, json.loads(body) if body else None

    status, handshake = get("/v1/handshake")
    assert status == 200
    assert handshake["operations_enabled"] is True
    assert "discarding-destination-v1" in handshake["capabilities"]

    _, scenario = post(
        "/v1/scenarios",
        {
            "name": "api-data-path",
            "fidelity": "DATA",
            "seed": "api-seed",
            "logical_size_bytes": 4096,
            "configuration": {
                "bulk_restore_min_hours": 0,
                "bulk_restore_max_hours": 0,
            },
        },
    )
    status, _ = post(
        f"/v1/scenarios/{scenario['id']}/materialize",
        {
            "source_bucket": "source",
            "destination_bucket": "destination",
            "region": "us-east-1",
            "object_count": 1,
            "logical_size_bytes": 4096,
            "prefixes": ["smoke"],
            "storage_class": "DEEP_ARCHIVE",
        },
    )
    assert status == 201
    _, execution = post(f"/v1/scenarios/{scenario['id']}/executions", {})
    context = ExecutionContext(
        tenant_id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        execution_id=execution["id"],
        correlation_id=execution["correlation_id"],
    )

    status, listing = post(
        "/v1/cloud/source/list-objects",
        ListObjectsRequest(context=context, bucket="source").model_dump(mode="json"),
    )
    assert status == 200
    item = ObjectDescriptor.model_validate(listing["objects"][0])

    _, restore = post(
        "/v1/cloud/source/restore-object",
        RestoreObjectRequest(
            context=context,
            object=item,
            tier="BULK",
            retention_days=1,
            idempotency_key=uuid.uuid4(),
        ).model_dump(mode="json"),
    )
    assert restore["accepted"] is True

    read_request = ReadRangeRequest(
        context=context, object=item, offset=0, length=item.size_bytes
    )
    read_http_request = Request(
        f"{base_url}/v1/cloud/source/read-range",
        data=read_request.model_dump_json().encode(),
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urlopen(read_http_request) as response:
        content = response.read()
    checksum = consume_and_discard([content]).checksum_sha256
    target = item.model_copy(update={"bucket": "destination", "checksum": checksum})
    put_request = PutObjectRequest(
        context=context,
        object=target,
        source_checksum_sha256=checksum,
        idempotency_key=uuid.uuid4(),
    )
    metadata = base64.urlsafe_b64encode(put_request.model_dump_json().encode()).decode()
    upload_http_request = Request(
        f"{base_url}/v1/cloud/destination/put-object",
        data=content,
        method="PUT",
        headers={"X-Raijin-Metadata": metadata, "Content-Type": "application/octet-stream"},
    )
    with urlopen(upload_http_request) as response:
        uploaded_status = response.status
        uploaded = json.loads(response.read())

    assert uploaded_status == 200
    assert uploaded["checksum_sha256"] == checksum

    _, clone = post(
        f"/v1/executions/{execution['id']}/clone",
        {"name": "api-data-path-replay"},
    )
    clone_context = context.model_copy(
        update={
            "execution_id": uuid.UUID(clone["execution"]["id"]),
            "correlation_id": uuid.UUID(clone["execution"]["correlation_id"]),
        }
    )
    _, clone_listing = post(
        "/v1/cloud/source/list-objects",
        ListObjectsRequest(context=clone_context, bucket="source").model_dump(mode="json"),
    )
    clone_item = ObjectDescriptor.model_validate(clone_listing["objects"][0])
    assert clone_item.key == item.key
    assert clone_item.size_bytes == item.size_bytes
    clone_read = ReadRangeRequest(
        context=clone_context, object=clone_item, offset=0, length=clone_item.size_bytes,
        allow_archived_for_audit=True,
    )
    clone_http_request = Request(
        f"{base_url}/v1/cloud/source/read-range",
        data=clone_read.model_dump_json().encode(),
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urlopen(clone_http_request) as response:
        clone_content = response.read()
    assert clone_content == content
    _, report = get(f"/v1/executions/{execution['id']}/report")
    assert report["fidelity"] == "DATA"
    assert report["physical_bytes_processed"] == 4096
    assert report["operation_counts"]["READ_RANGE"] >= 1
    assert report["operation_counts"]["PUT_OBJECT"] == 1
    assert report["virtual_objects"] == 2
    server.should_exit = True
    thread.join(timeout=5)
    simulator.store.cache_clear()
