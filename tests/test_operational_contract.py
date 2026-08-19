import os
from pathlib import Path

import pytest
from pydantic import ValidationError


_password = Path("/tmp/raijin-test-password-contract")
_password.write_text("test-password", encoding="utf-8")
os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("POSTGRES_PASSWORD_FILE", str(_password))
os.environ.setdefault("OCI_RUNTIME_CONFIG_FILE", "/tmp/raijin-test-oci-runtime.json")

from datetime import datetime, timezone

from app.main import AWS_CONNECTION_SCHEMA_VERSION, CostPricing, CostPricingUpdate, GlobalAwsPricing, LegacySourceConnectionMigration, OCI_VAULT_SECRET_SEARCH_QUERY, ObjectRecord, ObjectState, RestoreAttempt, RestoreObjectResult, RuntimeSettingsUpdate, Source, TaskState, destination_provenance_matches, observability, parse_aws_connection_payload, prometheus_metrics, public_s3_rates_from_catalog, public_transfer_rates_from_catalog, restore_queue_details, safe_aws_error_summary, safe_oci_error_summary, wave_cost_estimate
from app.real_worker import validate_restore_preflight


def test_object_model_contains_durable_multipart_checkpoint_fields():
    columns = ObjectRecord.__table__.columns
    assert {"multipart_upload_id", "multipart_part_size", "multipart_parts_json", "multipart_updated_at"} <= set(columns.keys())


def test_restore_models_preserve_attempt_and_per_object_evidence():
    assert {"restore_attempt_id"} <= set(ObjectRecord.__table__.columns.keys())
    assert {"job_id", "expected_objects", "succeeded_objects", "failed_objects", "report_manifest_key"} <= set(RestoreAttempt.__table__.columns.keys())
    assert {"attempt_id", "object_id", "task_status", "http_status", "error_code"} <= set(RestoreObjectResult.__table__.columns.keys())
    assert ObjectState.RESTORE_REQUEST_ACCEPTED == "RESTORE_REQUEST_ACCEPTED"


def test_restore_preflight_blocks_source_region_mismatch_before_batch_submission():
    class Client:
        def head_bucket(self, Bucket):
            return {"ResponseMetadata": {"HTTPHeaders": {"x-amz-bucket-region": "sa-east-1"}}}

    source = type("Source", (), {"s3_bucket": "source", "aws_region": "us-east-1", "aws_bucket_region": None})()
    with pytest.raises(RuntimeError, match="region mismatch"):
        validate_restore_preflight(None, source, Client(), {"control_bucket": "control"})


def test_source_region_sync_uses_base_task_update_and_invalidates_old_restore_state():
    source = Path("app/main.py").read_text(encoding="utf-8")
    start = source.index("def sync_source_aws_region")
    end = source.index("\n\n@app.post(\"/api/sources/{source_id}/migrate-aws-connection\")", start)
    handler = source[start:end]
    assert "Task.id.in_(pending_task_ids)" in handler
    assert "RESTORE_INVALIDATED_BY_REGION_SYNC" in handler
    assert "INVALIDATED_REGION_MISMATCH" in handler


def test_dashboard_keeps_superseded_region_tasks_in_audit_but_not_as_active_alerts():
    source = Path("app/main.py").read_text(encoding="utf-8")
    start = source.index("def operations_overview")
    end = source.index("\n\ndef restore_queue_details", start)
    handler = source[start:end]
    assert 'task_counts["ACTIONABLE_FAILED"]' in handler
    assert "latest_task_id" in handler


def test_restore_submission_reports_the_failed_aws_stage_without_exposing_details():
    worker = Path("app/real_worker.py").read_text(encoding="utf-8")
    assert "Restore preflight failed:" in worker
    assert "Control-bucket manifest upload failed:" in worker
    assert "S3 Batch Operations job creation failed:" in worker


def test_restore_polling_uses_the_same_control_prefix_as_submission_and_labels_report_errors():
    worker = Path("app/real_worker.py").read_text(encoding="utf-8")
    poll = worker[worker.index("def poll_restore"):worker.index("\n\ndef sha256_b64")]
    assert "operation = aws_operation_config(source, settings)" in poll
    assert "S3 Batch completion-report processing failed:" in poll
    assert "Raijin polling/report processing failed after AWS accepted" in worker


def test_source_creation_and_region_sync_require_the_connection_region():
    source = Path("app/main.py").read_text(encoding="utf-8")
    assert source.count("Source AWS region is defined by the selected AWS connection") == 2
    assert "Source bucket region differs from its AWS connection" in source


def test_prometheus_contract_contains_safe_operational_metrics(monkeypatch):
    class Data:
        def __getitem__(self, key):
            return {"tasks": {"failed": 2, "retrying": 3, "stale_leases": 4}, "transfers": {"active_multipart_checkpoints": 5, "stalled": 6}, "events": {"failures_last_24h": 7}, "disk": {"free_bytes": 8}}[key]

    monkeypatch.setattr("app.main.observability", lambda _session: Data())
    response = prometheus_metrics(object())
    assert response.media_type.startswith("text/plain")
    assert "raijin_failed_tasks 2" in response.body.decode()
    body = response.body.decode()
    assert "raijin_active_multipart_checkpoints 5" in body
    assert "raijin_stale_task_leases 4" in body
    assert "raijin_stalled_transfers 6" in body
    assert "raijin_failures_last_24h 7" in body


def test_destination_provenance_requires_s3_etag_and_last_modified_when_known():
    obj = type("Object", (), {"etag": "source-etag", "last_modified": datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)})()
    headers = {"opc-meta-s3-oci-source-etag": "source-etag", "opc-meta-s3-oci-source-last-modified": "2026-08-17T12:00:00+00:00"}
    assert destination_provenance_matches(obj, headers)
    headers["opc-meta-s3-oci-source-last-modified"] = "2026-08-17T12:01:00+00:00"
    assert not destination_provenance_matches(obj, headers)


def test_multipart_size_runtime_setting_has_safe_bounds():
    payload = {
        "transfer_workers": 4, "max_throughput_mbps": 1000, "multipart_part_size_mib": 64,
        "default_wave_size_bytes": 1024, "default_restore_days": 7, "default_restore_tier": "BULK",
        "task_lease_seconds": 300,
    }
    assert RuntimeSettingsUpdate(**payload).multipart_part_size_mib == 64
    assert RuntimeSettingsUpdate(**payload).cost_estimation_enabled is False
    with pytest.raises(ValidationError):
        RuntimeSettingsUpdate(**(payload | {"multipart_part_size_mib": 15}))
    with pytest.raises(ValidationError):
        RuntimeSettingsUpdate(**(payload | {"multipart_part_size_mib": 513}))


def test_aws_error_summary_exposes_only_status_and_code():
    error = type("ClientError", (Exception,), {"response": {"ResponseMetadata": {"HTTPStatusCode": 403}, "Error": {"Code": "AccessDenied", "Message": "sensitive detail"}}})()
    assert safe_aws_error_summary(error) == "ClientError (403 AccessDenied)"


def test_oci_error_summary_excludes_request_details():
    error = type("ServiceError", (Exception,), {"status": 404, "code": "NotAuthorizedOrNotFound", "message": "secret OCID must stay private"})()
    assert safe_oci_error_summary(error) == "ServiceError (404 NotAuthorizedOrNotFound)"


def test_restore_queue_contract_exposes_durable_batch_wait_details_only():
    now = datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)
    wave = type("Wave", (), {"batch_job_id": "job-123", "batch_job_status": "Preparing", "status": "RESTORING", "last_poll_at": now, "poll_count": 3})()
    task = type("Task", (), {"kind": "POLL_RESTORE", "state": TaskState.READY, "available_at": now, "created_at": datetime(2026, 8, 17, 11, 30, tzinfo=timezone.utc), "error": "Batch job status is Preparing"})()
    detail = restore_queue_details(wave, task, now)
    assert detail["batch_job_id"] == "job-123"
    assert detail["batch_status"] == "Preparing"
    assert detail["poll_count"] == 3
    assert detail["waiting_seconds"] == 1800
    assert detail["next_attempt_at"] == now


def test_aws_connection_secret_schema_requires_matching_account_role_arns():
    payload = {
        "schema_version": AWS_CONNECTION_SCHEMA_VERSION, "connection_name": "Finance Production",
        "aws_account_id": "123456789012", "default_region": "us-east-1",
        "bootstrap_access_key_id": "AKIAEXAMPLE", "bootstrap_secret_access_key": "secret",
        "migration_role_arn": "arn:aws:iam::123456789012:role/migration",
        "batch_operations_role_arn": "arn:aws:iam::123456789012:role/batch",
        "control_bucket": "finance-raijin-control",
    }
    assert parse_aws_connection_payload(__import__("json").dumps(payload))["connection_name"] == "Finance Production"
    payload["migration_role_arn"] = "arn:aws:iam::000000000000:role/migration"
    with pytest.raises(ValueError, match="aws_account_id"):
        parse_aws_connection_payload(__import__("json").dumps(payload))


def test_oci_secret_discovery_uses_the_vaultsecret_resource_type():
    assert OCI_VAULT_SECRET_SEARCH_QUERY == "query vaultsecret resources"


def test_legacy_source_adoption_requires_a_concrete_connection_id():
    assert LegacySourceConnectionMigration(aws_connection_id=1).aws_connection_id == 1
    with pytest.raises(ValidationError):
        LegacySourceConnectionMigration(aws_connection_id=0)


def test_cost_pricing_keeps_rates_optional_but_non_negative():
    pricing = CostPricingUpdate()
    assert pricing.currency == "USD"
    assert pricing.include_aws_transfer_out is True
    assert pricing.aws_deep_archive_bulk_retrieval_usd_per_gib is None
    with pytest.raises(ValidationError):
        CostPricingUpdate(aws_transfer_out_usd_per_gib=-0.01)
    assert CostPricing.__tablename__ == "cost_pricing"


def test_public_aws_price_list_maps_only_supported_s3_rates():
    def product(sku, group, price):
        return ({"sku": sku, "attributes": {"group": group}}, {sku: {"term": {"priceDimensions": {"rate": {"pricePerUnit": {"USD": str(price)}}}}}})

    products, terms = {}, {}
    for sku, group, price in (
        ("tier1", "S3-API-Tier1", 0.005),
        ("deep", "S3-GlacierDeepArchive-Retrieval-Bulk", 0.0025),
    ):
        item, item_terms = product(sku, group, price)
        products[sku] = item
        terms.update(item_terms)
    rates = public_s3_rates_from_catalog({"products": products, "terms": {"OnDemand": terms}})
    assert rates["aws_s3_put_list_usd_per_1000"] == pytest.approx(5)
    assert rates["aws_deep_archive_bulk_retrieval_usd_per_gib"] == pytest.approx(0.00268435456)
    assert GlobalAwsPricing.__tablename__ == "global_aws_pricing"


def test_public_aws_transfer_catalog_uses_only_external_egress_rate():
    catalog = {"products": {
        "mrap": {"sku": "mrap", "attributes": {"toLocation": "External", "transferType": "InterRegion Outbound"}},
        "global": {"sku": "global", "attributes": {"fromLocation": "Global", "toLocation": "External", "transferType": "AWS Outbound"}},
        "egress": {"sku": "egress", "attributes": {"fromLocation": "South America (Sao Paulo)", "toLocation": "External", "transferType": "AWS Outbound"}},
    }, "terms": {"OnDemand": {
        "mrap": {"term": {"priceDimensions": {"rate": {"pricePerUnit": {"USD": "0.0033"}}}}},
        "global": {"term": {"priceDimensions": {"rate": {"pricePerUnit": {"USD": "0"}}}}},
        "egress": {"term": {"priceDimensions": {"rate": {"pricePerUnit": {"USD": "0.15"}}}}},
    }}}
    rates = public_transfer_rates_from_catalog(catalog)
    assert rates["aws_transfer_out_usd_per_gib"] == pytest.approx(0.1610612736)


def test_cost_pricing_refresh_settings_have_safe_defaults_and_bounds():
    payload = {
        "transfer_workers": 4, "max_throughput_mbps": 1000, "multipart_part_size_mib": 64,
        "default_wave_size_bytes": 1024, "default_restore_days": 7, "default_restore_tier": "BULK",
        "task_lease_seconds": 300,
    }
    assert RuntimeSettingsUpdate(**payload).cost_pricing_auto_refresh_enabled is True
    assert RuntimeSettingsUpdate(**payload).cost_pricing_refresh_days == 7
    with pytest.raises(ValidationError):
        RuntimeSettingsUpdate(**(payload | {"cost_pricing_refresh_days": 0}))


def test_wave_cost_estimator_documents_transparent_unit_assumptions():
    source = Path("app/main.py").read_text(encoding="utf-8")
    start = source.index("def wave_cost_estimate")
    end = source.index("\n\ndef aws_connection_configuration", 0)
    if end < start:
        end = len(source)
    estimator = source[start:end]
    for component in ("S3 Batch Operations job", "AWS data transfer out to OCI", "Optional deep SHA-256 audit OCI reads"):
        assert component in estimator
    assert "pricing.expected_restore_poll_cycles" in estimator
    assert "if pricing.include_aws_transfer_out:" in estimator
    assert "never a promise" in estimator


def test_cost_estimate_endpoint_is_explicitly_gated_by_operational_setting():
    source = Path("app/main.py").read_text(encoding="utf-8")
    start = source.index('def get_wave_cost_estimate')
    handler = source[start:source.index('\n\n@app.get("/api/waves/{wave_id}/deep-audit-preview")', start)]
    assert "cost_estimation_enabled" in handler
    assert "Cost estimation is disabled in operational settings" in handler


def test_source_cost_endpoint_uses_created_waves_and_reports_unassigned_inventory():
    source = Path("app/main.py").read_text(encoding="utf-8")
    start = source.index('def get_source_cost_estimate')
    handler = source[start:source.index('\n\n@app.get("/api/waves/{wave_id}/deep-audit-preview")', start)]
    assert "wave_cost_estimate" in handler
    assert "unassigned_objects" in handler
    assert '"currency"' in handler
