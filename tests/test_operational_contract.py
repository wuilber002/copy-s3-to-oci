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

from app.main import AWS_CONNECTION_SCHEMA_VERSION, CostPricing, CostPricingUpdate, DiscoveryJob, DynamicWaveCreate, GlobalAwsPricing, LegacySourceConnectionMigration, OCI_VAULT_SECRET_SEARCH_QUERY, ObjectRecord, ObjectState, RestoreAttempt, RestoreObjectResult, RuntimeSettingsUpdate, Source, SourceTransferStrategyUpdate, TaskState, destination_provenance_matches, dynamic_schedule_times, observability, parse_aws_connection_payload, percentile_75, predict_object_transfer_seconds, prometheus_metrics, public_s3_rates_from_catalog, public_transfer_rates_from_catalog, restore_queue_details, safe_aws_error_summary, safe_oci_error_summary, wave_cost_estimate
from app.real_worker import restore_expiry_from_head_response, restored_from_head_response, should_poll_restore_with_head, validate_restore_preflight


def test_object_model_contains_durable_multipart_checkpoint_fields():
    columns = ObjectRecord.__table__.columns
    assert {"multipart_upload_id", "multipart_part_size", "multipart_parts_json", "multipart_updated_at"} <= set(columns.keys())


def test_source_model_has_a_durable_discovery_page_checkpoint():
    columns = Source.__table__.columns
    assert {"discovery_continuation_token", "discovery_pages_completed", "discovery_objects_inserted", "discovery_started_at", "discovery_elapsed_seconds"} <= set(columns.keys())
    worker = Path("app/real_worker.py").read_text(encoding="utf-8")
    assert 'request["ContinuationToken"] = continuation_token' in worker
    assert "DISCOVERY_CHECKPOINT_PAGES = 10" in worker
    assert "session.bulk_insert_mappings(ObjectRecord, pending_rows)" in worker
    assert "Atomically persist a bounded discovery batch" in worker


def test_source_transfer_strategy_is_durable_and_only_accepts_known_modes():
    assert "transfer_strategy" in Source.__table__.columns
    assert SourceTransferStrategyUpdate(transfer_strategy="AFTER_ALL_RESTORED").transfer_strategy == "AFTER_ALL_RESTORED"
    assert SourceTransferStrategyUpdate(transfer_strategy="AS_OBJECTS_AVAILABLE").transfer_strategy == "AS_OBJECTS_AVAILABLE"
    with pytest.raises(ValidationError):
        SourceTransferStrategyUpdate(transfer_strategy="anything")


def test_remote_discovery_has_a_durable_observable_queue_and_adaptive_throttle():
    columns = DiscoveryJob.__table__.columns
    assert {"source_id", "state", "available_at", "lease_expires_at", "attempts", "error", "completed_at"} <= set(columns.keys())
    worker = Path("app/real_worker.py").read_text(encoding="utf-8")
    assert "def claim_discovery_job" in worker
    assert "DISCOVERY_REQUEST_INTERVAL_SECONDS = 0.1" in worker
    assert "DISCOVERY_MAX_THROTTLE_RETRIES = 5" in worker
    assert "time.sleep(min(30, 2 ** throttle_attempt))" in worker
    app_source = Path("app/main.py").read_text(encoding="utf-8")
    assert '@app.get("/api/discovery-queue")' in app_source
    assert "Remote discovery cannot replace or merge an existing inventory" in app_source


def test_large_inventory_uses_keyset_pagination_and_production_indexes():
    indexes = {index.name for index in ObjectRecord.__table__.indexes}
    assert {"ix_objects_source_key_id", "ix_objects_source_state_key_id"} <= indexes
    app_source = Path("app/main.py").read_text(encoding="utf-8")
    assert "Read inventory with keyset pagination" in app_source
    assert 'ObjectRecord.object_key > after_key' in app_source
    script = Path("scripts/create-discovery-indexes.sh").read_text(encoding="utf-8")
    assert "CREATE INDEX CONCURRENTLY" in script


def test_s3_inventory_manifest_is_streamed_by_a_durable_discovery_job():
    columns = DiscoveryJob.__table__.columns
    assert {"mode", "inventory_manifest_uri", "inventory_file_index", "inventory_rows_completed"} <= set(columns.keys())
    worker = Path("app/real_worker.py").read_text(encoding="utf-8")
    assert "def import_s3_inventory_manifest" in worker
    assert "S3 Inventory shard" in worker
    assert "job.inventory_rows_completed = row_number" in worker
    assert 'elif discovery_job.mode == "S3_INVENTORY_MANIFEST"' in worker
    app_source = Path("app/main.py").read_text(encoding="utf-8")
    assert '@app.post("/api/sources/{source_id}/inventory/manifest")' in app_source


def test_inventory_file_import_is_batched_and_never_calls_aws_discovery():
    source = Path("app/main.py").read_text(encoding="utf-8")
    start = source.index("def upload_inventory_file")
    end = source.index("\n\n@app.post(\"/api/sources/{source_id}/discovery\")", start)
    handler = source[start:end]
    assert "bulk_insert_mappings(ObjectRecord, pending)" in handler
    assert "gzip.GzipFile" in handler
    assert "object_key/Key and size_bytes/Size" in handler
    assert '"INVENTORY_FILE_IMPORTED"' in handler
    assert "without AWS discovery" in handler


def test_discovery_worker_recovers_an_interrupted_checkpoint_without_counting_downtime():
    worker = Path("app/real_worker.py").read_text(encoding="utf-8")
    assert 'Source.status == "DISCOVERING"' in worker
    assert 'pending_source.status = "DISCOVERY_QUEUED"' in worker
    assert '"DISCOVERY_RECOVERED"' in worker
    assert "exact end unknowable" in worker


def test_startup_backfills_discovery_duration_for_historical_sources():
    source = Path("app/main.py").read_text(encoding="utf-8")
    assert "EXTRACT(EPOCH FROM (discovery_completed_at - discovery_requested_at))" in source
    assert "julianday(discovery_completed_at)" in source


def test_postgres_recovery_tool_requires_an_explicit_backup_and_confirmation():
    script = Path("scripts/restore-postgres.sh").read_text(encoding="utf-8")
    assert "--confirm-restore" in script
    assert "--backup" in script
    assert '[[ "$backup_file_real" == "$backup_root_real"/* ]]' in script
    assert "/usr/local/sbin/s3-oci-backup-postgres" in script
    assert "pg_restore" in script


def test_restore_poll_prefers_targeted_head_only_for_small_wave_subsets():
    assert should_poll_restore_with_head(10, 1_000)
    assert not should_poll_restore_with_head(11, 1_000)
    assert should_poll_restore_with_head(10_000, 1_000_000)
    assert not should_poll_restore_with_head(10_001, 1_000_000)
    assert restored_from_head_response({"Restore": 'ongoing-request="false", expiry-date="Fri, 22 Aug 2026 00:00:00 GMT"'})
    assert not restored_from_head_response({"Restore": 'ongoing-request="true"'})
    assert restore_expiry_from_head_response({"Restore": 'ongoing-request="false", expiry-date="Fri, 22 Aug 2026 00:00:00 GMT"'}).isoformat() == "2026-08-22T00:00:00+00:00"


def test_restore_models_preserve_attempt_and_per_object_evidence():
    assert {"restore_attempt_id", "restore_requested_at", "restored_at", "restore_expires_at"} <= set(ObjectRecord.__table__.columns.keys())
    assert {"job_id", "expected_objects", "succeeded_objects", "failed_objects", "report_manifest_key"} <= set(RestoreAttempt.__table__.columns.keys())
    assert {"attempt_id", "object_id", "task_status", "http_status", "error_code"} <= set(RestoreObjectResult.__table__.columns.keys())
    assert ObjectState.RESTORE_REQUEST_ACCEPTED == "RESTORE_REQUEST_ACCEPTED"


def test_wave_list_exposes_durable_restore_timing_without_per_wave_queries():
    source = Path("app/main.py").read_text(encoding="utf-8")
    start = source.index("def list_waves")
    end = source.index("\n\n@app.delete(\"/api/waves/{wave_id}\")", start)
    handler = source[start:end]
    assert "timing_by_wave" in handler
    assert '"restore_timing": timing_by_wave.get(wave.id, {})' in handler
    frontend = Path("app/static/index.html").read_text(encoding="utf-8")
    assert "const restoreLabel=x=>" in frontend
    assert "Restore em andamento" in frontend


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


def test_real_worker_imports_the_runtime_namespace_reader_used_by_transfer_and_audit():
    worker = Path("app/real_worker.py").read_text(encoding="utf-8")
    assert "read_oci_runtime_config" in worker[worker.index("from app.main import ("):worker.index(")\n\n# The bootstrap")]


def test_wave_report_returns_tasks_in_durable_execution_order():
    source = Path("app/main.py").read_text(encoding="utf-8")
    report = source[source.index("def wave_report"):source.index("\n\n@app.get(\"/api/waves/{wave_id}/cost-estimate\")")]
    assert 'select(Task).where(Task.wave_id == wave_id).order_by(Task.id)' in report
    assert 'for t in tasks' in report


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


def test_dynamic_wave_contract_keeps_prediction_and_scheduling_durable():
    assert {"planned_transfer_seconds"} <= set(ObjectRecord.__table__.columns.keys())
    from app.main import Wave, RuntimeSettings
    assert {"planner_mode", "predicted_transfer_seconds", "prediction_samples", "planned_restore_at", "planned_transfer_start_at"} <= set(Wave.__table__.columns.keys())
    assert {"dynamic_wave_target_seconds", "dynamic_wave_max_objects", "dynamic_restore_safety_seconds", "dynamic_pipeline_enabled"} <= set(RuntimeSettings.__table__.columns.keys())
    payload = DynamicWaveCreate(max_bytes=1024, target_transfer_seconds=3600, max_objects=10,
                                restore_days=3, restore_tier="BULK")
    assert payload.target_transfer_seconds == 3600


def test_dynamic_prediction_prefers_p75_history_then_conservative_link_model():
    settings = type("Settings", (), {"multipart_part_size_mib": 64, "max_throughput_mbps": 1000, "transfer_workers": 4})()
    obj = type("Object", (), {"size_bytes": 512 * 1024})()
    historical, samples = predict_object_transfer_seconds(obj, settings, {"up_to_1_mib": {"samples": 5, "p75_seconds": 3.5}})
    fallback, fallback_samples = predict_object_transfer_seconds(obj, settings, {})
    assert (historical, samples) == (3.5, 5)
    assert fallback > 0 and fallback_samples == 0
    assert percentile_75([1, 2, 3, 4]) == 3


def test_dynamic_schedule_starts_bulk_restore_before_predicted_transfer_window():
    now = datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc)
    times = dynamic_schedule_times(now, [{"restore_tier": "BULK", "predicted_transfer_seconds": 3600}], 6 * 3600)
    restore_at, transfer_at = times[0]
    assert restore_at == now
    assert transfer_at == now + __import__("datetime").timedelta(hours=6)


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
    assert pricing.include_oci_costs is True
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


def test_public_aws_price_list_preserves_sub_cent_request_rates():
    def product(sku, attributes, price):
        return ({"sku": sku, "attributes": attributes}, {sku: {"term": {"priceDimensions": {"rate": {"pricePerUnit": {"USD": str(price)}}}}}})

    products, terms = {}, {}
    for sku, attributes, price in (
        ("batch-job", {"feeCode": "S3-BatchOperations-Jobs"}, 0.25),
        ("batch-object", {"feeDescription": "Per object fee for object operations performed by Batch Operations"}, 0.000001),
        ("tier1", {"group": "S3-API-Tier1"}, 0.000007),
        ("tier2", {"group": "S3-API-Tier2"}, 0.00000056),
    ):
        item, item_terms = product(sku, attributes, price)
        products[sku] = item
        terms.update(item_terms)
    rates = public_s3_rates_from_catalog({"products": products, "terms": {"OnDemand": terms}})
    assert rates["aws_batch_job_usd"] == pytest.approx(0.25)
    assert rates["aws_batch_object_usd_per_1000"] == pytest.approx(0.001)
    assert rates["aws_s3_put_list_usd_per_1000"] == pytest.approx(0.007)
    assert rates["aws_s3_get_usd_per_1000"] == pytest.approx(0.00056)


def test_public_pricing_uses_standard_storage_and_batch_object_operation_not_similar_skus():
    def product(sku, attributes, price):
        return ({"sku": sku, "attributes": attributes}, {sku: {"term": {"priceDimensions": {"rate": {"pricePerUnit": {"USD": str(price)}}}}}})

    products, terms = {}, {}
    for sku, attributes, price in (
        ("infrequent", {"storageClass": "Infrequent Access", "usagetype": "TimedStorage-SIA-ByteHrs"}, 0.0125),
        ("standard", {"storageClass": "General Purpose", "usagetype": "TimedStorage-ByteHrs"}, 0.023),
        ("manifest", {"feeDescription": "Per object fee to generate Batch Operations Manifest"}, 0.000000015),
        ("object-operation", {"feeDescription": "Per object fee for object operations performed by Batch Operations"}, 0.000001),
    ):
        item, item_terms = product(sku, attributes, price)
        products[sku] = item
        terms.update(item_terms)
    rates = public_s3_rates_from_catalog({"products": products, "terms": {"OnDemand": terms}})
    assert rates["aws_restore_temp_standard_usd_per_gib_month"] == pytest.approx(0.023 * (1024**3 / 1_000_000_000))
    assert rates["aws_batch_object_usd_per_1000"] == pytest.approx(0.001)


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
    assert "if pricing.include_oci_costs:" in estimator
    assert '"total_completeness"' in estimator
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
    assert '"total_completeness"' in handler


def test_public_price_refresh_covers_active_source_regions_including_legacy_sources():
    source = Path("app/main.py").read_text(encoding="utf-8")
    start = source.index("def active_pricing_regions")
    handler = source[start:source.index("\n\ndef refresh_due_global_aws_pricing", start)]
    assert "Source.aws_region" in handler
    assert "AwsConnection.default_region" in handler
