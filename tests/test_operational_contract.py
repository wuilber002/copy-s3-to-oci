import os
from pathlib import Path

import pytest
from pydantic import ValidationError
from sqlalchemy import BigInteger, create_engine, select
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker


@compiles(BigInteger, "sqlite")
def _compile_big_integer_as_sqlite_integer(_type, _compiler, **_kwargs):
    """Keep PostgreSQL BIGINT models auto-incrementable in SQLite contract tests."""
    return "INTEGER"


_password = Path("/tmp/raijin-test-password-contract")
_password.write_text("test-password", encoding="utf-8")
os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("POSTGRES_PASSWORD_FILE", str(_password))
os.environ.setdefault("OCI_RUNTIME_CONFIG_FILE", "/tmp/raijin-test-oci-runtime.json")

from datetime import datetime, timedelta, timezone

from app.main import AWS_CONNECTION_SCHEMA_VERSION, AwsConnection, Base, CostPricing, CostPricingUpdate, DiscoveryChange, DiscoveryJob, DynamicPipelineRun, DynamicWaveCreate, Event, GlobalAwsPricing, LegacySourceConnectionMigration, OCI_VAULT_SECRET_SEARCH_QUERY, ObjectRecord, ObjectState, RestoreAttempt, RestoreObjectResult, RuntimeSettings, RuntimeSettingsUpdate, Source, SourcePrefix, SourceTransferStrategyUpdate, Task, TaskState, Wave, active_source_scope_conflicts, automatic_dynamic_duration_limit, create_dynamic_waves, delete_unexecuted_source_data, destination_provenance_matches, dynamic_schedule_times, dynamic_wave_plan, list_sources, normalize_source_prefixes, observability, parse_aws_connection_payload, percentile_75, predict_object_transfer_seconds, prometheus_metrics, public_s3_rates_from_catalog, public_transfer_rates_from_catalog, replan_dynamic_pipeline, restore_availability_poll_delay_seconds, restore_queue_details, restore_result_diagnostics, safe_aws_error_summary, safe_oci_error_summary, source_key_in_scope, source_transfer_strategy_locked, wave_cost_estimate
from app.real_worker import GOVERNANCE_TASK_KINDS, TRANSFER_TASK_KINDS, restore_expiry_from_head_response, restored_from_head_response, restored_pending_archives_from_head, should_poll_restore_with_head, task_kinds_for_role, validate_restore_preflight


def test_object_model_contains_durable_multipart_checkpoint_fields():
    columns = ObjectRecord.__table__.columns
    assert {"multipart_upload_id", "multipart_part_size", "multipart_parts_json", "multipart_updated_at"} <= set(columns.keys())


def test_source_model_has_a_durable_discovery_page_checkpoint():
    columns = Source.__table__.columns
    assert {"discovery_continuation_token", "discovery_prefix_index", "discovery_pages_completed", "discovery_objects_inserted", "discovery_started_at", "discovery_elapsed_seconds"} <= set(columns.keys())
    assert {"source_id", "prefix"} <= set(SourcePrefix.__table__.columns.keys())
    worker = Path("app/real_worker.py").read_text(encoding="utf-8")
    assert 'request["ContinuationToken"] = continuation_token' in worker
    assert "DISCOVERY_CHECKPOINT_PAGES = 10" in worker
    assert "prefixes = source_prefix_values(source)" in worker
    assert "session.bulk_insert_mappings(ObjectRecord, pending_rows)" in worker
    assert "Atomically persist a bounded discovery batch" in worker


def test_source_transfer_strategy_is_durable_and_only_accepts_known_modes():
    assert "transfer_strategy" in Source.__table__.columns
    assert SourceTransferStrategyUpdate(transfer_strategy="AFTER_ALL_RESTORED").transfer_strategy == "AFTER_ALL_RESTORED"
    assert SourceTransferStrategyUpdate(transfer_strategy="AS_OBJECTS_AVAILABLE").transfer_strategy == "AS_OBJECTS_AVAILABLE"
    with pytest.raises(ValidationError):
        SourceTransferStrategyUpdate(transfer_strategy="anything")


def test_source_scope_supports_multiple_non_overlapping_prefixes():
    assert normalize_source_prefixes(["Operational_test/", "Smoke_test/"]) == ["Operational_test/", "Smoke_test/"]
    with pytest.raises(Exception):
        normalize_source_prefixes(["Smoke_test/", "Smoke_test/nested/"])
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(name="multi-prefix", s3_bucket="source", aws_region="us-east-1", destination_bucket="destination")
        session.add(source); session.flush()
        session.add_all([SourcePrefix(id=1, source_id=source.id, prefix="Smoke_test/"), SourcePrefix(id=2, source_id=source.id, prefix="Operational_test/")])
        session.flush()
        assert source_key_in_scope(source, "Smoke_test/file.bin")
        assert source_key_in_scope(source, "Operational_test/file.bin")
        assert not source_key_in_scope(source, "Resilience_test/file.bin")


def test_source_selector_exposes_one_operational_status_per_source():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        discovered = Source(id=901, name="discovered", s3_bucket="bucket-1", aws_region="us-east-1", destination_bucket="destination", status="DISCOVERED")
        queued = Source(id=902, name="queued", s3_bucket="bucket-2", aws_region="us-east-1", destination_bucket="destination", status="DISCOVERED")
        transferring = Source(id=903, name="transferring", s3_bucket="bucket-3", aws_region="us-east-1", destination_bucket="destination", status="DISCOVERED")
        restoring = Source(id=904, name="restoring", s3_bucket="bucket-4", aws_region="us-east-1", destination_bucket="destination", status="DISCOVERED")
        session.add_all([discovered, queued, transferring, restoring]); session.flush()
        session.add_all([
            Wave(id=901, source_id=queued.id, name="queued", max_bytes=1, restore_days=1, restore_tier="BULK", status="READY_FOR_RESTORE"),
            Wave(id=902, source_id=transferring.id, name="transferring", max_bytes=1, restore_days=1, restore_tier="BULK", status="TRANSFERRING"),
            Wave(id=903, source_id=restoring.id, name="restoring", max_bytes=1, restore_days=1, restore_tier="BULK", status="RESTORING"),
        ])
        session.commit()
        statuses = {row["name"]: row["operational_status"] for row in list_sources(session)}
        assert statuses == {"discovered": "DISCOVERED", "queued": "QUEUED", "transferring": "TRANSFERRING", "restoring": "RESTORING"}


def test_global_actionable_failure_banner_ignores_archived_sources():
    app_source = Path("app/main.py").read_text(encoding="utf-8")
    assert "Source.archived_at.is_(None)" in app_source
    assert "Task.id == latest_task_id" in app_source


def test_active_sources_cannot_silently_share_an_s3_prefix_scope():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        parent = Source(name="parent", s3_bucket="shared", aws_region="us-east-1", destination_bucket="destination")
        child = Source(name="child", s3_bucket="shared", aws_region="us-east-1", destination_bucket="destination")
        archived = Source(name="archived", s3_bucket="shared", aws_region="us-east-1", destination_bucket="destination", archived_at=datetime.now(timezone.utc))
        session.add_all([parent, child, archived]); session.flush()
        session.add_all([
            SourcePrefix(id=20, source_id=parent.id, prefix="app/"),
            SourcePrefix(id=21, source_id=child.id, prefix="other/"),
            SourcePrefix(id=22, source_id=archived.id, prefix="app/images/"),
        ])
        session.flush()
        conflicts = active_source_scope_conflicts(session, "shared", ["app/images/archives/"], child.id)
        assert [(item["source_name"], item["existing_prefix"]) for item in conflicts] == [("parent", "app/")]
        assert not active_source_scope_conflicts(session, "shared", ["unrelated/"], child.id)


def test_transfer_strategy_locks_when_a_wave_enters_the_pipeline():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(name="strategy-lock", s3_bucket="source", aws_region="us-east-1", destination_bucket="destination")
        session.add(source); session.flush()
        wave = Wave(id=1, source_id=source.id, name="draft", max_bytes=1, restore_days=1, restore_tier="BULK", status="PLANNED")
        session.add(wave); session.flush()
        assert not source_transfer_strategy_locked(session, source.id)
        wave.status = "RESTORE_SCHEDULED"
        session.flush()
        assert source_transfer_strategy_locked(session, source.id)


def test_governance_and_transfer_workers_claim_disjoint_durable_responsibilities():
    assert GOVERNANCE_TASK_KINDS == {"SUBMIT_BATCH_RESTORE", "POLL_RESTORE", "VERIFY_WAVE"}
    assert TRANSFER_TASK_KINDS == {"TRANSFER_WAVE"}
    assert GOVERNANCE_TASK_KINDS.isdisjoint(TRANSFER_TASK_KINDS)
    assert task_kinds_for_role("governance") == GOVERNANCE_TASK_KINDS
    assert task_kinds_for_role("transfer") == TRANSFER_TASK_KINDS
    assert task_kinds_for_role("all") is None
    worker = Path("app/real_worker.py").read_text(encoding="utf-8")
    assert "def ensure_transfer_task" in worker
    assert "RESTORE_PARTIALLY_AVAILABLE" in worker
    assert "PARTIAL_TRANSFER_COMPLETED" in worker


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


def test_rediscovery_preserves_migration_evidence_and_requires_a_justification():
    assert {"last_discovery_mode", "discovery_generation"} <= set(Source.__table__.columns.keys())
    assert {"is_rediscovery", "justification", "objects_new", "objects_updated", "objects_changed"} <= set(DiscoveryJob.__table__.columns.keys())
    assert {"source_id", "object_key", "change_type", "previous_size_bytes", "current_size_bytes"} <= set(DiscoveryChange.__table__.columns.keys())
    app_source = Path("app/main.py").read_text(encoding="utf-8")
    assert '@app.post("/api/sources/{source_id}/rediscovery")' in app_source
    assert '@app.post("/api/sources/{source_id}/rediscovery/inventory/manifest")' in app_source
    assert "Rediscovery justification must contain more than 10 characters" in app_source
    assert "Do not silently repoint" in app_source


def test_modified_source_reprocessing_creates_an_eligible_successor_revision():
    object_columns = set(ObjectRecord.__table__.columns.keys())
    change_columns = set(DiscoveryChange.__table__.columns.keys())
    assert {"is_current_revision", "previous_object_id", "superseded_at"} <= object_columns
    assert {"reprocessed_object_id", "reprocessed_at", "current_version_id", "current_last_modified"} <= change_columns
    app_source = Path("app/main.py").read_text(encoding="utf-8")
    assert '@app.post("/api/sources/{source_id}/discovery-changes/reprocess-modified")' in app_source
    assert "Validate OCI destination after the latest rediscovery" in app_source
    assert "MODIFIED_SOURCE_REPROCESS_QUEUED" in app_source
    page = Path("app/static/index.html").read_text(encoding="utf-8")
    assert "Preparar nova transferência" in page
    assert "reprocessModifiedDiscoveryObjects" in page


def test_deleting_an_unexecuted_source_removes_all_derived_records_in_order():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(name="delete-preview", s3_bucket="source", aws_region="us-east-1", destination_bucket="destination")
        session.add(source); session.flush()
        # SQLite only auto-increments an exact INTEGER primary key; production
        # uses PostgreSQL sequences for these BIGINT identifiers.
        job = DiscoveryJob(id=100, source_id=source.id)
        run = DynamicPipelineRun(id=101, source_id=source.id)
        session.add_all([job, run]); session.flush()
        wave = Wave(id=102, source_id=source.id, pipeline_run_id=run.id, name="planned", max_bytes=1, restore_days=1, restore_tier="BULK")
        session.add(wave); session.flush()
        obj = ObjectRecord(id=103, source_id=source.id, object_key="key", size_bytes=1, wave_id=wave.id, state=ObjectState.WAVE_ASSIGNED)
        session.add(obj); session.flush()
        attempt = RestoreAttempt(id=104, wave_id=wave.id, aws_region="us-east-1")
        session.add(attempt); session.flush()
        session.add_all([
            RestoreObjectResult(id=105, attempt_id=attempt.id, object_id=obj.id), Task(id=106, wave_id=wave.id, kind="SUBMIT_BATCH_RESTORE"),
            DiscoveryChange(id=107, source_id=source.id, discovery_job_id=job.id, object_key="key"),
            Event(id=108, source_id=source.id, wave_id=wave.id, kind="TEST", message="preview"),
        ])
        session.commit()
        deleted = delete_unexecuted_source_data(session, source.id)
        session.delete(source); session.commit()
        assert deleted["discovery_jobs"] == 1
        assert deleted["objects"] == 1
        assert session.scalar(select(Source.id).where(Source.id == source.id)) is None
        assert session.scalar(select(DiscoveryJob.id).where(DiscoveryJob.source_id == source.id)) is None


def test_restore_diagnostics_distinguish_already_in_progress_from_real_failures():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(id=201, name="diagnosis", s3_bucket="source", aws_region="us-east-1", destination_bucket="destination")
        wave = Wave(id=202, source_id=source.id, name="wave", max_bytes=3, restore_days=1, restore_tier="BULK")
        session.add_all([source, wave]); session.flush()
        objects = [ObjectRecord(id=203 + index, source_id=source.id, wave_id=wave.id,
                                object_key=f"key-{index}", size_bytes=1) for index in range(3)]
        session.add_all(objects); session.flush()
        attempt = RestoreAttempt(id=206, wave_id=wave.id, aws_region="us-east-1", expected_objects=3)
        session.add(attempt); session.flush()
        session.add_all([
            RestoreObjectResult(id=207, attempt_id=attempt.id, object_id=objects[0].id, task_status="SUCCEEDED"),
            RestoreObjectResult(id=208, attempt_id=attempt.id, object_id=objects[1].id, task_status="FAILED",
                                http_status=409, error_code="RestoreAlreadyInProgress",
                                error_message="Object restore is already in progress"),
            RestoreObjectResult(id=209, attempt_id=attempt.id, object_id=objects[2].id, task_status="FAILED",
                                http_status=403, error_code="AccessDenied", error_message="Access denied"),
        ])
        session.commit()
        diagnosis = restore_result_diagnostics(session, attempt.id)
        assert diagnosis["raw_succeeded"] == 1
        assert diagnosis["accepted_equivalent"] == 1
        assert diagnosis["effective_accepted"] == 2
        assert diagnosis["unexpected_failed"] == 1
        assert diagnosis["action_required"] is True
        assert diagnosis["reasons"][0]["code"] in {"AccessDenied", "RestoreAlreadyInProgress"}
        access_denied = next(reason for reason in diagnosis["reasons"] if reason["code"] == "AccessDenied")
        assert access_denied["sample_keys"] == ["key-2"]


def test_restore_diagnostics_accept_an_already_running_restore_without_resubmission():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(id=211, name="overlap", s3_bucket="source", aws_region="us-east-1", destination_bucket="destination")
        wave = Wave(id=212, source_id=source.id, name="wave", max_bytes=1, restore_days=1, restore_tier="BULK")
        obj = ObjectRecord(id=213, source_id=source.id, wave_id=wave.id, object_key="shared/key", size_bytes=1)
        attempt = RestoreAttempt(id=214, wave_id=wave.id, aws_region="us-east-1", expected_objects=1)
        session.add_all([source, wave, obj, attempt]); session.flush()
        session.add(RestoreObjectResult(id=215, attempt_id=attempt.id, object_id=obj.id, task_status="FAILED",
                                        http_status=409, error_code="RestoreAlreadyInProgress",
                                        error_message="Object restore is already in progress"))
        session.commit()
        diagnosis = restore_result_diagnostics(session, attempt.id)
        assert diagnosis["effective_accepted"] == 1
        assert diagnosis["unexpected_failed"] == 0
        assert diagnosis["action_required"] is False
        assert "do not submit another restore job" in diagnosis["recommended_action"]


def test_frontend_marks_rediscovery_as_a_controlled_operation():
    page = Path("app/static/index.html").read_text(encoding="utf-8")
    assert 'id="source-discovery-action"' in page
    assert "Executar re-discovery" in page
    assert "rediscovery-justification" in page
    assert "discovery-origin-tag" in page


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


def test_restore_poll_is_targeted_to_pending_wave_objects():
    assert should_poll_restore_with_head(10, 1_000)
    assert should_poll_restore_with_head(10_000, 1_000_000)
    assert restored_from_head_response({"Restore": 'ongoing-request="false", expiry-date="Fri, 22 Aug 2026 00:00:00 GMT"'})
    assert not restored_from_head_response({"Restore": 'ongoing-request="true"'})
    assert restore_expiry_from_head_response({"Restore": 'ongoing-request="false", expiry-date="Fri, 22 Aug 2026 00:00:00 GMT"'}).isoformat() == "2026-08-22T00:00:00+00:00"


def test_restore_poll_uses_the_source_bucket_after_orm_checkpoint_safety_refactor():
    calls = []

    class S3:
        def head_object(self, **kwargs):
            calls.append(kwargs)
            return {"Restore": 'ongoing-request="false", expiry-date="Fri, 22 Aug 2026 00:00:00 GMT"'}

    connection = type("Connection", (), {"restore_poll_requests_per_second": 10, "restore_poll_concurrency": 10})()
    source = type("Source", (), {"s3_bucket": "archive-source", "aws_connection": connection})()
    obj = type("Object", (), {"id": 17, "object_key": "prefix/file.bin", "version_id": None})()
    ready, metrics = restored_pending_archives_from_head(S3(), source, [obj])
    assert calls == [{"Bucket": "archive-source", "Key": "prefix/file.bin"}]
    assert ready[17].isoformat() == "2026-08-22T00:00:00+00:00"
    assert metrics["requests"] == 1


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


def test_obsolete_source_region_sync_endpoint_is_not_exposed():
    source = Path("app/main.py").read_text(encoding="utf-8")
    assert "/sync-aws-region" not in source
    assert "def sync_source_aws_region" not in source


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


def test_source_creation_and_update_require_the_connection_region():
    source = Path("app/main.py").read_text(encoding="utf-8")
    assert source.count("Source AWS region is defined by the selected AWS connection") == 2
    assert source.count("payload.aws_region != connection.default_region") == 2


def test_prometheus_contract_contains_safe_operational_metrics(monkeypatch):
    class Data:
        def __getitem__(self, key):
            return {"tasks": {"failed": 2, "retrying": 3, "stale_leases": 4}, "transfers": {"active_multipart_checkpoints": 5, "stalled": 6}, "events": {"failures_last_24h": 7}, "restore_expiry": {"at_risk": 9, "waves": []}, "disk": {"free_bytes": 8}}[key]

    monkeypatch.setattr("app.main.observability", lambda _session: Data())
    response = prometheus_metrics(object())
    assert response.media_type.startswith("text/plain")
    assert "raijin_failed_tasks 2" in response.body.decode()
    body = response.body.decode()
    assert "raijin_active_multipart_checkpoints 5" in body
    assert "raijin_stale_task_leases 4" in body
    assert "raijin_stalled_transfers 6" in body
    assert "raijin_failures_last_24h 7" in body
    assert "raijin_restore_expiry_risk_waves 9" in body


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
    assert RuntimeSettingsUpdate(**payload).laboratory_mode_enabled is False
    assert "laboratory_mode_enabled" in RuntimeSettings.__table__.columns
    with pytest.raises(ValidationError):
        RuntimeSettingsUpdate(**(payload | {"multipart_part_size_mib": 15}))
    with pytest.raises(ValidationError):
        RuntimeSettingsUpdate(**(payload | {"multipart_part_size_mib": 513}))


def test_dynamic_wave_contract_keeps_prediction_and_scheduling_durable():
    assert {"planned_transfer_seconds"} <= set(ObjectRecord.__table__.columns.keys())
    from app.main import DynamicPipelineRun, Wave, RuntimeSettings
    assert {"planner_mode", "pipeline_run_id", "predicted_transfer_seconds", "prediction_samples", "planned_restore_at", "planned_transfer_start_at"} <= set(Wave.__table__.columns.keys())
    assert {"source_id", "planner_version", "status", "target_max_bytes", "transfer_strategy", "restore_horizon_waves", "completed_at"} <= set(DynamicPipelineRun.__table__.columns.keys())
    assert {"dynamic_wave_target_seconds", "dynamic_wave_max_objects", "dynamic_restore_safety_seconds", "dynamic_restore_horizon_waves", "dynamic_pipeline_enabled"} <= set(RuntimeSettings.__table__.columns.keys())
    payload = DynamicWaveCreate(restore_days=3, restore_tier="BULK")
    assert payload.restore_days == 3
    assert automatic_dynamic_duration_limit(1) == (16 * 3600, 8 * 3600)
    assert automatic_dynamic_duration_limit(2) == (36 * 3600, 12 * 3600)


def test_dynamic_planner_uses_scalar_boundaries_and_assigns_every_object_once():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(
            id=951,
            name="large-plan-contract",
            s3_bucket="source",
            aws_region="us-east-1",
            destination_bucket="destination",
            status="DISCOVERED",
        )
        session.add(source)
        session.flush()
        session.add_all([
            ObjectRecord(
                id=960 + index,
                source_id=source.id,
                object_key=f"prefix/object-{index:04d}.bin",
                size_bytes=1024 + index,
                state=ObjectState.DISCOVERED,
            )
            for index in range(37)
        ])
        session.commit()

        plan = dynamic_wave_plan(
            session,
            source.id,
            max_bytes=10 * 1024**4,
            target_transfer_seconds=16 * 3600,
            max_objects=500_000,
        )
        assert sum(item["object_count"] for item in plan["waves"]) == 37
        assert all("objects" not in item for item in plan["waves"])

        response = create_dynamic_waves(
            source.id,
            DynamicWaveCreate(
                restore_days=1,
                restore_tier="BULK",
                schedule_restores=False,
            ),
            session,
        )
        assert response["objects"] == 37
        assigned = list(session.execute(
            select(ObjectRecord.wave_id, ObjectRecord.state).where(
                ObjectRecord.source_id == source.id
            )
        ))
        assert len(assigned) == 37
        assert all(wave_id is not None and state == ObjectState.WAVE_ASSIGNED for wave_id, state in assigned)
        assert session.scalar(select(Wave).where(Wave.source_id == source.id)).planner_mode == "DYNAMIC"


def test_dynamic_flight_board_uses_local_planned_and_actual_wave_timing():
    source = Path("app/main.py").read_text(encoding="utf-8")
    frontend = Path("app/static/index.html").read_text(encoding="utf-8")
    assert '@app.get("/api/flight-board")' in source
    assert '@app.get("/api/flight-board/availability")' in source
    assert 'Wave.planner_mode == "DYNAMIC"' in source
    assert '"QUEUE"' in source and '"RESTORE"' in source and '"READY"' in source and '"TRANSFER"' in source
    assert 'id="flight-board-button"' in frontend
    assert 'function showFlightBoard()' in frontend
    assert 'flight-board-legend' in frontend
    assert 'forecast_restore = phase("RESTORE", restore_end, expected_available_at, planned=True)' in source
    assert 'timeline_start = min(submitted_points)' in source
    assert '#flight-board-modal .modal-panel{width:min(1500px,calc(100vw - 2rem));overflow-x:hidden}' in frontend
    assert '.flight-board-chart{overflow:hidden;max-height:none}' in frontend
    assert '.flight-board-track{position:relative;height:30px;border-radius:4px;background:#0b1220;min-width:0}' in frontend
    assert '.flight-board-tick.last{left:auto!important;right:0;transform:none' in frontend
    assert '.flight-board-tick.last .flight-board-tick-label{left:auto;right:.25rem}' in frontend
    assert '@app.get("/api/sources/{source_id}/pipeline-history")' in source
    assert 'id="pipeline-history-card"' in frontend
    assert 'function loadPipelineHistory()' in frontend


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
    assert transfer_at == now + __import__("datetime").timedelta(hours=48)


def test_dynamic_schedule_uses_safety_to_advance_later_restore_not_delay_first_wave():
    now = datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc)
    plans = [
        {"restore_tier": "BULK", "predicted_transfer_seconds": 60 * 3600},
        {"restore_tier": "BULK", "predicted_transfer_seconds": 3600},
    ]
    first, second = dynamic_schedule_times(now, plans, 6 * 3600)
    assert first == (now, now + __import__("datetime").timedelta(hours=48))
    assert second[1] == now + __import__("datetime").timedelta(hours=108)
    assert second[0] == now + __import__("datetime").timedelta(hours=54)


def test_dynamic_scheduler_uses_a_durable_restore_horizon_and_not_all_jobs_at_once():
    source = Path("app/main.py").read_text(encoding="utf-8")
    worker = Path("app/real_worker.py").read_text(encoding="utf-8")
    assert "def release_dynamic_restore_horizon" in source
    assert "restore_horizon_waves" in source
    assert "release_dynamic_restore_horizon(session, result[\"settings\"])" in source
    assert "release_dynamic_restore_horizon(session, settings)" in worker


def test_dynamic_replan_preserves_submitted_waves_and_exposes_forecast():
    source = Path("app/main.py").read_text(encoding="utf-8")
    worker = Path("app/real_worker.py").read_text(encoding="utf-8")
    assert "def replan_dynamic_pipeline" in source
    assert 'Task.kind == "SUBMIT_BATCH_RESTORE"' in source
    assert 'if wave.status != "RESTORE_SCHEDULED" or has_batch_task:' in source
    assert '"pipeline_completion_at"' in source
    assert "replan_dynamic_pipeline(session, settings)" in worker


def test_dynamic_replan_uses_control_mode_logical_elapsed_time():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    # SQLite intentionally returns naive datetimes even for timezone-aware
    # columns; production PostgreSQL keeps the UTC offset.
    initial = datetime(2026, 8, 25, 12, 0)
    with Session() as session:
        source = Source(
            id=971,
            name="logical-replan",
            s3_bucket="sim-source",
            aws_region="us-east-1",
            destination_bucket="sim-destination",
            backend_kind="SIMULATED",
            simulation_fidelity="CONTROL",
        )
        settings = RuntimeSettings(
            id=1,
            transfer_workers=2,
            max_throughput_mbps=1100,
            dynamic_pipeline_enabled=True,
        )
        run = DynamicPipelineRun(
            id=972,
            source_id=source.id,
            status="SCHEDULED",
            scheduled_restores=True,
            restore_safety_seconds=0,
        )
        completed = Wave(
            id=973,
            source_id=source.id,
            pipeline_run_id=run.id,
            name="wave-001",
            max_bytes=1,
            restore_days=1,
            restore_tier="BULK",
            status="COMPLETED",
            planner_mode="DYNAMIC",
            predicted_transfer_seconds=60,
            planned_transfer_start_at=initial,
        )
        future = Wave(
            id=974,
            source_id=source.id,
            pipeline_run_id=run.id,
            name="wave-002",
            max_bytes=1,
            restore_days=1,
            restore_tier="BULK",
            status="RESTORE_SCHEDULED",
            planner_mode="DYNAMIC",
            predicted_transfer_seconds=60,
            planned_transfer_start_at=initial + timedelta(seconds=60),
            planned_restore_at=initial,
        )
        session.add_all([source, settings, run, completed, future])
        session.flush()
        session.add_all([
            ObjectRecord(
                id=975,
                source_id=source.id,
                wave_id=completed.id,
                object_key="a.bin",
                size_bytes=1,
                state=ObjectState.TRANSFERRED,
                transfer_started_at=initial,
                transferred_at=initial,
                transfer_elapsed_seconds=300,
            ),
            ObjectRecord(
                id=976,
                source_id=source.id,
                wave_id=completed.id,
                object_key="b.bin",
                size_bytes=1,
                state=ObjectState.TRANSFERRED,
                transfer_started_at=initial,
                transferred_at=initial,
                transfer_elapsed_seconds=300,
            ),
        ])
        session.flush()
        changed = replan_dynamic_pipeline(session, settings, now=initial)
        assert changed == 1
        assert future.planned_transfer_start_at == initial + timedelta(seconds=300)
        event = session.scalar(select(Event).where(Event.kind == "DYNAMIC_WAVE_REPLANNED"))
        assert event is not None
        assert "simulated logical transfer duration" in event.message


def test_connection_api_limits_are_durable_and_used_by_workers():
    source = Path("app/main.py").read_text(encoding="utf-8")
    worker = Path("app/real_worker.py").read_text(encoding="utf-8")
    assert {"discovery_requests_per_second", "restore_poll_requests_per_second", "restore_poll_concurrency"} <= set(AwsConnection.__table__.columns.keys())
    assert "/operational-limits" in source
    assert 'getattr(connection, "discovery_requests_per_second"' in worker
    assert 'getattr(connection, "restore_poll_requests_per_second"' in worker


def test_long_restore_polling_heartbeats_its_lease_and_checkpoints_progress():
    worker = Path("app/real_worker.py").read_text(encoding="utf-8")
    assert "def task_lease_heartbeat" in worker
    assert "Task.worker_id == WORKER_ID" in worker
    assert "progress_callback=persist_poll_progress" in worker
    assert "RESTORE_POLL_HEAD_BATCH_SIZE = 1_000" in worker
    assert "wave.last_availability_poll_objects" in worker


def test_restore_completion_evidence_is_idempotent_and_request_metrics_are_durable():
    worker = Path("app/real_worker.py").read_text(encoding="utf-8")
    source = Path("app/main.py").read_text(encoding="utf-8")
    poll = worker[worker.index("def poll_restore"):worker.index("\n\nDIRECT_SHA_LIMIT")]
    assert "if attempt.completed_at is None or attempt.report_manifest_key is None:" in poll
    assert "restore_result_diagnostics(session, attempt.id)" in poll
    assert "RestoreAlreadyInProgress" in source
    assert "attempt.batch_describe_requests" in poll
    assert "attempt.completion_report_list_requests" in poll
    assert "completion_report_get_requests" in source
    assert "attempt and attempt.job_id and not attempt.failure_summary" in worker
    assert "/api/waves/{wave_id}/retry-restore-evidence" in source


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


def test_restore_availability_polling_starts_at_two_hours_then_converges_to_thirty_minutes():
    accepted = datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)
    assert restore_availability_poll_delay_seconds(accepted, accepted, "BULK") == 7200
    assert restore_availability_poll_delay_seconds(accepted, accepted + timedelta(hours=24), "BULK") == 3600
    assert restore_availability_poll_delay_seconds(accepted, accepted + timedelta(hours=36), "BULK") == 1800
    assert restore_availability_poll_delay_seconds(accepted, accepted + timedelta(hours=2), "BULK", partial_availability=True) == 1800
    assert restore_availability_poll_delay_seconds(accepted, accepted + timedelta(hours=2), "BULK", partial_availability=True, transfer_strategy="AS_OBJECTS_AVAILABLE") == 300
    assert restore_availability_poll_delay_seconds(accepted, accepted + timedelta(hours=2), "BULK", partial_availability=True, transfer_strategy="AS_OBJECTS_AVAILABLE", pending_objects=5_001) == 600
    assert restore_availability_poll_delay_seconds(accepted, accepted + timedelta(hours=2), "BULK", partial_availability=True, transfer_strategy="AS_OBJECTS_AVAILABLE", pending_objects=50_001) == 1800


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
