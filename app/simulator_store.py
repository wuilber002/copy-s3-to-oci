"""Repository for the external simulator catalog.

No control-plane table or queue model is imported here. The simulator only
owns the ``sim_*`` catalog and exposes it through its versioned HTTP contract.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import uuid

from sqlalchemy import Engine, URL, create_engine, delete, func, select
from sqlalchemy.orm import Session, sessionmaker

from app.runtime_context import RuntimeIsolationError, database_name
from app.simulation_migrations import current_revision
from app.simulation_schema import (
    DEFAULT_DATA_PHYSICAL_BUDGET_BYTES,
    SIMULATION_SCHEMA_VERSION,
    ExecutionState,
    GeneratorRelease,
    InjectedFault,
    ScenarioState,
    ScenarioTemplate,
    SimulatedMultipartUpload,
    SimulatedMultipartPart,
    SimulatedOperation,
    SimulatedRestoreObjectResult,
    SimulatedRestoreJob,
    SimulationClock,
    SimulationExecution,
    SimulationScenario,
    SimulationTombstone,
    VirtualBucket,
    VirtualObject,
)


class ScenarioConflictError(ValueError):
    """Raised when an immutable scenario name is already reserved."""


def _canonical_json(value: dict | list) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _database_url_with_password(raw_url: str, password_file: str | None) -> str:
    if not password_file:
        return raw_url
    password = Path(password_file).read_text(encoding="utf-8").strip()
    parsed = URL.create(raw_url) if "://" not in raw_url else None
    if parsed is not None:  # pragma: no cover - defensive; URLs normally contain ://.
        return parsed.set(password=password).render_as_string(hide_password=False)

    from sqlalchemy.engine import make_url

    return make_url(raw_url).set(password=password).render_as_string(hide_password=False)


def simulator_database_url(environ: dict[str, str] | None = None) -> str:
    values = os.environ if environ is None else environ
    raw_url = values.get("RAIJIN_SIMULATOR_DATABASE_URL") or values.get("DATABASE_URL") or ""
    if not raw_url:
        raise RuntimeIsolationError("RAIJIN_SIMULATOR_DATABASE_URL is required")
    if database_name(raw_url) != "migration_simulation" and not raw_url.startswith("sqlite"):
        raise RuntimeIsolationError("Simulator requires the migration_simulation database")
    forbidden = [
        name
        for name in (
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "OCI_CLI_CONFIG_FILE",
            "OCI_CLI_KEY_FILE",
        )
        if values.get(name)
    ]
    if forbidden:
        raise RuntimeIsolationError(
            "Simulator refuses cloud credentials: " + ", ".join(sorted(forbidden))
        )
    return _database_url_with_password(
        raw_url, values.get("RAIJIN_SIMULATOR_POSTGRES_PASSWORD_FILE")
    )


@dataclass(frozen=True)
class ScenarioCreate:
    name: str
    fidelity: str
    seed: str
    logical_size_bytes: int
    physical_budget_bytes: int = DEFAULT_DATA_PHYSICAL_BUDGET_BYTES
    clock_acceleration: float = 3600.0
    retention_days: int = 60
    quarantine_days: int = 30
    template_id: str | None = None
    template_snapshot: dict | None = None
    configuration: dict | None = None
    fault_rules: list | None = None


@dataclass(frozen=True)
class TemplateWrite:
    name: str
    description: str
    fidelity: str
    configuration: dict
    fault_rules: list


class SimulatorStore:
    def __init__(self, engine: Engine):
        self.engine = engine
        self.sessions = sessionmaker(bind=engine, expire_on_commit=False)

    @classmethod
    def from_environ(cls, environ: dict[str, str] | None = None) -> "SimulatorStore":
        return cls(create_engine(simulator_database_url(environ), pool_pre_ping=True))

    def require_current_schema(self) -> None:
        revision = current_revision(self.engine)
        if revision != SIMULATION_SCHEMA_VERSION:
            raise RuntimeError(
                f"Simulator schema revision {revision}; expected {SIMULATION_SCHEMA_VERSION}. "
                "Run scripts/migrate-simulation.py before enabling operations."
            )

    def ensure_generator_release(self) -> None:
        """Register the executable generator version without deleting history."""
        self.require_current_schema()
        from app.simulated_data import GENERATOR_VERSION

        with self.sessions() as session:
            release = session.get(GeneratorRelease, GENERATOR_VERSION)
            if release is None:
                session.add(GeneratorRelease(version=GENERATOR_VERSION, state="ACTIVE"))
            else:
                release.state = "ACTIVE"
                release.deprecated_at = None
                release.purge_eligible_at = None
            session.commit()

    def apply_generator_housekeeping(
        self, now: datetime | None = None, quarantine_days: int = 30
    ) -> dict:
        """Mark legacy code removable only after its final scenario reference.

        This changes catalog markers only. Removing an implementation from the
        source tree always requires a later reviewed software release.
        """
        self.require_current_schema()
        from app.simulated_data import GENERATOR_VERSION

        current = _aware(now or datetime.now(timezone.utc))
        deprecated = purge_eligible = 0
        with self.sessions() as session:
            releases = list(session.scalars(select(GeneratorRelease).order_by(
                GeneratorRelease.created_at, GeneratorRelease.version
            )))
            for release in releases:
                references = int(session.scalar(select(func.count(SimulationScenario.id)).where(
                    SimulationScenario.generator_version == release.version,
                    SimulationScenario.state != ScenarioState.PURGED.value,
                )) or 0)
                release.referenced_scenarios = references
                if release.version == GENERATOR_VERSION or references:
                    release.state = "ACTIVE"
                    release.deprecated_at = None
                    release.purge_eligible_at = None
                    continue
                if release.state == "ACTIVE":
                    release.state = "DEPRECATED"
                    release.deprecated_at = current
                    deprecated += 1
                elif (
                    release.state == "DEPRECATED"
                    and release.deprecated_at
                    and current >= _aware(release.deprecated_at) + timedelta(days=quarantine_days)
                ):
                    release.state = "PURGE_ELIGIBLE"
                    release.purge_eligible_at = current
                    purge_eligible += 1
            session.commit()
        return {"deprecated": deprecated, "purge_eligible": purge_eligible}

    def list_generator_releases(self) -> list[dict]:
        self.require_current_schema()
        with self.sessions() as session:
            return [
                {
                    "version": item.version,
                    "state": item.state,
                    "referenced_scenarios": item.referenced_scenarios,
                    "deprecated_at": item.deprecated_at,
                    "purge_eligible_at": item.purge_eligible_at,
                }
                for item in session.scalars(select(GeneratorRelease).order_by(
                    GeneratorRelease.created_at, GeneratorRelease.version
                ))
            ]

    def create_scenario(self, request: ScenarioCreate) -> SimulationScenario:
        self.require_current_schema()
        fidelity = request.fidelity.strip().upper()
        if fidelity not in {"CONTROL", "DATA"}:
            raise ValueError("fidelity must be CONTROL or DATA")
        name = request.name.strip()
        if not name:
            raise ValueError("Scenario name is required")
        if not request.seed:
            raise ValueError("Scenario seed is required")
        if request.logical_size_bytes < 0:
            raise ValueError("Logical size cannot be negative")
        if request.physical_budget_bytes < 0:
            raise ValueError("Physical budget cannot be negative")
        if request.clock_acceleration <= 0:
            raise ValueError("Clock acceleration must be positive")
        if request.retention_days < 0 or request.quarantine_days < 0:
            raise ValueError("Retention and quarantine cannot be negative")

        with self.sessions() as session:
            if session.scalar(
                select(SimulationScenario.id).where(SimulationScenario.name == name)
            ):
                raise ScenarioConflictError(
                    f"A simulation scenario named '{name}' already exists. "
                    "Choose a unique scenario name or reuse the existing execution."
                )
            template_snapshot = request.template_snapshot or {}
            configuration = request.configuration or {}
            fault_rules = request.fault_rules or []
            if request.template_id:
                template = session.get(ScenarioTemplate, request.template_id)
                if template is None or not template.active:
                    raise ValueError("Scenario template is absent or inactive")
                fidelity = template.fidelity
                configuration = json.loads(template.configuration_json)
                fault_rules = json.loads(template.fault_rules_json)
                template_snapshot = {
                    "id": template.id,
                    "name": template.name,
                    "description": template.description,
                    "fidelity": template.fidelity,
                    "configuration": configuration,
                    "fault_rules": fault_rules,
                    "updated_at": template.updated_at.isoformat(),
                }
            item = SimulationScenario(
                name=name,
                fidelity=fidelity,
                seed=request.seed,
                logical_size_bytes=request.logical_size_bytes,
                physical_budget_bytes=(
                    request.physical_budget_bytes if fidelity == "DATA" else 0
                ),
                clock_acceleration=request.clock_acceleration,
                retention_days=request.retention_days,
                quarantine_days=request.quarantine_days,
                template_id=request.template_id,
                template_snapshot_json=_canonical_json(template_snapshot),
                configuration_json=_canonical_json(configuration),
                fault_rules_json=_canonical_json(fault_rules),
            )
            session.add(item)
            session.commit()
        return item

    def ensure_default_templates(self) -> None:
        self.require_current_schema()
        defaults = [
            ("Ideal control", "CONTROL", {"bulk_restore_min_hours": 30, "bulk_restore_max_hours": 32, "network_throughput_mbps": 1100}, []),
            ("Rapid restore", "CONTROL", {"bulk_restore_min_hours": 1, "bulk_restore_max_hours": 2, "network_throughput_mbps": 1100}, []),
            ("Slow restore", "CONTROL", {"bulk_restore_min_hours": 44, "bulk_restore_max_hours": 48, "network_throughput_mbps": 1100}, []),
            ("Gradual restore", "CONTROL", {"bulk_restore_min_hours": 24, "bulk_restore_max_hours": 48, "network_throughput_mbps": 1100}, []),
            ("Degraded network", "CONTROL", {"bulk_restore_min_hours": 30, "bulk_restore_max_hours": 36, "network_throughput_mbps": 350, "per_object_latency_ms": 25}, []),
            ("Oscillating network", "CONTROL", {"bulk_restore_min_hours": 30, "bulk_restore_max_hours": 36, "network_throughput_mbps": 1100, "network_profile_period_hours": 6, "network_profile": [{"start_hour": 0, "end_hour": 2, "throughput_mbps": 1100}, {"start_hour": 2, "end_hour": 4, "throughput_mbps": 250, "latency_ms": 50}, {"start_hour": 4, "end_hour": 5, "unavailable": True}, {"start_hour": 5, "end_hour": 6, "throughput_mbps": 700}]}, []),
            ("Partial restore failures", "CONTROL", {"bulk_restore_min_hours": 24, "bulk_restore_max_hours": 48}, [{"operation": "RESTORE", "action": "REJECT", "probability": 0.01, "error_code": "SimulatedRestoreRejected"}]),
            ("Insufficient retention", "CONTROL", {"bulk_restore_min_hours": 44, "bulk_restore_max_hours": 48, "network_throughput_mbps": 100}, []),
            ("Multipart interrupted", "DATA", {"bulk_restore_min_hours": 0, "bulk_restore_max_hours": 0}, [{"operation": "UPLOAD_PART", "action": "TIMEOUT", "part_number": 2, "attempt": 1}]),
            ("Deterministic corruption", "DATA", {"bulk_restore_min_hours": 0, "bulk_restore_max_hours": 0}, [{"operation": "READ_RANGE", "action": "CORRUPT", "attempt": 1}]),
            ("Transient throttling", "DATA", {"bulk_restore_min_hours": 0, "bulk_restore_max_hours": 0}, [{"operation": "UPLOAD_PART", "action": "TIMEOUT", "probability": 0.15, "attempt": 1}]),
        ]
        with self.sessions() as session:
            for name, fidelity, configuration, faults in defaults:
                if session.scalar(select(ScenarioTemplate.id).where(ScenarioTemplate.name == name)):
                    continue
                session.add(ScenarioTemplate(
                    name=name,
                    description=f"Built-in reproducible {name.lower()} scenario",
                    fidelity=fidelity,
                    configuration_json=_canonical_json(configuration),
                    fault_rules_json=_canonical_json(faults),
                ))
            session.commit()

    def list_templates(self) -> list[ScenarioTemplate]:
        self.require_current_schema()
        with self.sessions() as session:
            return list(session.scalars(
                select(ScenarioTemplate).where(ScenarioTemplate.active.is_(True)).order_by(ScenarioTemplate.name)
            ).all())

    @staticmethod
    def _validate_template(request: TemplateWrite) -> tuple[str, str]:
        name = request.name.strip()
        fidelity = request.fidelity.strip().upper()
        if not name:
            raise ValueError("Template name is required")
        if fidelity not in {"CONTROL", "DATA"}:
            raise ValueError("Template fidelity must be CONTROL or DATA")
        if not isinstance(request.configuration, dict):
            raise ValueError("Template configuration must be an object")
        if not isinstance(request.fault_rules, list):
            raise ValueError("Template fault rules must be an array")
        return name, fidelity

    def create_template(self, request: TemplateWrite) -> ScenarioTemplate:
        """Create an editable template; executions always consume a snapshot."""
        self.require_current_schema()
        name, fidelity = self._validate_template(request)
        with self.sessions() as session:
            if session.scalar(select(ScenarioTemplate.id).where(ScenarioTemplate.name == name)):
                raise ValueError("Scenario template name already exists")
            item = ScenarioTemplate(
                name=name,
                description=request.description.strip(),
                fidelity=fidelity,
                configuration_json=_canonical_json(request.configuration),
                fault_rules_json=_canonical_json(request.fault_rules),
                active=True,
            )
            session.add(item)
            session.commit()
            return item

    def update_template(self, template_id: str, request: TemplateWrite) -> ScenarioTemplate:
        """Edit future scenario defaults without mutating existing snapshots."""
        self.require_current_schema()
        name, fidelity = self._validate_template(request)
        with self.sessions() as session:
            item = session.get(ScenarioTemplate, template_id)
            if item is None or not item.active:
                raise LookupError("Scenario template not found")
            duplicate = session.scalar(select(ScenarioTemplate.id).where(
                ScenarioTemplate.name == name,
                ScenarioTemplate.id != template_id,
            ))
            if duplicate:
                raise ValueError("Scenario template name already exists")
            item.name = name
            item.description = request.description.strip()
            item.fidelity = fidelity
            item.configuration_json = _canonical_json(request.configuration)
            item.fault_rules_json = _canonical_json(request.fault_rules)
            item.updated_at = datetime.now(timezone.utc)
            session.commit()
            return item

    def list_scenarios(self) -> list[SimulationScenario]:
        self.require_current_schema()
        with self.sessions() as session:
            return list(
                session.scalars(
                    select(SimulationScenario).order_by(SimulationScenario.created_at.desc())
                ).all()
            )

    def get_scenario(self, scenario_id: str) -> SimulationScenario | None:
        self.require_current_schema()
        with self.sessions() as session:
            return session.get(SimulationScenario, scenario_id)

    def create_execution(self, scenario_id: str) -> SimulationExecution:
        self.require_current_schema()
        with self.sessions() as session:
            scenario = session.get(SimulationScenario, scenario_id)
            if scenario is None:
                raise LookupError("Scenario not found")
            if scenario.state != "ACTIVE":
                raise ValueError("Only ACTIVE scenarios can start an execution")
            snapshot = {
                "scenario_id": scenario.id,
                "scenario_name": scenario.name,
                "fidelity": scenario.fidelity,
                "seed": scenario.seed,
                "logical_size_bytes": scenario.logical_size_bytes,
                "physical_budget_bytes": scenario.physical_budget_bytes,
                "clock_acceleration": scenario.clock_acceleration,
                "contract_version": scenario.contract_version,
                "generator_version": scenario.generator_version,
                "template_snapshot": json.loads(scenario.template_snapshot_json),
                "configuration": json.loads(scenario.configuration_json),
                "fault_rules": json.loads(scenario.fault_rules_json),
            }
            execution = SimulationExecution(
                scenario_id=scenario.id,
                correlation_id=str(uuid.uuid4()),
                state=ExecutionState.CREATED.value,
                immutable_snapshot_json=_canonical_json(snapshot),
                physical_budget_bytes=scenario.physical_budget_bytes,
            )
            session.add(execution)
            session.flush()
            session.add(
                SimulationClock(
                    execution_id=execution.id,
                    acceleration=scenario.clock_acceleration,
                    # Virtual time advances only through durable simulator
                    # decisions (restore polling, scheduled work or injected
                    # recovery).  A free-running accelerated clock can cross
                    # a retention deadline while the real worker is simply
                    # busy with requests or streaming.
                    paused=True,
                    paused_virtual_at=datetime.now(timezone.utc),
                )
            )
            session.commit()
            return execution

    def get_execution(self, execution_id: str) -> SimulationExecution | None:
        self.require_current_schema()
        with self.sessions() as session:
            return session.get(SimulationExecution, execution_id)

    def execution_report(self, execution_id: str) -> dict:
        """Return compact, durable simulator-side evidence for one execution."""
        self.require_current_schema()
        with self.sessions() as session:
            execution = session.get(SimulationExecution, execution_id)
            if execution is None:
                raise LookupError("Simulation execution not found")
            scenario = session.get(SimulationScenario, execution.scenario_id)
            operation_counts = {
                operation: int(count)
                for operation, count in session.execute(
                    select(SimulatedOperation.operation, func.count(SimulatedOperation.id))
                    .where(SimulatedOperation.execution_id == execution_id)
                    .group_by(SimulatedOperation.operation)
                    .order_by(SimulatedOperation.operation)
                )
            }
            fault_counts = {
                fault_type: int(count)
                for fault_type, count in session.execute(
                    select(InjectedFault.fault_type, func.count(InjectedFault.id))
                    .where(InjectedFault.execution_id == execution_id)
                    .group_by(InjectedFault.fault_type)
                    .order_by(InjectedFault.fault_type)
                )
            }
            restore_jobs = {
                state: int(count)
                for state, count in session.execute(
                    select(SimulatedRestoreJob.state, func.count(SimulatedRestoreJob.id))
                    .where(SimulatedRestoreJob.execution_id == execution_id)
                    .group_by(SimulatedRestoreJob.state)
                    .order_by(SimulatedRestoreJob.state)
                )
            }
            multipart_uploads = {
                state: int(count)
                for state, count in session.execute(
                    select(SimulatedMultipartUpload.state, func.count(SimulatedMultipartUpload.id))
                    .where(SimulatedMultipartUpload.execution_id == execution_id)
                    .group_by(SimulatedMultipartUpload.state)
                    .order_by(SimulatedMultipartUpload.state)
                )
            }
            bucket_ids = list(session.scalars(select(VirtualBucket.id).where(
                VirtualBucket.scenario_id == execution.scenario_id
            )))
            virtual_objects = int(session.scalar(select(func.count(VirtualObject.id)).where(
                VirtualObject.bucket_id.in_(bucket_ids)
            )) or 0) if bucket_ids else 0
            elapsed = None
            if execution.real_started_at:
                end = execution.real_finished_at or datetime.now(timezone.utc)
                elapsed = max(0, (_aware(end) - _aware(execution.real_started_at)).total_seconds())
            return {
                "execution_id": execution.id,
                "scenario_id": execution.scenario_id,
                "scenario_name": scenario.name if scenario else None,
                "fidelity": scenario.fidelity if scenario else None,
                "state": execution.state,
                "contract_version": scenario.contract_version if scenario else None,
                "generator_version": scenario.generator_version if scenario else None,
                "real_elapsed_seconds": elapsed,
                "physical_budget_bytes": int(execution.physical_budget_bytes or 0),
                "physical_bytes_processed": int(execution.physical_bytes_processed or 0),
                "virtual_objects": virtual_objects,
                "operation_counts": operation_counts,
                "fault_counts": fault_counts,
                "restore_jobs": restore_jobs,
                "multipart_uploads": multipart_uploads,
                "real_started_at": execution.real_started_at,
                "real_finished_at": execution.real_finished_at,
                "virtual_started_at": execution.virtual_started_at,
                "virtual_finished_at": execution.virtual_finished_at,
            }

    @staticmethod
    def _clock_now(clock: SimulationClock, now: datetime | None = None) -> datetime:
        if clock.paused and clock.paused_virtual_at:
            return clock.paused_virtual_at
        if int(clock.hold_count or 0) > 0 and clock.held_virtual_at:
            return clock.held_virtual_at
        current = _aware(now or datetime.now(timezone.utc))
        anchor = clock.real_anchor_at
        if anchor.tzinfo is None:
            anchor = anchor.replace(tzinfo=timezone.utc)
        virtual = clock.virtual_anchor_at
        if virtual.tzinfo is None:
            virtual = virtual.replace(tzinfo=timezone.utc)
        return virtual + timedelta(seconds=(current - anchor).total_seconds() * clock.acceleration)

    def pause_running_clocks_after_startup(self) -> int:
        """Recover orphaned phase holds without pausing the scenario clock.

        A phase hold belongs to a live worker process. After a restart it is
        orphaned and must be cleared, but a simulator/API restart must not
        silently change an actively running scenario into a manual pause.
        """
        self.require_current_schema()
        changed = 0
        with self.sessions() as session:
            clocks = list(session.scalars(select(SimulationClock)))
            now = datetime.now(timezone.utc)
            for clock in clocks:
                current = self._clock_now(clock, now)
                if int(clock.hold_count or 0) > 0:
                    changed += 1
                    # Continue from the exact held instant after releasing
                    # the orphaned hold. A manually paused scenario remains
                    # paused; a running one remains running.
                    if clock.paused:
                        clock.paused_virtual_at = current
                    else:
                        clock.virtual_anchor_at, clock.real_anchor_at = current, now
                clock.hold_count, clock.held_virtual_at = 0, None
            session.commit()
        return changed

    def clock_status(self, execution_id: str) -> dict:
        self.require_current_schema()
        with self.sessions() as session:
            clock = session.get(SimulationClock, execution_id)
            if clock is None:
                raise LookupError("Simulation clock not found")
            return {
                "execution_id": execution_id,
                "paused": clock.paused,
                "held": int(clock.hold_count or 0) > 0,
                "hold_count": int(clock.hold_count or 0),
                "acceleration": clock.acceleration,
                "real_now": datetime.now(timezone.utc),
                "virtual_now": self._clock_now(clock),
            }

    def control_clock(self, execution_id: str, action: str, advance_seconds: float = 0) -> dict:
        self.require_current_schema()
        normalized = action.strip().upper()
        if normalized not in {"PAUSE", "RESUME", "ADVANCE", "HOLD", "RELEASE"}:
            raise ValueError("Clock action must be PAUSE, RESUME, ADVANCE, HOLD or RELEASE")
        with self.sessions() as session:
            clock = session.get(SimulationClock, execution_id)
            if clock is None:
                raise LookupError("Simulation clock not found")
            now = datetime.now(timezone.utc)
            current = self._clock_now(clock, now)
            if normalized == "PAUSE":
                clock.paused, clock.paused_virtual_at = True, current
            elif normalized == "RESUME":
                if int(clock.hold_count or 0) > 0:
                    clock.held_virtual_at = current
                else:
                    clock.virtual_anchor_at, clock.real_anchor_at = current, now
                clock.paused, clock.paused_virtual_at = False, None
            elif normalized == "HOLD":
                if int(clock.hold_count or 0) == 0:
                    clock.held_virtual_at = current
                clock.hold_count = int(clock.hold_count or 0) + 1
            elif normalized == "RELEASE":
                count = int(clock.hold_count or 0)
                if count <= 0:
                    # A worker can restart after its durable wave flag was
                    # committed but before the simulator observed the hold.
                    # Releasing an already-cleared hold is therefore a safe
                    # recovery no-op, never a reason to fail a migration.
                    session.commit()
                    return self.clock_status(execution_id)
                clock.hold_count = count - 1
                if clock.hold_count == 0:
                    released = clock.held_virtual_at or current
                    clock.held_virtual_at = None
                    if clock.paused:
                        clock.paused_virtual_at = released
                    else:
                        clock.virtual_anchor_at, clock.real_anchor_at = released, now
            else:
                if advance_seconds < 0:
                    raise ValueError("Clock advance cannot be negative")
                advanced = current + timedelta(seconds=advance_seconds)
                clock.virtual_anchor_at, clock.real_anchor_at = advanced, now
                if clock.paused:
                    clock.paused_virtual_at = advanced
                if int(clock.hold_count or 0) > 0:
                    clock.held_virtual_at = advanced
            session.commit()
        return self.clock_status(execution_id)

    def set_execution_state(self, execution_id: str, state: str) -> SimulationExecution:
        """Record an execution terminal state without mutating its snapshot."""
        normalized = state.strip().upper()
        if normalized not in {item.value for item in ExecutionState}:
            raise ValueError("Invalid simulation execution state")
        with self.sessions() as session:
            execution = session.get(SimulationExecution, execution_id)
            if execution is None:
                raise LookupError("Simulation execution not found")
            if execution.state in {"SUCCEEDED", "FAILED", "CANCELLED"} and execution.state != normalized:
                raise ValueError("A terminal simulation execution is immutable")
            now = datetime.now(timezone.utc)
            if normalized == "RUNNING" and execution.real_started_at is None:
                execution.real_started_at = now
                execution.virtual_started_at = self._clock_now(session.get(SimulationClock, execution.id), now)
            if normalized in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                execution.real_finished_at = execution.real_finished_at or now
                execution.virtual_finished_at = execution.virtual_finished_at or self._clock_now(
                    session.get(SimulationClock, execution.id), now
                )
            execution.state = normalized
            session.commit()
            return execution

    def apply_housekeeping(self, now: datetime | None = None) -> dict:
        """Advance lifecycle markers; physical purge always remains explicit."""
        current = _aware(now or datetime.now(timezone.utc))
        deprecated = purge_eligible = 0
        terminal_states = {"SUCCEEDED", "FAILED", "CANCELLED"}
        with self.sessions() as session:
            scenarios = list(session.scalars(select(SimulationScenario).where(
                SimulationScenario.state.in_([ScenarioState.ACTIVE.value, ScenarioState.DEPRECATED.value])
            )))
            for scenario in scenarios:
                executions = list(session.scalars(select(SimulationExecution).where(
                    SimulationExecution.scenario_id == scenario.id
                )))
                if not executions or any(item.state not in terminal_states for item in executions):
                    continue
                terminal_at = max(
                    (item.real_finished_at or item.created_at for item in executions),
                    default=scenario.created_at,
                )
                scenario.terminal_at = scenario.terminal_at or terminal_at
                terminal_at = _aware(scenario.terminal_at)
                if scenario.state == ScenarioState.ACTIVE.value and current >= terminal_at + timedelta(days=scenario.retention_days):
                    scenario.state = ScenarioState.DEPRECATED.value
                    scenario.deprecated_at = current
                    deprecated += 1
                if scenario.state == ScenarioState.DEPRECATED.value and scenario.deprecated_at and current >= _aware(scenario.deprecated_at) + timedelta(days=scenario.quarantine_days):
                    scenario.state = ScenarioState.PURGE_ELIGIBLE.value
                    scenario.purge_eligible_at = current
                    purge_eligible += 1
            session.commit()
        return {"deprecated": deprecated, "purge_eligible": purge_eligible, "purged": 0}

    def restore_scenario(self, scenario_id: str) -> SimulationScenario:
        """Rollback a lifecycle marker during its recoverable quarantine."""
        with self.sessions() as session:
            scenario = session.get(SimulationScenario, scenario_id)
            if scenario is None:
                raise LookupError("Scenario not found")
            if scenario.state not in {ScenarioState.DEPRECATED.value, ScenarioState.PURGE_ELIGIBLE.value}:
                raise ValueError("Only a deprecated or purge-eligible scenario can be restored")
            scenario.state = ScenarioState.ACTIVE.value
            scenario.deprecated_at = None
            scenario.purge_eligible_at = None
            session.commit()
            return scenario

    def purge_scenario(self, scenario_id: str, reason: str) -> dict:
        """Remove catalog payload/evidence after quarantine, retaining a tombstone.

        The simulator deliberately cannot prove that the Raijin control plane
        has archived every reference. Its public endpoint is therefore called
        only through the control-plane guard that performs that cross-database
        audit first.
        """
        normalized_reason = reason.strip()
        if len(normalized_reason) < 10:
            raise ValueError("Purge reason must contain at least 10 characters")
        with self.sessions() as session:
            scenario = session.get(SimulationScenario, scenario_id)
            if scenario is None:
                raise LookupError("Scenario not found")
            if scenario.state != ScenarioState.PURGE_ELIGIBLE.value:
                raise ValueError("Only a PURGE_ELIGIBLE scenario can be physically purged")
            execution_ids = list(session.scalars(select(SimulationExecution.id).where(
                SimulationExecution.scenario_id == scenario_id
            )))
            bucket_ids = list(session.scalars(select(VirtualBucket.id).where(
                VirtualBucket.scenario_id == scenario_id
            )))
            restore_job_ids = list(session.scalars(select(SimulatedRestoreJob.id).where(
                SimulatedRestoreJob.execution_id.in_(execution_ids)
            ))) if execution_ids else []
            upload_ids = list(session.scalars(select(SimulatedMultipartUpload.id).where(
                SimulatedMultipartUpload.execution_id.in_(execution_ids)
            ))) if execution_ids else []
            evidence = {
                "scenario_id": scenario.id,
                "name": scenario.name,
                "execution_ids": execution_ids,
                "virtual_buckets": len(bucket_ids),
                "virtual_objects": int(session.scalar(select(func.count(VirtualObject.id)).where(
                    VirtualObject.bucket_id.in_(bucket_ids)
                )) or 0) if bucket_ids else 0,
                "restore_jobs": len(restore_job_ids),
                "multipart_uploads": len(upload_ids),
                "operations": int(session.scalar(select(func.count(SimulatedOperation.id)).where(
                    SimulatedOperation.execution_id.in_(execution_ids)
                )) or 0) if execution_ids else 0,
                "faults": int(session.scalar(select(func.count(InjectedFault.id)).where(
                    InjectedFault.execution_id.in_(execution_ids)
                )) or 0) if execution_ids else 0,
            }
            if restore_job_ids:
                session.execute(delete(SimulatedRestoreObjectResult).where(
                    SimulatedRestoreObjectResult.job_id.in_(restore_job_ids)
                ))
            if upload_ids:
                session.execute(delete(SimulatedMultipartPart).where(
                    SimulatedMultipartPart.upload_id.in_(upload_ids)
                ))
            if execution_ids:
                session.execute(delete(SimulatedRestoreJob).where(
                    SimulatedRestoreJob.execution_id.in_(execution_ids)
                ))
                session.execute(delete(SimulatedMultipartUpload).where(
                    SimulatedMultipartUpload.execution_id.in_(execution_ids)
                ))
                session.execute(delete(SimulatedOperation).where(
                    SimulatedOperation.execution_id.in_(execution_ids)
                ))
                session.execute(delete(InjectedFault).where(
                    InjectedFault.execution_id.in_(execution_ids)
                ))
            if bucket_ids:
                session.execute(delete(VirtualObject).where(VirtualObject.bucket_id.in_(bucket_ids)))
                session.execute(delete(VirtualBucket).where(VirtualBucket.id.in_(bucket_ids)))
            digest = hashlib.sha256(_canonical_json(evidence).encode("utf-8")).hexdigest()
            session.add(SimulationTombstone(
                resource_type="SCENARIO",
                resource_id=scenario.id,
                resource_name=scenario.name,
                reason=normalized_reason,
                evidence_sha256=digest,
            ))
            scenario.state = ScenarioState.PURGED.value
            session.commit()
            return {"state": scenario.state, "evidence": evidence, "evidence_sha256": digest}
