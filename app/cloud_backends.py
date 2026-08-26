"""Transport-neutral cloud backend boundary.

This foundation deliberately exposes only identity and readiness. Individual
S3/OCI operations will be migrated behind typed ports in later phases instead
of introducing one untyped catch-all client.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from typing import Protocol
from urllib.request import Request, urlopen

from app.runtime_context import OperationMode, RuntimeContext
from app.simulator_contract import SimulatorHandshake, validate_handshake


@dataclass(frozen=True)
class BackendReadiness:
    mode: OperationMode
    contract_version: str | None
    operations_enabled: bool
    capabilities: tuple[str, ...] = ()


@dataclass(frozen=True)
class BackendClock:
    """Clock exposed to planning code, separate from real task lease time."""

    real_now: datetime
    effective_now: datetime
    acceleration: float = 1.0
    paused: bool = False
    held: bool = False


class CloudBackend(Protocol):
    def readiness(self, require_operations: bool = False) -> BackendReadiness: ...

    def clock(self, execution_id: str | None = None) -> BackendClock: ...


class RealCloudBackend:
    def readiness(self, require_operations: bool = False) -> BackendReadiness:
        return BackendReadiness(OperationMode.REAL, None, True)

    def clock(self, execution_id: str | None = None) -> BackendClock:
        now = datetime.now(timezone.utc)
        return BackendClock(now, now)


class SimulatedCloudBackend:
    def __init__(self, context: RuntimeContext, timeout_seconds: float = 5.0):
        if not context.is_simulation or not context.simulator_base_url:
            raise ValueError("SimulatedCloudBackend requires a SIMULATION runtime context")
        self.context = context
        self.timeout_seconds = timeout_seconds

    def readiness(self, require_operations: bool = False) -> BackendReadiness:
        request = Request(
            f"{self.context.simulator_base_url}/v1/handshake",
            headers={"Accept": "application/json"},
        )
        with urlopen(request, timeout=self.timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
        handshake = validate_handshake(
            SimulatorHandshake.model_validate(payload),
            self.context.simulator_contract_version,
            require_operations=require_operations,
        )
        return BackendReadiness(
            OperationMode.SIMULATION,
            handshake.contract_version,
            handshake.operations_enabled,
            tuple(handshake.capabilities),
        )

    @staticmethod
    def _datetime(value: str) -> datetime:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)

    def clock(self, execution_id: str | None = None) -> BackendClock:
        if not execution_id:
            raise ValueError("A simulation execution is required for the virtual clock")
        request = Request(
            f"{self.context.simulator_base_url}/v1/executions/{execution_id}/clock",
            headers={"Accept": "application/json"},
        )
        with urlopen(request, timeout=self.timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return BackendClock(
            real_now=self._datetime(payload["real_now"]),
            effective_now=self._datetime(payload["virtual_now"]),
            acceleration=float(payload["acceleration"]),
            paused=bool(payload["paused"]),
            held=bool(payload.get("held", False)),
        )


def backend_for(context: RuntimeContext) -> CloudBackend:
    if context.is_real:
        return RealCloudBackend()
    return SimulatedCloudBackend(context)
