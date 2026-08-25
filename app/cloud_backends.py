"""Transport-neutral cloud backend boundary.

This foundation deliberately exposes only identity and readiness. Individual
S3/OCI operations will be migrated behind typed ports in later phases instead
of introducing one untyped catch-all client.
"""

from __future__ import annotations

from dataclasses import dataclass
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


class CloudBackend(Protocol):
    def readiness(self, require_operations: bool = False) -> BackendReadiness: ...


class RealCloudBackend:
    def readiness(self, require_operations: bool = False) -> BackendReadiness:
        return BackendReadiness(OperationMode.REAL, None, True)


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


def backend_for(context: RuntimeContext) -> CloudBackend:
    if context.is_real:
        return RealCloudBackend()
    return SimulatedCloudBackend(context)
