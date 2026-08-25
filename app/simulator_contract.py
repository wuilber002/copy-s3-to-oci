"""Versioned wire contract shared by RAIJIN and the simulator service."""

from __future__ import annotations

from pydantic import BaseModel, Field

from app.runtime_context import SIMULATOR_CONTRACT_VERSION, SIMULATOR_SERVICE_VERSION


class SimulatorHandshake(BaseModel):
    service: str = "raijin-simulator"
    service_version: str = SIMULATOR_SERVICE_VERSION
    contract_version: str = SIMULATOR_CONTRACT_VERSION
    operations_enabled: bool = False
    capabilities: list[str] = Field(default_factory=list)


class SimulatorHealth(BaseModel):
    status: str = "ok"
    mode: str = "SIMULATION"
    contract_version: str = SIMULATOR_CONTRACT_VERSION
    ready: bool = False
    detail: str | None = None


class SimulatorContractError(RuntimeError):
    """Raised when control plane and simulator cannot safely communicate."""


def validate_handshake(
    handshake: SimulatorHandshake,
    expected_contract: str = SIMULATOR_CONTRACT_VERSION,
    require_operations: bool = False,
) -> SimulatorHandshake:
    if handshake.service != "raijin-simulator":
        raise SimulatorContractError("Unexpected simulator service identity")
    if handshake.contract_version != expected_contract:
        raise SimulatorContractError(
            f"Simulator contract mismatch: expected {expected_contract}, got {handshake.contract_version}"
        )
    if require_operations and not handshake.operations_enabled:
        raise SimulatorContractError("Simulator operational backends are not enabled yet")
    return handshake
