"""Process-wide execution context and isolation guardrails.

The operation mode is immutable for the lifetime of a process.  Keeping this
small module independent from FastAPI and SQLAlchemy lets the API, workers,
simulator and future Edge Agent enforce the same rules before doing work.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import os
from pathlib import Path
from urllib.parse import urlparse


SIMULATOR_CONTRACT_VERSION = "1"
SIMULATOR_SERVICE_VERSION = "0.1.0"


class OperationMode(str, Enum):
    REAL = "REAL"
    SIMULATION = "SIMULATION"


class SimulationFidelity(str, Enum):
    CONTROL = "CONTROL"
    DATA = "DATA"


class RuntimeIsolationError(RuntimeError):
    """Raised before startup when real and simulated dependencies are mixed."""


def parse_operation_mode(value: str | None) -> OperationMode:
    normalized = str(value or OperationMode.REAL.value).strip().upper()
    try:
        return OperationMode(normalized)
    except ValueError as error:
        raise RuntimeIsolationError("RAIJIN_OPERATION_MODE must be REAL or SIMULATION") from error


def database_name(database_url: str) -> str:
    parsed = urlparse(database_url)
    return parsed.path.lstrip("/").split("?", 1)[0]


@dataclass(frozen=True)
class RuntimeContext:
    mode: OperationMode
    database_url: str
    simulator_base_url: str | None = None
    simulator_contract_version: str = SIMULATOR_CONTRACT_VERSION

    @classmethod
    def from_environ(cls, environ: dict[str, str] | None = None) -> "RuntimeContext":
        values = os.environ if environ is None else environ
        return cls(
            mode=parse_operation_mode(values.get("RAIJIN_OPERATION_MODE")),
            database_url=values.get("DATABASE_URL", ""),
            simulator_base_url=(values.get("RAIJIN_SIMULATOR_BASE_URL") or "").rstrip("/") or None,
            simulator_contract_version=values.get(
                "RAIJIN_SIMULATOR_CONTRACT_VERSION", SIMULATOR_CONTRACT_VERSION
            ),
        )

    @property
    def is_real(self) -> bool:
        return self.mode is OperationMode.REAL

    @property
    def is_simulation(self) -> bool:
        return self.mode is OperationMode.SIMULATION

    def validate(self, environ: dict[str, str] | None = None) -> None:
        values = os.environ if environ is None else environ
        if not self.database_url:
            raise RuntimeIsolationError("DATABASE_URL is required")
        if self.simulator_contract_version != SIMULATOR_CONTRACT_VERSION:
            raise RuntimeIsolationError(
                "Configured simulator contract does not match this RAIJIN release"
            )
        if self.is_real:
            return

        if not self.simulator_base_url:
            raise RuntimeIsolationError(
                "RAIJIN_SIMULATOR_BASE_URL is required in SIMULATION mode"
            )
        if not self.simulator_base_url.startswith(("http://", "https://")):
            raise RuntimeIsolationError("RAIJIN_SIMULATOR_BASE_URL must be an HTTP(S) URL")

        # PostgreSQL modes must never share a logical database. SQLite remains
        # available only for isolated unit tests and local contract validation.
        scheme = urlparse(self.database_url).scheme
        if scheme.startswith("postgres") and database_name(self.database_url) != "migration_simulation":
            raise RuntimeIsolationError(
                "SIMULATION mode requires the migration_simulation database"
            )

        forbidden_credentials = (
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "AWS_SHARED_CREDENTIALS_FILE",
            "AWS_WEB_IDENTITY_TOKEN_FILE",
            "OCI_CLI_CONFIG_FILE",
            "OCI_CLI_KEY_FILE",
        )
        mounted = [name for name in forbidden_credentials if values.get(name)]
        if mounted:
            raise RuntimeIsolationError(
                "SIMULATION mode refuses cloud credentials: " + ", ".join(sorted(mounted))
            )

        runtime_config = values.get("OCI_RUNTIME_CONFIG_FILE")
        if runtime_config and Path(runtime_config).is_file():
            raise RuntimeIsolationError(
                "SIMULATION mode refuses a mounted OCI runtime configuration"
            )

    def require_real_cloud(self, operation: str) -> None:
        if self.is_simulation:
            raise RuntimeIsolationError(
                f"Real cloud operation '{operation}' is forbidden in SIMULATION mode"
            )


def load_runtime_context(environ: dict[str, str] | None = None) -> RuntimeContext:
    context = RuntimeContext.from_environ(environ)
    context.validate(environ)
    return context


def mode_switch_requested(environ: dict[str, str] | None = None) -> bool:
    """Whether the host requested a drain before changing operation mode."""
    values = os.environ if environ is None else environ
    request = Path(values.get("RAIJIN_MODE_REQUEST_FILE", "/run/mode-control/request"))
    return request.is_file() or Path(f"{request}.processing").is_file()
