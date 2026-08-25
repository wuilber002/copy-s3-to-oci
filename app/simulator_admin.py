"""Typed administrative client for the external simulator service."""

from __future__ import annotations

import json
from urllib.request import Request, urlopen


class SimulatorAdminError(RuntimeError):
    pass


class SimulatorAdminClient:
    def __init__(self, base_url: str, timeout_seconds: float = 60.0):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def _request(self, method: str, path: str, payload: dict | None = None):
        request = Request(
            f"{self.base_url}{path}",
            data=(json.dumps(payload).encode("utf-8") if payload is not None else None),
            method=method,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                body = response.read()
                return json.loads(body) if body else None
        except Exception as error:
            raise SimulatorAdminError(f"Simulator admin request failed: {path}: {error}") from error

    def list_scenarios(self) -> list[dict]:
        return self._request("GET", "/v1/scenarios")

    def create_scenario(self, payload: dict) -> dict:
        return self._request("POST", "/v1/scenarios", payload)

    def materialize(self, scenario_id: str, payload: dict) -> dict:
        return self._request("POST", f"/v1/scenarios/{scenario_id}/materialize", payload)

    def create_execution(self, scenario_id: str) -> dict:
        return self._request("POST", f"/v1/scenarios/{scenario_id}/executions", {})

    def get_execution(self, execution_id: str) -> dict:
        return self._request("GET", f"/v1/executions/{execution_id}")

    def execution_report(self, execution_id: str) -> dict:
        return self._request("GET", f"/v1/executions/{execution_id}/report")

    def clone_execution(
        self,
        execution_id: str,
        name: str,
        fidelity: str | None = None,
        physical_budget_bytes: int | None = None,
    ) -> dict:
        return self._request(
            "POST",
            f"/v1/executions/{execution_id}/clone",
            {"name": name, "fidelity": fidelity, "physical_budget_bytes": physical_budget_bytes},
        )

    def list_templates(self) -> list[dict]:
        return self._request("GET", "/v1/templates")

    def create_template(self, payload: dict) -> dict:
        return self._request("POST", "/v1/templates", payload)

    def update_template(self, template_id: str, payload: dict) -> dict:
        return self._request("PUT", f"/v1/templates/{template_id}", payload)

    def clock_status(self, execution_id: str) -> dict:
        return self._request("GET", f"/v1/executions/{execution_id}/clock")

    def control_clock(self, execution_id: str, action: str, advance_seconds: float = 0) -> dict:
        return self._request(
            "POST",
            f"/v1/executions/{execution_id}/clock",
            {"action": action, "advance_seconds": advance_seconds},
        )

    def set_execution_state(self, execution_id: str, state: str) -> dict:
        return self._request(
            "POST",
            f"/v1/executions/{execution_id}/state",
            {"state": state},
        )

    def apply_housekeeping(self) -> dict:
        return self._request("POST", "/v1/housekeeping", {})

    def restore_scenario(self, scenario_id: str) -> dict:
        return self._request("POST", f"/v1/scenarios/{scenario_id}/restore", {})

    def purge_scenario(self, scenario_id: str, reason: str) -> dict:
        return self._request(
            "POST", f"/v1/scenarios/{scenario_id}/purge", {"reason": reason}
        )
