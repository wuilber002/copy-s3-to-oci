#!/usr/bin/env python3
"""Simulation-only worker that exercises durable task leases without cloud calls."""

import json
import time
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

BASE_URL = "http://127.0.0.1:8080"
WORKER_ID = "simulated-worker"


def post(path: str, payload: dict) -> Optional[dict]:
    request = Request(
        f"{BASE_URL}{path}",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=10) as response:
        body = response.read()
    return json.loads(body) if body else None


def get(path: str) -> dict:
    with urlopen(f"{BASE_URL}{path}", timeout=10) as response:
        return json.loads(response.read())


while True:
    try:
        if not get("/api/settings").get("simulation_enabled"):
            time.sleep(5)
            continue
        task = post("/api/tasks/claim", {"worker_id": WORKER_ID, "lease_seconds": 300})
        if task:
            try:
                post(f"/api/tasks/{task['task_id']}/simulate", {"worker_id": WORKER_ID})
            except HTTPError:
                # Simulation disabled is the normal production-safe state.
                pass
        time.sleep(2)
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError):
        time.sleep(5)
