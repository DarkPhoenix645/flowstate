"""Run and job routes on the demo API process."""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import parse_qs

from flowstate.config.postgres import ConfigStore
from flowstate.config.resolve import ConfigValidationError
from flowstate.jobs.backend import process_backends
from flowstate.jobs.registry import job_catalog
from flowstate.jobs.service import cancel_run, start_run
from flowstate.jobs.store import RunStore


def run_dispatch(
    store: ConfigStore,
    method: str,
    path: str,
    query: str,
    raw: bytes,
    settings: Any,
    backends: dict[str, Any] | None = None,
) -> tuple[int, dict[str, Any]] | None:
    if path != "/api/jobs" and not path.startswith("/api/runs"):
        return None
    chosen = backends
    if chosen is None:
        chosen = process_backends(settings.runner_url)
    runs = RunStore(store.database_url)
    if path == "/api/jobs" and method == "GET":
        return 200, {"jobs": job_catalog()}
    if path == "/api/runs" and method == "GET":
        params = parse_qs(query)
        kind = params.get("kind", [None])[0]
        status = params.get("status", [None])[0]
        return 200, {"runs": runs.list_runs(kind, status)}
    if path == "/api/runs" and method == "POST":
        body = json.loads(raw) if raw else {}
        if not isinstance(body, dict):
            raise ConfigValidationError("body must be a JSON object")
        row = start_run(store, runs, chosen, body, settings)
        return 202, row
    if path.startswith("/api/runs/"):
        rest = path.removeprefix("/api/runs/")
        if rest.endswith("/cancel") and method == "POST":
            run_id = rest[: -len("/cancel")]
            row = cancel_run(runs, chosen, run_id)
            if row is None:
                return 404, {"error": "run not found"}
            return 200, row
        if "/" not in rest and method == "GET":
            row = runs.get(rest)
            if row is None:
                return 404, {"error": "run not found"}
            return 200, row
    return 404, {"error": "not found"}
