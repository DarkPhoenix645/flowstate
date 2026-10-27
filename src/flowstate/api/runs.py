"""Run and job routes on the demo API process."""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import parse_qs

from flowstate.api.page import limit_of, slice_page
from flowstate.config.postgres import ConfigStore
from flowstate.config.resolve import ConfigValidationError
from flowstate.jobs.backend import process_backends
from flowstate.jobs.registry import job_catalog
from flowstate.jobs.replay import ReplayBusy, ReplayGate, process_gate
from flowstate.jobs.service import cancel_run, record_run, start_run
from flowstate.jobs.store import RunStore
from flowstate.resample.scenario import ScenarioCommand
from flowstate.streaming.control import ControlMessage, StartRequest


def run_dispatch(
    store: ConfigStore,
    method: str,
    path: str,
    query: str,
    raw: bytes,
    settings: Any,
    backends: dict[str, Any] | None = None,
    replay: ReplayGate | None = None,
) -> tuple[int, dict[str, Any]] | None:
    if path != "/api/jobs" and not path.startswith("/api/runs"):
        return None
    chosen = backends
    if chosen is None:
        chosen = process_backends(settings.runner_url)
    runs = RunStore(store.database_url)
    gate = replay if replay is not None else process_gate()
    params = parse_qs(query)
    if path == "/api/jobs" and method == "GET":
        jobs = sorted(job_catalog(), key=lambda item: item["name"])
        page, nxt = slice_page(
            jobs,
            limit_of(params.get("limit", [None])[0]),
            params.get("cursor", [None])[0],
            "name",
        )
        return 200, {"jobs": page, "next_cursor": nxt}
    if path == "/api/runs" and method == "GET":
        kind = params.get("kind", [None])[0]
        status = params.get("status", [None])[0]
        page, nxt = runs.page_runs(
            kind,
            status,
            limit_of(params.get("limit", [None])[0]),
            params.get("cursor", [None])[0],
        )
        return 200, {"runs": page, "next_cursor": nxt}
    if path == "/api/runs" and method == "POST":
        body = json.loads(raw) if raw else {}
        if not isinstance(body, dict):
            raise ConfigValidationError("body must be a JSON object")
        if body.get("kind") == "replay":
            return 202, _start_replay(store, runs, gate, body, settings)
        row = start_run(store, runs, chosen, body, settings)
        return 202, row
    if path.startswith("/api/runs/"):
        rest = path.removeprefix("/api/runs/")
        if rest.endswith("/control") and method == "POST":
            return _control(runs, gate, rest[: -len("/control")], raw)
        if rest.endswith("/commands") and method == "POST":
            return _commands(runs, gate, rest[: -len("/commands")], raw)
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
            if row["kind"] == "replay":
                row = dict(row)
                row["producer"] = gate.view(rest)
            return 200, row
    return 404, {"error": "not found"}


def _start_replay(
    store: ConfigStore,
    runs: RunStore,
    gate: ReplayGate,
    body: dict[str, Any],
    settings: Any,
) -> dict[str, Any]:
    if body.get("job") not in (None, "replay"):
        raise ConfigValidationError("replay job must be replay")
    if gate.busy():
        raise ReplayBusy("a replay is already running")
    fields = dict(body.get("params") or {})
    for name in StartRequest.model_fields:
        if name in body:
            fields[name] = body[name]
    try:
        start = StartRequest.model_validate(fields)
    except Exception as exc:
        raise ConfigValidationError(str(exc)) from None
    run_id = uuid_run()
    start = start.model_copy(update={"session_id": run_id})
    record_run(
        store,
        runs,
        settings,
        run_id=run_id,
        kind="replay",
        job="replay",
        params=start.model_dump(mode="json"),
    )
    gate.begin(run_id, start)
    runs.mark_running(run_id, run_id)
    saved = runs.get(run_id)
    assert saved is not None
    return saved


def _control(
    runs: RunStore, gate: ReplayGate, run_id: str, raw: bytes
) -> tuple[int, dict[str, Any]]:
    row = runs.get(run_id)
    if row is None:
        return 404, {"error": "run not found"}
    try:
        message = ControlMessage.model_validate_json(raw or b"{}")
    except Exception as exc:
        raise ConfigValidationError(str(exc)) from None
    answer = gate.control(run_id, message)
    if message.op == "stop" and answer.get("accepted"):
        runs.finish(run_id, "succeeded")
    return 200, answer


def _commands(
    runs: RunStore, gate: ReplayGate, run_id: str, raw: bytes
) -> tuple[int, dict[str, Any]]:
    if runs.get(run_id) is None:
        return 404, {"error": "run not found"}
    try:
        command = ScenarioCommand.model_validate_json(raw or b"{}")
    except Exception as exc:
        raise ConfigValidationError(str(exc)) from None
    return 200, gate.command(run_id, command)


def uuid_run() -> str:
    import uuid

    return uuid.uuid4().hex
