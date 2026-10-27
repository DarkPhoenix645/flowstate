"""Create a run, then let the backend move it."""

from __future__ import annotations

import threading
import time
import uuid
from typing import Any

from flowstate.config.demo import active_config_hash
from flowstate.config.postgres import ConfigStore
from flowstate.config.resolve import (
    ConfigNotActiveError,
    ConfigValidationError,
)
from flowstate.jobs.backend import JobBackend, require_backend
from flowstate.jobs.grafana import grafana_link
from flowstate.jobs.registry import JobSpec, validate_job
from flowstate.jobs.store import RunStore


def start_run(
    configs: ConfigStore,
    runs: RunStore,
    backends: dict[str, JobBackend],
    body: dict[str, Any],
    settings: Any,
    *,
    wait: bool = False,
) -> dict[str, Any]:
    kind = body.get("kind")
    job = body.get("job")
    params = body.get("params") or {}
    backend_name = body.get("backend") or "compose"
    if not isinstance(kind, str) or not isinstance(job, str):
        raise ConfigValidationError("kind and job are required")
    spec = validate_job(kind, job, params)
    backend = require_backend(backends, str(backend_name))
    active = configs.active()
    resample = active.get("resample_config")
    if resample is None:
        raise ConfigNotActiveError(
            "no active resample_config. Run make config-seed."
        )
    policy = active.get("scenario_policy")
    snapshot = {
        "resample_config": resample.content,
        "scenario_policy": None if policy is None else policy.content,
        "replay_seed": configs.replay_seed(),
    }
    run_id = str(uuid.uuid4())
    _insert(
        runs,
        run_id=run_id,
        kind=spec.kind,
        job=spec.name,
        params=params,
        snapshot=snapshot,
        config_hash=active_config_hash(configs),
        policy_id=None if policy is None else policy.id,
        resample_id=resample.id,
        backend=str(backend_name),
        grafana=grafana_link(settings, spec.name, run_id),
    )
    if wait:
        _execute(runs, backend, spec, params, run_id)
    else:
        threading.Thread(
            target=_execute,
            args=(runs, backend, spec, params, run_id),
            daemon=True,
        ).start()
    saved = runs.get(run_id)
    assert saved is not None
    return saved


def record_run(
    configs: ConfigStore,
    runs: RunStore,
    settings: Any,
    *,
    run_id: str,
    kind: str,
    job: str,
    params: dict[str, Any],
) -> dict[str, Any]:
    """Store one run with the config snapshot before any work starts."""
    active = configs.active()
    resample = active.get("resample_config")
    if resample is None:
        raise ConfigNotActiveError(
            "no active resample_config. Run make config-seed."
        )
    policy = active.get("scenario_policy")
    snapshot = {
        "resample_config": resample.content,
        "scenario_policy": None if policy is None else policy.content,
        "replay_seed": configs.replay_seed(),
    }
    _insert(
        runs,
        run_id=run_id,
        kind=kind,
        job=job,
        params=params,
        snapshot=snapshot,
        config_hash=active_config_hash(configs),
        policy_id=None if policy is None else policy.id,
        resample_id=resample.id,
        backend="replay",
        grafana=grafana_link(settings, job, run_id),
    )
    saved = runs.get(run_id)
    assert saved is not None
    return saved


def _insert(
    runs: RunStore,
    *,
    run_id: str,
    kind: str,
    job: str,
    params: dict[str, Any],
    snapshot: dict[str, Any],
    config_hash: str | None,
    policy_id: str | None,
    resample_id: str,
    backend: str,
    grafana: str | None,
) -> None:
    runs.insert(
        {
            "run_id": run_id,
            "kind": kind,
            "job": job,
            "params": params,
            "config_snapshot": snapshot,
            "config_hash": config_hash,
            "scenario_policy_version_id": policy_id,
            "resample_config_version_id": resample_id,
            "backend": backend,
            "grafana_url": grafana,
        }
    )


def reconcile(runs: RunStore, backends: dict[str, JobBackend]) -> None:
    """Match every running row to its backend. Unknown ids fail."""
    for row in runs.running():
        backend = backends.get(str(row["backend"]))
        external_id = row.get("external_id")
        if backend is None:
            continue
        if not external_id:
            runs.finish(
                row["run_id"],
                "failed",
                error="backend has no record of this job",
            )
            continue
        _apply(runs, backend, row["run_id"], str(external_id))


def cancel_run(
    runs: RunStore,
    backends: dict[str, JobBackend],
    run_id: str,
) -> dict[str, Any] | None:
    row = runs.get(run_id)
    if row is None:
        return None
    if row["status"] not in ("queued", "running"):
        raise ConfigValidationError(
            f"run {run_id} is {row['status']}"
        )
    backend = backends.get(str(row["backend"]))
    external_id = row.get("external_id")
    if backend is not None and external_id:
        backend.cancel(str(external_id))
    runs.finish(run_id, "cancelled")
    saved = runs.get(run_id)
    assert saved is not None
    return saved


def _execute(
    runs: RunStore,
    backend: JobBackend,
    spec: JobSpec,
    params: dict[str, Any],
    run_id: str,
) -> None:
    external_id = backend.submit(spec, params)
    runs.mark_running(run_id, external_id)
    while True:
        _apply(runs, backend, run_id, external_id)
        row = runs.get(run_id)
        if row is None or row["status"] not in ("queued", "running"):
            return
        time.sleep(0.05)


def _apply(
    runs: RunStore,
    backend: JobBackend,
    run_id: str,
    external_id: str,
) -> None:
    state = backend.status(external_id)
    if state is None:
        runs.finish(
            run_id, "failed", error="backend has no record of this job"
        )
        return
    status = str(state["status"])
    if status in ("succeeded", "failed", "cancelled"):
        error = None
        if status == "failed":
            error = backend.log_tail(external_id) or "job failed"
        runs.finish(
            run_id,
            status,
            error=error,
            result_location=state.get("result_location"),
        )
