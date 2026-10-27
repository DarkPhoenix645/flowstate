"""Job backends. The API calls the runner over HTTP."""

from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
import uuid
from typing import Any, Protocol

from flowstate.config.resolve import ConfigValidationError
from flowstate.jobs.registry import JobSpec


class JobBackend(Protocol):
    def submit(self, spec: JobSpec, params: dict[str, Any]) -> str:
        """Start the job. Return an external id."""

    def status(self, external_id: str) -> dict[str, Any] | None:
        """Running, succeeded, failed, or cancelled. None if unknown."""

    def cancel(self, external_id: str) -> dict[str, Any] | None:
        """Ask the backend to stop the job."""

    def log_tail(self, external_id: str) -> str:
        """Last lines of the job log."""


class FakeBackend:
    """In-memory backend for tests."""

    def __init__(
        self,
        result: str = "succeeded",
        log: str = "",
        hold: threading.Event | None = None,
    ) -> None:
        self.result = result
        self.log = log
        self.hold = hold
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self._jobs: dict[str, dict[str, Any]] = {}

    def submit(self, spec: JobSpec, params: dict[str, Any]) -> str:
        self.calls.append((spec.name, params))
        external_id = f"fake-{len(self.calls)}"
        self._jobs[external_id] = {
            "status": "running",
            "log": "",
            "result_location": None,
        }

        def finish() -> None:
            if self.hold is not None:
                self.hold.wait(timeout=5)
            job = self._jobs.get(external_id)
            if job is None or job["status"] == "cancelled":
                return
            job["status"] = self.result
            job["log"] = self.log

        threading.Thread(target=finish, daemon=True).start()
        return external_id

    def status(self, external_id: str) -> dict[str, Any] | None:
        return self._jobs.get(external_id)

    def cancel(self, external_id: str) -> dict[str, Any] | None:
        job = self._jobs.get(external_id)
        if job is None:
            return None
        job["status"] = "cancelled"
        return job

    def log_tail(self, external_id: str) -> str:
        job = self._jobs.get(external_id)
        if job is None:
            return ""
        return str(job["log"])


class ComposeBackend:
    """Host side of the runner. Cluster commands go over HTTP."""

    def __init__(self, runner_url: str) -> None:
        self._url = runner_url.rstrip("/")
        self._jobs: dict[str, dict[str, Any]] = {}

    def submit(self, spec: JobSpec, params: dict[str, Any]) -> str:
        external_id = str(uuid.uuid4())
        self._jobs[external_id] = {
            "status": "running",
            "log": "",
            "result_location": None,
        }
        threading.Thread(
            target=self._finish,
            args=(external_id, spec, params),
            daemon=True,
        ).start()
        return external_id

    def _finish(
        self, external_id: str, spec: JobSpec, params: dict[str, Any]
    ) -> None:
        try:
            if spec.name == "amplify":
                location, log = _amplify(self._url, params)
                state = "succeeded"
            elif spec.engine == "spark":
                location, log = _spark_job(self._url, spec, params)
                state = "succeeded"
            else:
                raise RuntimeError(f"compose has no handler for {spec.name}")
        except Exception as exc:
            self._store(external_id, "failed", str(exc), None)
            return
        self._store(external_id, state, log, location)

    def _store(
        self,
        external_id: str,
        status: str,
        log: str,
        location: str | None,
    ) -> None:
        job = self._jobs.get(external_id)
        if job is None or job["status"] == "cancelled":
            return
        self._jobs[external_id] = {
            "status": status,
            "log": log,
            "result_location": location,
        }

    def status(self, external_id: str) -> dict[str, Any] | None:
        return self._jobs.get(external_id)

    def cancel(self, external_id: str) -> dict[str, Any] | None:
        job = self._jobs.get(external_id)
        if job is None or job["status"] != "running":
            return job
        job["status"] = "cancelled"
        return job

    def log_tail(self, external_id: str) -> str:
        job = self._jobs.get(external_id)
        if job is None:
            return ""
        return str(job["log"])


def configured_backends(runner_url: str) -> dict[str, JobBackend]:
    """compose is the only configured backend. dataproc is #72."""
    return {"compose": ComposeBackend(runner_url)}


_PROCESS: dict[str, JobBackend] | None = None


def process_backends(runner_url: str) -> dict[str, JobBackend]:
    """One backend set for the API process. The poller and requests share it."""
    global _PROCESS
    if _PROCESS is None:
        _PROCESS = configured_backends(runner_url)
    return _PROCESS


def require_backend(
    backends: dict[str, JobBackend], name: str
) -> JobBackend:
    backend = backends.get(name)
    if backend is None:
        raise ConfigValidationError(f"backend {name} is not configured")
    return backend


_ANALYTICS_DDL = {
    "demand_hotspots": ("analytics_demand_hotspots.sql",),
    "cancellation_analysis": ("analytics_cancellation_analysis.sql",),
    "ridership_trends": ("analytics_ridership_trends.sql",),
    "cross_modal": (
        "analytics_ridehail_metro.sql",
        "analytics_flights_ridehail.sql",
    ),
}


def _spark_job(
    runner_url: str, spec: JobSpec, params: dict[str, Any]
) -> tuple[str, str]:
    script = "/opt/src/" + spec.entrypoint.replace(".", "/") + ".py"
    body: dict[str, Any] = {"op": "spark_submit", "script": script}
    if spec.name != "hdfs_probe":
        body["args"] = _analytics_args(params)
    logs = [_post(runner_url, body)]
    for name in _ANALYTICS_DDL.get(spec.name, ()):
        logs.append(
            _post(
                runner_url,
                {
                    "op": "beeline",
                    "file": f"/opt/src/flowstate/warehouse/ddl/{name}",
                },
            )
        )
    if spec.name == "hdfs_probe":
        location = "hdfs://namenode:9000/tmp/spark_hdfs_probe"
    elif spec.name == "cross_modal":
        location = (
            "hdfs://namenode:9000/user/hive/warehouse/"
            "analytics_ridehail_metro"
        )
    else:
        table = _ANALYTICS_DDL[spec.name][0].removesuffix(".sql")
        location = f"hdfs://namenode:9000/user/hive/warehouse/{table}"
    return location, "\n".join(logs)


def _analytics_args(params: dict[str, Any]) -> list[str]:
    run_id = str(params.get("run_id") or "")
    if not run_id:
        raise RuntimeError("analytics job needs run_id")
    return [
        f"--run-id={run_id}",
        f"--corpus-rows={int(params['corpus_rows'])}",
        f"--seed={int(params['seed'])}",
        f"--bundle-hash={params['bundle_hash']}",
        f"--bundle-version={params['bundle_version']}",
    ]


def _amplify(runner_url: str, params: dict[str, Any]) -> tuple[str, str]:
    from flowstate.config import get_settings
    from flowstate.ingest.amplify import AmplifyRequest
    from flowstate.normalize.cities import DEFAULT_CITY_ID
    from flowstate.normalize.places.review import require_station_reviews
    from flowstate.resample import run_amplify

    request = AmplifyRequest.model_validate(
        {key: value for key, value in params.items() if key != "run_id"}
    )
    settings = get_settings()
    require_station_reviews(settings.data_dir, DEFAULT_CITY_ID)
    run_amplify(settings, request.rows, request.seed)
    from flowstate.lake.amplify import (
        HDFS_AMPLIFIED_DIR,
        META_FILES,
        amplified_hdfs_uri,
    )
    from flowstate.lake.ridehail import _copy_in

    root = settings.staged_dir / "rides_amplified"
    parts = sorted(root.glob("part-*.parquet"))
    copy_root = settings.lake_dir / "rides_amplified"
    for path in parts:
        _copy_in(path, copy_root / path.name)
    for name in META_FILES:
        _copy_in(root / name, copy_root / "_meta" / name)
    logs = [
        _post(runner_url, {"op": "hdfs_rm", "dest": HDFS_AMPLIFIED_DIR})
    ]
    logs.append(
        _post(
            runner_url,
            {
                "op": "hdfs_mkdir",
                "dest": f"{HDFS_AMPLIFIED_DIR}/_meta",
            },
        )
    )
    for path in parts:
        logs.append(
            _post(
                runner_url,
                {
                    "op": "hdfs_put",
                    "src": f"/opt/data/lake/rides_amplified/{path.name}",
                    "dest": f"{HDFS_AMPLIFIED_DIR}/{path.name}",
                },
            )
        )
    for name in META_FILES:
        logs.append(
            _post(
                runner_url,
                {
                    "op": "hdfs_put",
                    "src": f"/opt/data/lake/rides_amplified/_meta/{name}",
                    "dest": f"{HDFS_AMPLIFIED_DIR}/_meta/{name}",
                },
            )
        )
    logs.append(
        _post(
            runner_url,
            {
                "op": "spark_submit",
                "script": (
                    "/opt/src/flowstate/batch/spark_jobs/"
                    "conservation_check.py"
                ),
            },
        )
    )
    logs.append(
        _post(
            runner_url,
            {
                "op": "beeline",
                "file": (
                    "/opt/src/flowstate/warehouse/ddl/"
                    "rollup_conservation.sql"
                ),
            },
        )
    )
    location = amplified_hdfs_uri(settings, "")
    return location.rstrip("/"), "\n".join(logs)


def _post(runner_url: str, body: dict[str, Any]) -> str:
    raw = json.dumps(body).encode()
    request = urllib.request.Request(
        runner_url + "/ops",
        data=raw,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=3600) as response:
            payload = json.load(response)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise RuntimeError(detail[-4000:]) from None
    except urllib.error.URLError as exc:
        raise RuntimeError(f"runner is unreachable: {exc.reason}") from None
    if not payload.get("ok"):
        raise RuntimeError(str(payload.get("log") or "runner failed"))
    return str(payload.get("log") or "")
