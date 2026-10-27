"""Run API. The compose backend is not started here."""

from __future__ import annotations

import json
import os
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

import psycopg
import pytest

from flowstate.api.server import dispatch
from flowstate.config.postgres import SCHEMA_VERSION, ConfigStore
from flowstate.config.seed import seed
from flowstate.jobs.backend import FakeBackend
from flowstate.jobs.service import reconcile
from flowstate.jobs.store import RunStore

_LOCAL_URL = (
    "postgresql://flowstate:flowstate@127.0.0.1:5432/flowstate"
)

_SECRET = "runner-secret"


@pytest.fixture
def database_url():
    url = os.environ.get("FLOWSTATE_CONFIG_DATABASE_URL", _LOCAL_URL)
    try:
        with psycopg.connect(url, connect_timeout=2) as conn:
            row = conn.execute(
                "SELECT version FROM schema_version"
            ).fetchone()
    except psycopg.OperationalError:
        pytest.skip("Postgres is not available")
    if row is None or row[0] != SCHEMA_VERSION:
        pytest.skip("Postgres schema is not version 3")
    with psycopg.connect(url) as conn:
        conn.execute("DELETE FROM run")
        conn.execute(
            "TRUNCATE config_active, config_version RESTART IDENTITY"
        )
    return url


def _backends(result: str = "succeeded", log: str = "") -> dict:
    return {"compose": FakeBackend(result=result, log=log)}


def test_an_unknown_job_is_422_and_does_not_need_the_database():
    store = ConfigStore(
        f"postgresql://flowstate:{_SECRET}@127.0.0.1:1/flowstate"
    )
    status, body = dispatch(
        store,
        "POST",
        "/api/runs",
        json.dumps(
            {"kind": "analytics", "job": "not-a-job", "params": {}}
        ).encode(),
        backends=_backends(),
    )
    assert status == 422
    assert _SECRET not in json.dumps(body)


def test_an_unconfigured_backend_is_422():
    store = ConfigStore(
        f"postgresql://flowstate:{_SECRET}@127.0.0.1:1/flowstate"
    )
    status, body = dispatch(
        store,
        "POST",
        "/api/runs",
        json.dumps(
            {
                "kind": "analytics",
                "job": "hdfs_probe",
                "params": {},
                "backend": "dataproc",
            }
        ).encode(),
        backends=_backends(),
    )
    assert status == 422
    assert "not configured" in body["error"]


def test_the_api_package_does_not_call_docker():
    root = Path("src/flowstate")
    files = list(root.joinpath("api").rglob("*.py"))
    files.extend(root.joinpath("jobs").rglob("*.py"))
    text = "\n".join(path.read_text() for path in files)
    assert "docker" not in text
    compose = Path("docker-compose.yml").read_text()
    assert "docker.sock" not in compose


@pytest.mark.integration
def test_a_run_keeps_the_snapshot_and_a_missing_job_adds_no_row(
    database_url,
):
    seed(database_url)
    store = ConfigStore(database_url)
    runs = RunStore(database_url)
    before = len(runs.list_runs())
    status, body = dispatch(
        store,
        "POST",
        "/api/runs",
        json.dumps({"kind": "amplify", "job": "missing"}).encode(),
        backends=_backends(),
    )
    assert status == 422
    assert len(runs.list_runs()) == before

    status, body = dispatch(
        store,
        "POST",
        "/api/runs",
        json.dumps(
            {
                "kind": "amplify",
                "job": "amplify",
                "params": {"rows": 1_000_000, "seed": 42},
            }
        ).encode(),
        backends=_backends(),
    )
    assert status == 202
    assert body["config_snapshot"]["resample_config"][
        "assumed_weekday_boardings"
    ]
    assert body["config_hash"]
    assert body["resample_config_version_id"]
    saved = _wait(runs, body["run_id"])
    assert saved["status"] == "succeeded"
    assert saved["params"]["rows"] == 1_000_000
    assert saved["params"]["seed"] == 42


@pytest.mark.integration
def test_a_failed_backend_stores_the_log_tail(database_url):
    seed(database_url)
    store = ConfigStore(database_url)
    status, body = dispatch(
        store,
        "POST",
        "/api/runs",
        json.dumps(
            {"kind": "analytics", "job": "hdfs_probe", "params": {}}
        ).encode(),
        backends=_backends(result="failed", log="spark-submit boom"),
    )
    assert status == 202
    saved = _wait(RunStore(database_url), body["run_id"])
    assert saved["status"] == "failed"
    assert saved["error"] == "spark-submit boom"


@pytest.mark.integration
@pytest.mark.integration
def test_a_run_is_running_before_the_backend_finishes(database_url):
    seed(database_url)
    hold = threading.Event()
    store = ConfigStore(database_url)
    runs = RunStore(database_url)
    status, body = dispatch(
        store,
        "POST",
        "/api/runs",
        json.dumps(
            {"kind": "analytics", "job": "hdfs_probe", "params": {}}
        ).encode(),
        backends={"compose": FakeBackend(hold=hold)},
    )
    assert status == 202
    seen = None
    for _ in range(50):
        seen = runs.get(body["run_id"])
        if seen is not None and seen["status"] == "running":
            break
        time.sleep(0.02)
    assert seen is not None
    assert seen["status"] == "running"
    assert seen["external_id"]
    hold.set()
    finished = _wait(runs, body["run_id"])
    assert finished["status"] == "succeeded"


@pytest.mark.integration
def test_restart_reconciles_a_running_row(database_url):
    seed(database_url)
    runs = RunStore(database_url)
    with psycopg.connect(database_url) as conn:
        conn.execute(
            """
            INSERT INTO run (
                run_id, kind, job, params, config_snapshot,
                backend, status, external_id
            )
            VALUES (
                'stuck', 'analytics', 'hdfs_probe', '{}', '{}',
                'compose', 'running', 'gone'
            )
            """
        )
    reconcile(runs, _backends())
    saved = runs.get("stuck")
    assert saved is not None
    assert saved["status"] == "failed"
    assert saved["error"] == "backend has no record of this job"


def _wait(runs: RunStore, run_id: str) -> dict:
    saved = None
    for _ in range(50):
        saved = runs.get(run_id)
        if saved is not None and saved["status"] not in (
            "queued",
            "running",
        ):
            return saved
        time.sleep(0.02)
    assert saved is not None
    return saved


@pytest.mark.integration
def test_runner_rejects_a_free_command():
    raw = json.dumps(
        {"op": "spark_submit", "script": "/tmp/evil.sh"}
    ).encode()
    request = urllib.request.Request(
        "http://127.0.0.1:8091/ops",
        data=raw,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=3) as response:
            status, body = response.status, json.load(response)
    except urllib.error.HTTPError as exc:
        status, body = exc.code, json.load(exc)
    except urllib.error.URLError:
        pytest.skip("runner is not up")
    assert status == 422
    assert body["ok"] is False


def test_jobs_lists_the_registry_without_a_database():
    store = ConfigStore(
        f"postgresql://flowstate:{_SECRET}@127.0.0.1:1/flowstate"
    )
    status, body = dispatch(
        store, "GET", "/api/jobs", b"", backends=_backends()
    )
    assert status == 200
    names = {item["name"] for item in body["jobs"]}
    assert names == {"amplify", "hdfs_probe"}
