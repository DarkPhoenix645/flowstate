"""MapReduce analytics submit. The jar stays free of the Compose host."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

from flowstate.api.server import dispatch
from flowstate.config.postgres import ConfigStore
from flowstate.jobs.backend import (
    FakeBackend,
    mr_submit,
    yarn_application_id,
)
from flowstate.jobs.registry import get_job

_SECRET = "runner-secret"
ROOT = Path("src/flowstate/batch/mapreduce")


def _runner():
    path = Path("infra/runner/server.py")
    spec = importlib.util.spec_from_file_location("runner_server", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_the_jar_does_not_name_the_compose_cluster():
    text = "\n".join(path.read_text() for path in ROOT.rglob("*.java"))
    assert "namenode" not in text
    assert "mapreduce.map.memory.mb" not in text
    assert "resourcemanager" not in text


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="the runner checks Linux container paths",
)
def test_yarn_jar_forwards_args_without_a_memory_flag():
    runner = _runner()
    jar = (
        "/opt/src/flowstate/batch/mapreduce/target/"
        "flowstate-mapreduce-0.1.0.jar"
    )
    argv = runner._argv(
        {
            "op": "yarn_jar",
            "jar": jar,
            "main": "com.flowstate.mapreduce.AnalyticsDriver",
            "args": [
                "--job=demand_hotspots",
                "--input=hdfs://namenode:9000/lake/rides_amplified",
            ],
        }
    )
    assert argv[:4] == [
        runner.YARN,
        "jar",
        jar,
        "com.flowstate.mapreduce.AnalyticsDriver",
    ]
    assert argv[-1].startswith("--input=")
    assert not any("memory" in item for item in argv)


def test_mapreduce_submit_passes_uris_and_not_memory():
    spec = get_job("demand_hotspots")
    body = mr_submit(
        spec,
        {
            "run_id": "run-1",
            "corpus_rows": 100_000_000,
            "seed": 42,
            "bundle_hash": "abc",
            "bundle_version": "resampler-1.0.0",
        },
    )
    assert body["op"] == "yarn_jar"
    args = " ".join(body["args"])
    assert "--input=hdfs://namenode:9000/lake/rides_amplified" in args
    assert "--corpus-rows=100000000" in args
    assert "memory" not in args
    assert "gs://" not in args
    tracked = mr_submit(spec, {
        "run_id": "run-1",
        "corpus_rows": 100_000_000,
        "seed": 42,
        "bundle_hash": "abc",
        "bundle_version": "resampler-1.0.0",
    }, "run-1")
    assert tracked["track"] == "run-1"


def test_a_yarn_log_keeps_the_application_id():
    text = "Submitted application application_12_3\nmore\n"
    assert yarn_application_id(text) == "application_12_3"
    assert yarn_application_id("no id") is None


def test_a_probe_cannot_use_mapreduce():
    store = ConfigStore(
        f"postgresql://flowstate:{_SECRET}@127.0.0.1:1/flowstate"
    )
    status, body = dispatch(
        store,
        "POST",
        "/api/runs",
        b'{"kind":"analytics","job":"hdfs_probe","engine":"mapreduce","params":{}}',
        backends={"compose": FakeBackend()},
    )
    assert status == 422
    assert "not available" in body["error"]


