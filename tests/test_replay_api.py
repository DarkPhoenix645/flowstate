"""Replay routes. The producer process is not started."""

from __future__ import annotations

import json
import os
from pathlib import Path

import psycopg
import pytest

from flowstate.api.server import dispatch
from flowstate.config.postgres import SCHEMA_VERSION, ConfigStore
from flowstate.config.seed import seed
from flowstate.jobs.replay import ReplayGate
from flowstate.jobs.store import RunStore
from flowstate.streaming.control import ControlMessage, StartRequest

_LOCAL_URL = (
    "postgresql://flowstate:flowstate@127.0.0.1:5432/flowstate"
)

_SECRET = "replay-secret"


class _Proc:
    def __init__(self) -> None:
        self.returncode: int | None = None
        self.killed = False

    def poll(self) -> int | None:
        return self.returncode

    def wait(self, timeout: float | None = None) -> int:
        self.returncode = 0
        return 0

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9


class _Reader:
    def __init__(self) -> None:
        self.status: dict[str, dict] = {}
        self.requests: dict[str, dict] = {}
        self.commands: dict[str, dict] = {}

    def latest_status(self, run_id: str) -> dict | None:
        return self.status.get(run_id)

    def wait_request(self, request_id: str, timeout: float) -> dict | None:
        return self.requests.get(request_id) or {
            "accepted": True,
            "state": "playing",
            "playhead": "2024-10-26T00:00:00+05:30",
        }

    def wait_command(self, scenario_id: str, timeout: float) -> dict | None:
        return self.commands.get(scenario_id)

    def forget_command(self, scenario_id: str) -> None:
        self.commands.pop(scenario_id, None)


def _store() -> ConfigStore:
    return ConfigStore(
        f"postgresql://flowstate:{_SECRET}@127.0.0.1:1/flowstate"
    )


def _gate(reader: _Reader | None = None) -> tuple[ReplayGate, list, _Proc]:
    sent: list = []
    proc = _Proc()
    reader = reader or _Reader()

    def sender(message: ControlMessage) -> str:
        sent.append(message)
        return "req-1"

    def commander(command: object) -> None:
        sent.append(command)

    gate = ReplayGate(
        sender=sender,
        commander=commander,
        starter=lambda: proc,
        reader=reader,
        clock=lambda: 100.0,
    )
    gate.process = proc
    return gate, sent, proc


def test_a_second_replay_is_409_and_needs_no_database():
    gate, _sent, _proc = _gate()
    gate.run_id = "live"
    status, body = dispatch(
        _store(),
        "POST",
        "/api/runs",
        json.dumps({"kind": "replay", "rows": 1000, "seed": 42}).encode(),
        backends={},
        replay=gate,
    )
    assert status == 409
    assert _SECRET not in json.dumps(body)


def test_stop_and_a_rejected_command_report_accepted():
    reader = _Reader()
    reader.requests["req-1"] = {
        "accepted": True,
        "state": "stopped",
        "playhead": "2024-10-26T01:00:00+05:30",
    }
    gate, sent, _proc = _gate(reader)
    gate.run_id = "abc"
    answer = gate.control("abc", ControlMessage(op="pause"))
    assert answer["accepted"] is True
    assert answer["playhead"] == "2024-10-26T01:00:00+05:30"
    assert sent[0].op == "pause"

    def commander(command: object) -> None:
        sent.append(command)
        scenario_id = getattr(command, "scenario_id")
        reader.commands[scenario_id] = {
            "accepted": False,
            "op": "command",
            "reason": "scenario_id already used",
        }

    gate.commander = commander
    from flowstate.resample.scenario import ScenarioCommand

    command = ScenarioCommand(
        mode="ride_hail",
        scenario_type="custom_shock",
        location_ref="b_1",
        start="2024-10-26T01:00:00+05:30",
        duration="PT1H",
        scenario_id="shock-2",
    )
    rejected = gate.command("abc", command)
    assert rejected["accepted"] is False
    assert rejected["qa_failure"] is True


def test_a_quiet_producer_is_offline_and_not_an_error():
    reader = _Reader()
    reader.status["abc"] = {
        "state": "playing",
        "playhead": "2024-10-26T00:10:00+05:30",
        "seen_at": 80.0,
        "sent": 3,
        "acked": 3,
        "failed": 0,
    }
    gate, _sent, _proc = _gate(reader)
    view = gate.view("abc")
    assert view["state"] == "producer_offline"
    assert view["sent"] == 3
    assert "error" not in view


def test_shutdown_sends_shutdown_and_the_child_ends():
    gate, sent, proc = _gate()
    gate.run_id = "abc"
    gate.shutdown()
    assert sent[-1].op == "shutdown"
    assert proc.returncode == 0
    assert gate.run_id is None


def test_begin_starts_the_child_and_sends_the_session_id():
    gate, sent, proc = _gate()
    gate.process = None
    gate.begin("sess", StartRequest(rows=1000, seed=42))
    assert gate.process is proc
    assert sent[0].op == "start"
    assert sent[0].start.session_id == "sess"


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
        pytest.skip("Postgres schema is not current")
    with psycopg.connect(url) as conn:
        conn.execute("DELETE FROM run")
        conn.execute(
            "TRUNCATE config_active, config_version RESTART IDENTITY"
        )
    return url


@pytest.mark.integration
def test_post_replay_stores_the_session_and_shows_offline(database_url):
    seed(database_url)
    gate, sent, _proc = _gate()
    gate.process = None
    store = ConfigStore(database_url)
    status, body = dispatch(
        store,
        "POST",
        "/api/runs",
        json.dumps({"kind": "replay", "rows": 1000, "seed": 42}).encode(),
        backends={},
        replay=gate,
    )
    assert status == 202
    assert body["run_id"] == sent[0].start.session_id
    assert body["config_hash"]
    assert body["config_snapshot"]["resample_config"]
    status, fetched = dispatch(
        store,
        "GET",
        f"/api/runs/{body['run_id']}",
        b"",
        backends={},
        replay=gate,
    )
    assert status == 200
    assert fetched["producer"]["state"] == "producer_offline"
    assert RunStore(database_url).get(body["run_id"])["status"] == "running"
    from flowstate.jobs.service import reconcile

    reconcile(RunStore(database_url), {})
    assert RunStore(database_url).get(body["run_id"])["status"] == "running"


def test_make_stream_serve_and_stream_ctl_are_gone():
    text = Path("Makefile").read_text()
    assert "stream-serve:" not in text
    assert "stream-ctl:" not in text
    assert "stream-produce:" in text
