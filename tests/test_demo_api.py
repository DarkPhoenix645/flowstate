"""Demo API routes. No run routes."""

from __future__ import annotations

import json
import os
from pathlib import Path

import psycopg
import pytest

from flowstate.api.server import dispatch
from flowstate.config import Settings
from flowstate.config.demo import get_settings
from flowstate.config.model import ResampleConfig
from flowstate.config.postgres import SCHEMA_VERSION, ConfigStore
from flowstate.ingest.opensky_flights import windows

_SECRET = "opensky-client-secret-value"
_LOCAL_URL = (
    "postgresql://flowstate:flowstate@127.0.0.1:5432/flowstate"
)


def _store() -> ConfigStore:
    return ConfigStore(
        f"postgresql://flowstate:{_SECRET}@127.0.0.1:1/flowstate"
    )


def _knob(body: dict, name: str) -> dict:
    return next(item for item in body["knobs"] if item["name"] == name)


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
        pytest.skip("Postgres schema is not version 2")
    with psycopg.connect(url) as conn:
        conn.execute(
            "TRUNCATE config_active, config_version RESTART IDENTITY"
        )
        conn.execute("DELETE FROM run_config")
        conn.execute("DELETE FROM benchmark")
    return url


def test_unknown_config_field_is_422_and_hides_the_secret():
    status, body = dispatch(
        _store(),
        "PUT",
        "/api/config",
        json.dumps({"brokers": "localhost:9094"}).encode(),
    )
    assert status == 422
    text = json.dumps(body)
    assert _SECRET not in text
    assert "9094" not in text


def test_snapshot_is_a_fixture_and_has_no_credentials():
    os.environ["FLOWSTATE_OPENSKY_CLIENT_SECRET"] = _SECRET
    status, body = dispatch(_store(), "GET", "/api/events/snapshot", b"")
    assert status == 200
    from flowstate.streaming.ride_event_schema import RideEvent

    assert body["fixture"] is True
    event = body["events"][0]
    assert "fixture" not in event
    assert event["event_id"] == "fixture-1"
    RideEvent.model_validate(event)
    text = json.dumps(body)
    assert _SECRET not in text
    assert "client_secret" not in text


def test_system_names_the_five_host_ports():
    status, body = dispatch(_store(), "GET", "/api/system", b"")
    assert status == 200
    names = [row["name"] for row in body["services"]]
    assert names == ["namenode", "kafka", "spark", "hive", "grafana"]
    assert _SECRET not in json.dumps(body)


def test_datasets_reads_the_manifest(tmp_path: Path):
    raw = tmp_path / "raw"
    (raw / "provenance").mkdir(parents=True)
    (raw / "provenance" / "manifest.json").write_text(
        json.dumps(
            {
                "datasets": [
                    {
                        "dataset_id": "rides",
                        "valid": True,
                        "invalid_reason": None,
                    }
                ]
            }
        )
    )

    class _Settings:
        datasets_dir = raw

    status, body = dispatch(
        _store(), "GET", "/api/datasets", b"", settings=_Settings()
    )
    assert status == 200
    assert body["datasets"] == [
        {"dataset_id": "rides", "valid": True, "invalid_reason": None}
    ]


def test_incomplete_departures_are_409_and_arrivals_stay_ok(
    tmp_path: Path, monkeypatch
):
    import flowstate.ingest.opensky_flights as sky

    monkeypatch.setattr(sky, "CACHE", tmp_path / "windows")
    monkeypatch.setattr(sky, "LEGACY_CACHE", tmp_path / "legacy")
    (tmp_path / "legacy").mkdir()
    start, end = windows()[0]
    path = sky.cache_path("arrival", start, end)
    path.parent.mkdir(parents=True)
    path.write_text("{}")
    os.environ["FLOWSTATE_OPENSKY_CLIENT_SECRET"] = _SECRET

    class _Settings:
        datasets_dir = tmp_path

    status, arrivals = dispatch(
        _store(),
        "GET",
        "/api/datasets/opensky?direction=arrivals",
        b"",
        settings=_Settings(),
    )
    assert status == 200
    assert arrivals["windows_cached"] == 1
    assert arrivals["windows_required"] == len(windows())
    assert arrivals["gap"] == len(windows()) - 1
    assert "days" not in arrivals

    status, departures = dispatch(
        _store(),
        "GET",
        "/api/datasets/opensky?direction=departures",
        b"",
        settings=_Settings(),
    )
    assert status == 409
    assert departures["gap"] == len(windows())
    assert _SECRET not in json.dumps(departures)


@pytest.mark.integration
def test_config_put_merges_and_rejects_without_a_row(
    database_url, monkeypatch
):
    for name in (
        "FLOWSTATE_ASSUMED_WEEKDAY_BOARDINGS",
        "FLOWSTATE_ASSUMED_BUS_CAPACITY",
        "FLOWSTATE_ASSUMED_MAX_LOAD_FACTOR",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(
        "flowstate.config.demo.get_settings",
        lambda: Settings(_env_file=None),
    )
    store = ConfigStore(database_url)
    status, body = dispatch(
        store,
        "GET",
        "/api/config",
        b"",
        settings=Settings(_env_file=None),
    )
    assert status == 200
    assert _knob(body, "assumed_weekday_boardings")["source"] == "default"
    assert _knob(body, "assumed_weekday_boardings")["value"] == (
        ResampleConfig().assumed_weekday_boardings
    )
    assert _knob(body, "metro_substitution_multiplier")["editable"] is False
    assert body["config_hash"] is None

    monkeypatch.setattr("flowstate.config.demo.get_settings", get_settings)
    monkeypatch.setenv("FLOWSTATE_ASSUMED_WEEKDAY_BOARDINGS", "4800000")
    status, body = dispatch(store, "GET", "/api/config", b"")
    assert _knob(body, "assumed_weekday_boardings")["source"] == "env"
    assert _knob(body, "assumed_weekday_boardings")["value"] == 4_800_000

    before = store.list_versions("resample_config")
    status, body = dispatch(
        store,
        "PUT",
        "/api/config",
        json.dumps({"assumed_weekday_boardings": 4_000_000}).encode(),
    )
    assert status == 200
    assert _knob(body, "assumed_weekday_boardings")["source"] == "db"
    assert _knob(body, "assumed_weekday_boardings")["value"] == 4_000_000
    kept = _knob(body, "assumed_bus_capacity")["value"]
    monkeypatch.setenv("FLOWSTATE_ASSUMED_BUS_CAPACITY", "99")
    status, body = dispatch(store, "GET", "/api/config", b"")
    assert _knob(body, "assumed_bus_capacity")["value"] == kept
    assert _knob(body, "assumed_bus_capacity")["source"] == "db"

    status, _body = dispatch(
        store,
        "PUT",
        "/api/config",
        json.dumps({"assumed_max_load_factor": 9}).encode(),
    )
    assert status == 422
    assert len(store.list_versions("resample_config")) == len(before) + 1

    status, body = dispatch(
        store,
        "PUT",
        "/api/config",
        json.dumps({"replay_seed": 7}).encode(),
    )
    assert status == 200
    assert _knob(body, "replay_seed") == {
        "name": "replay_seed",
        "value": 7,
        "source": "db",
        "editable": True,
    }
    assert len(store.list_versions("resample_config")) == len(before) + 2
    assert _knob(body, "assumed_weekday_boardings")["source"] == "db"
    assert _knob(body, "assumed_weekday_boardings")["value"] == 4_000_000

    status, body = dispatch(store, "POST", "/api/config/reset", b"")
    assert status == 200
    assert _knob(body, "replay_seed")["source"] == "default"
    assert store.replay_seed() is None
    assert "resample_config" not in store.active()
    assert store.list_versions("resample_config")


@pytest.mark.integration
def test_benchmarks_are_404_until_a_row_exists(database_url):
    store = ConfigStore(database_url)
    status, body = dispatch(store, "GET", "/api/benchmarks", b"")
    assert status == 404
    assert body["error"] == "no comparison has been run"
    with psycopg.connect(database_url) as conn:
        columns = conn.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'benchmark'
            """
        ).fetchall()
    names = {row[0] for row in columns}
    assert "run_id" in names
