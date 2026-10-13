import datetime as dt
import os
import subprocess
import sys
from pathlib import Path

import pytest

from flowstate.ingest import opensky_flights as sky
from flowstate.ingest.opensky_flights import (
    Client,
    OpenSkyCreditsExhausted,
)

UTC = dt.UTC
BASE = dt.datetime(2024, 10, 26, tzinfo=UTC)


def _windows():
    return [
        (
            BASE + dt.timedelta(days=2 * i),
            BASE + dt.timedelta(days=2 * i, hours=23),
        )
        for i in range(4)
    ]


def _clients():
    return [
        Client(index=1, client_id="a", secret="a"),
        Client(index=2, client_id="b", secret="b"),
    ]


def _patch(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(sky, "CACHE", tmp_path / "windows")
    monkeypatch.setattr(sky, "OPENSKY_DIR", tmp_path)
    monkeypatch.setattr(sky, "windows", _windows)
    monkeypatch.setattr(sky, "LEGACY_CACHE", tmp_path / "legacy")


def _flight(n: int) -> dict:
    return {
        "icao24": "abc123",
        "callsign": "TEST",
        "firstSeen": 1_700_000_000 + n,
        "lastSeen": 1_700_003_600 + n,
        "estDepartureAirport": "VOBL" if n % 2 else None,
        "estArrivalAirport": None,
    }


def test_arrivals_are_fetched_before_departures(monkeypatch, tmp_path):
    _patch(monkeypatch, tmp_path)
    order: list[str] = []

    def fetch_fn(client, direction, start, end):
        order.append(direction)
        return [_flight(len(order))]

    sky.run_pool(_clients(), fetch_fn)
    assert order[:4] == ["arrival"] * 4
    assert order[4:] == ["departure"] * 4


def test_429_stops_that_pair_and_the_window_is_retried(monkeypatch, tmp_path):
    _patch(monkeypatch, tmp_path)
    seen: list[int] = []

    def fetch_fn(client, direction, start, end):
        seen.append(client.index)
        if client.index == 1:
            raise OpenSkyCreditsExhausted("pair 1")
        return [_flight(len(seen))]

    sky.run_pool(_clients(), fetch_fn)
    assert seen.count(1) == 1
    assert seen.count(2) == 8


def test_credit_stop_keeps_arrival_csv(monkeypatch, tmp_path):
    _patch(monkeypatch, tmp_path)
    for index, (start, end) in enumerate(_windows()):
        sky._write_window(
            sky.cache_path("arrival", start, end), [_flight(index)]
        )
        if index < 3:
            sky._write_window(
                sky.cache_path("departure", start, end), [_flight(index)]
            )

    def fetch_fn(client, direction, start, end):
        raise OpenSkyCreditsExhausted(client.label())

    with pytest.raises(OpenSkyCreditsExhausted):
        sky.run_pool(_clients(), fetch_fn)
    arrival = sky.csv_path("arrival").read_text()
    departure = sky.csv_path("departure").read_text()
    assert arrival.count("\n") == 5
    assert departure.count("\n") == 4
    assert sky.coverage("arrival")["complete"] is True
    assert sky.coverage("departure")["complete"] is False


def test_cached_arrival_is_not_fetched(monkeypatch, tmp_path):
    _patch(monkeypatch, tmp_path)
    start, end = _windows()[0]
    sky._write_window(sky.cache_path("arrival", start, end), [_flight(0)])
    seen: list[str] = []

    def fetch_fn(client, direction, start, end):
        seen.append(direction)
        return [_flight(1)]

    sky.run_pool(_clients(), fetch_fn)
    assert seen.count("arrival") == 3
    assert seen.count("departure") == 4


def test_partial_file_is_not_cached(monkeypatch, tmp_path):
    _patch(monkeypatch, tmp_path)
    start, end = _windows()[0]
    path = sky.cache_path("arrival", start, end)
    path.parent.mkdir(parents=True)
    path.with_suffix(".json.partial").write_text("[]")
    assert sky.window_file("arrival", start, end) is None
    assert sky.coverage("arrival")["windows_cached"] == 0


def test_flat_arrival_cache_is_adopted(monkeypatch, tmp_path):
    _patch(monkeypatch, tmp_path)
    start, end = _windows()[0]
    flat = sky.CACHE / sky.cache_path("arrival", start, end).name
    flat.parent.mkdir(parents=True)
    flat.write_text("[]")
    assert sky.adopt_flat_arrival_cache() == 1
    assert not flat.exists()
    assert sky.window_file("arrival", start, end) is not None


def test_second_process_refuses_the_lock(tmp_path, monkeypatch):
    monkeypatch.setattr(sky, "OPENSKY_DIR", tmp_path)
    script = (
        "import os\n"
        "from pathlib import Path\n"
        "import flowstate.ingest.opensky_flights as sky\n"
        "sky.OPENSKY_DIR = Path(os.environ['LOCKDIR'])\n"
        "try:\n"
        "    with sky.fetch_lock():\n"
        "        pass\n"
        "except sky.OpenSkyFetchBusy:\n"
        "    raise SystemExit(3)\n"
        "raise SystemExit(0)\n"
    )
    env = os.environ.copy()
    env["LOCKDIR"] = str(tmp_path)
    env["PYTHONPATH"] = "src"
    with sky.fetch_lock():
        proc = subprocess.run(
            [sys.executable, "-c", script],
            env=env,
            cwd=sky.ROOT,
            capture_output=True,
            text=True,
        )
    assert proc.returncode == 3, proc.stderr
    with sky.fetch_lock():
        pass
