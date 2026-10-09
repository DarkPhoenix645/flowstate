import pytest

from flowstate.config import Settings

_OPENSKY_KEYS = [
    "FLOWSTATE_OPENSKY_CLIENT_ID",
    "FLOWSTATE_OPENSKY_CLIENT_SECRET",
    "FLOWSTATE_OPENSKY_CLIENT_ID_2",
    "FLOWSTATE_OPENSKY_CLIENT_SECRET_2",
    "FLOWSTATE_OPENSKY_CLIENT_ID_3",
    "FLOWSTATE_OPENSKY_CLIENT_SECRET_3",
    "FLOWSTATE_OPENSKY_CLIENT_ID_4",
    "FLOWSTATE_OPENSKY_CLIENT_SECRET_4",
    "OPENSKY_CLIENT_ID",
    "OPENSKY_CLIENT_SECRET",
    "OPENSKY_CLIENT_ID_1",
    "OPENSKY_CLIENT_SECRET_1",
]


def _clear_opensky(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in _OPENSKY_KEYS:
        monkeypatch.delenv(key, raising=False)


def test_default_data_dir():
    s = Settings()
    assert s.data_dir.name == "data"
    assert s.spark_master == "spark://spark:7077"


def test_opensky_clients_use_flowstate_prefix_only(monkeypatch):
    _clear_opensky(monkeypatch)
    monkeypatch.setenv("OPENSKY_CLIENT_ID", "legacy")
    monkeypatch.setenv("OPENSKY_CLIENT_SECRET", "legacy-secret")
    monkeypatch.setenv("FLOWSTATE_OPENSKY_CLIENT_ID", "one")
    monkeypatch.setenv("FLOWSTATE_OPENSKY_CLIENT_SECRET", "s1")
    monkeypatch.setenv("FLOWSTATE_OPENSKY_CLIENT_ID_3", "three")
    monkeypatch.setenv("FLOWSTATE_OPENSKY_CLIENT_SECRET_3", "s3")
    clients = Settings().opensky_clients()
    assert clients == [(1, "one", "s1"), (3, "three", "s3")]


def test_opensky_unprefixed_names_are_ignored(monkeypatch):
    _clear_opensky(monkeypatch)
    monkeypatch.setenv("OPENSKY_CLIENT_ID", "legacy")
    monkeypatch.setenv("OPENSKY_CLIENT_SECRET", "legacy-secret")
    assert Settings().opensky_clients() == []


def test_opensky_incomplete_pair_raises(monkeypatch):
    _clear_opensky(monkeypatch)
    monkeypatch.setenv("FLOWSTATE_OPENSKY_CLIENT_ID_2", "two")
    with pytest.raises(RuntimeError, match="pair 2"):
        Settings().opensky_clients()
