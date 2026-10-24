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
    "FLOWSTATE_OPENSKY_CLIENT_ID_5",
    "FLOWSTATE_OPENSKY_CLIENT_SECRET_5",
    "FLOWSTATE_OPENSKY_CLIENT_ID_6",
    "FLOWSTATE_OPENSKY_CLIENT_SECRET_6",
    "FLOWSTATE_OPENSKY_CLIENT_ID_7",
    "FLOWSTATE_OPENSKY_CLIENT_SECRET_7",
    "FLOWSTATE_OPENSKY_CLIENT_ID_8",
    "FLOWSTATE_OPENSKY_CLIENT_SECRET_8",
    "FLOWSTATE_OPENSKY_CLIENT_ID_9",
    "FLOWSTATE_OPENSKY_CLIENT_SECRET_9",
    "FLOWSTATE_OPENSKY_CLIENT_ID_10",
    "FLOWSTATE_OPENSKY_CLIENT_SECRET_10",
]


def _clear_opensky(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in _OPENSKY_KEYS:
        monkeypatch.delenv(key, raising=False)


def test_default_data_dir():
    s = Settings()
    assert s.data_dir.name == "data"
    assert s.datasets_dir == s.raw_dir
    assert s.spark_master == "spark://spark:7077"


def test_opensky_clients_collect_complete_pairs(monkeypatch):
    _clear_opensky(monkeypatch)
    monkeypatch.setenv("FLOWSTATE_OPENSKY_CLIENT_ID", "one")
    monkeypatch.setenv("FLOWSTATE_OPENSKY_CLIENT_SECRET", "s1")
    monkeypatch.setenv("FLOWSTATE_OPENSKY_CLIENT_ID_3", "three")
    monkeypatch.setenv("FLOWSTATE_OPENSKY_CLIENT_SECRET_3", "s3")
    clients = Settings().opensky_clients()
    assert clients == [(1, "one", "s1"), (3, "three", "s3")]


def test_opensky_pair_10_is_accepted(monkeypatch):
    _clear_opensky(monkeypatch)
    monkeypatch.setenv("FLOWSTATE_OPENSKY_CLIENT_ID_10", "ten")
    monkeypatch.setenv("FLOWSTATE_OPENSKY_CLIENT_SECRET_10", "s10")
    assert Settings().opensky_clients() == [(10, "ten", "s10")]


def test_opensky_incomplete_pair_raises(monkeypatch):
    _clear_opensky(monkeypatch)
    monkeypatch.setenv("FLOWSTATE_OPENSKY_CLIENT_ID_2", "two")
    with pytest.raises(RuntimeError, match="pair 2"):
        Settings().opensky_clients()


_BUS_PRIOR_KEYS = [
    "FLOWSTATE_ASSUMED_WEEKDAY_BOARDINGS",
    "FLOWSTATE_ASSUMED_BUS_CAPACITY",
    "FLOWSTATE_ASSUMED_MAX_LOAD_FACTOR",
    "FLOWSTATE_METRO_SUBSTITUTION_MULTIPLIER",
    "FLOWSTATE_BUS_HOUR_PROFILE",
    "FLOWSTATE_BUS_PEAK_SHARE_LOW",
    "FLOWSTATE_BUS_PEAK_SHARE_HIGH",
    "FLOWSTATE_BUS_TRIP_LENGTH_KM_LOW",
    "FLOWSTATE_BUS_TRIP_LENGTH_KM_HIGH",
]


def _clear_bus_prior(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in _BUS_PRIOR_KEYS:
        monkeypatch.delenv(key, raising=False)


def test_bus_prior_defaults_when_env_absent(monkeypatch):
    _clear_bus_prior(monkeypatch)
    s = Settings(_env_file=None)
    assert s.assumed_weekday_boardings == 4_600_000
    assert s.assumed_bus_capacity == 41.3
    assert s.assumed_max_load_factor == 1.4
    assert s.metro_substitution_multiplier == 1.0
    assert s.bus_hour_profile == "metro_ridehail_blend"
    assert s.bus_peak_share_low == 0.47
    assert s.bus_peak_share_high == 0.51
    assert s.bus_trip_length_km_low == 10.7
    assert s.bus_trip_length_km_high == 10.9


def test_bus_prior_empty_env_means_default(monkeypatch):
    _clear_bus_prior(monkeypatch)
    for key in _BUS_PRIOR_KEYS:
        monkeypatch.setenv(key, "  ")
    s = Settings(_env_file=None)
    assert s.assumed_weekday_boardings == 4_600_000
    assert s.assumed_bus_capacity == 41.3
    assert s.assumed_max_load_factor == 1.4
    assert s.metro_substitution_multiplier == 1.0
    assert s.bus_hour_profile == "metro_ridehail_blend"
    assert s.bus_peak_share_low == 0.47
    assert s.bus_peak_share_high == 0.51
    assert s.bus_trip_length_km_low == 10.7
    assert s.bus_trip_length_km_high == 10.9


def test_bus_prior_present_env_overrides(monkeypatch):
    _clear_bus_prior(monkeypatch)
    monkeypatch.setenv("FLOWSTATE_ASSUMED_WEEKDAY_BOARDINGS", "3310000")
    monkeypatch.setenv("FLOWSTATE_ASSUMED_BUS_CAPACITY", "40.0")
    monkeypatch.setenv("FLOWSTATE_ASSUMED_MAX_LOAD_FACTOR", "1.2")
    monkeypatch.setenv("FLOWSTATE_METRO_SUBSTITUTION_MULTIPLIER", "1.0")
    monkeypatch.setenv("FLOWSTATE_BUS_HOUR_PROFILE", "metro_ridehail_blend")
    monkeypatch.setenv("FLOWSTATE_BUS_PEAK_SHARE_LOW", "0.47")
    monkeypatch.setenv("FLOWSTATE_BUS_PEAK_SHARE_HIGH", "0.51")
    monkeypatch.setenv("FLOWSTATE_BUS_TRIP_LENGTH_KM_LOW", "10.7")
    monkeypatch.setenv("FLOWSTATE_BUS_TRIP_LENGTH_KM_HIGH", "10.9")
    s = Settings(_env_file=None)
    assert s.assumed_weekday_boardings == 3_310_000
    assert s.assumed_bus_capacity == 40.0
    assert s.assumed_max_load_factor == 1.2

