from pathlib import Path

import pyarrow as pa
import pytest

from flowstate.config import Settings
from flowstate.fixtures import ride_fixture_rows
from flowstate.schema import RIDE_COLUMNS, ZONE_ID_BY_MODE, empty_row
from flowstate.staged import (
    read_parquet,
    read_rides,
    staged_parquet_path,
    write_parquet,
    write_rides,
)

_ZONE_VALUES = {
    "fixture-ridehail-auto": ("ward_id", "b_112", "b_84", "auto"),
    "fixture-ridehail-cab": ("ward_id", "b_84", "b_112", "cab"),
    "fixture-metro-station": ("station_id", "BYPH", "MAGR", "train"),
    "fixture-bus-stop": ("stop_id", "21629", None, "bus"),
    "fixture-aviation-arrival": ("airport", None, "VOBL", "airplane"),
    "fixture-aviation-departure": ("airport", "VOBL", None, "airplane"),
}


def test_staged_path_is_under_staged(tmp_path: Path):
    settings = Settings(data_dir=tmp_path)
    path = staged_parquet_path(settings, "fixture", "rides")
    assert path == tmp_path / "staged" / "fixture" / "rides.parquet"
    assert staged_parquet_path(settings, "fixture", "rides.parquet") == path


def test_staged_path_rejects_traversal(tmp_path: Path):
    settings = Settings(data_dir=tmp_path)
    with pytest.raises(ValueError):
        staged_parquet_path(settings, "../raw", "rides")
    with pytest.raises(ValueError):
        staged_parquet_path(settings, "fixture", "a/b")


def test_fixture_roundtrip(tmp_path: Path):
    settings = Settings(data_dir=tmp_path)
    rows = ride_fixture_rows()
    path = write_rides(rows, settings, "fixture")
    assert path == tmp_path / "staged" / "fixture" / "rides.parquet"
    assert path.is_file()
    assert not (Path("data") / "staged" / "fixture").exists()

    loaded = read_rides(path)
    assert len(loaded) == len(rows)
    for got, want in zip(loaded, rows, strict=True):
        assert list(got)[: len(RIDE_COLUMNS)] == RIDE_COLUMNS
        for column in RIDE_COLUMNS:
            assert got[column] == want[column]
        assert got["city"] == "bengaluru"
        assert got["fare"] is None
        kind, pickup, drop, vehicle = _ZONE_VALUES[got["trip_id"]]
        assert ZONE_ID_BY_MODE[got["mode"]] == kind
        assert got["pickup_zone"] == pickup
        assert got["drop_zone"] == drop
        assert got["vehicle_type"] == vehicle
        for zone in (got["pickup_zone"], got["drop_zone"]):
            assert zone is None or isinstance(zone, str)


def test_fixture_is_not_a_rejected_taxi_extract():
    banned = ("uber", "ola", "kaggle", "ncr")
    for row in ride_fixture_rows():
        blob = " ".join(str(v).lower() for v in row.values() if v is not None)
        for word in banned:
            assert word not in blob
        assert set(row) == set(RIDE_COLUMNS)


def test_zone_column_rejects_non_strings(tmp_path: Path):
    settings = Settings(data_dir=tmp_path)
    row = empty_row(
        trip_id="bad-zone",
        city="bengaluru",
        mode="ride_hail",
        vehicle_type="auto",
        timestamp="2025-08-01T00:00:00+05:30",
        pickup_zone=112,
        status="completed",
        source="namma_yatri",
    )
    with pytest.raises(TypeError, match="pickup_zone"):
        write_rides([row], settings, "fixture", name="bad")


def test_fare_roundtrip(tmp_path: Path):
    settings = Settings(data_dir=tmp_path)
    row = empty_row(
        trip_id="fare",
        city="bengaluru",
        mode="ride_hail",
        vehicle_type="auto",
        timestamp="2025-08-01T00:00:00+05:30",
        pickup_zone="b_112",
        status="completed",
        fare=12.5,
        source="synthetic",
    )
    path = write_rides([row], settings, "fixture", name="fare")
    assert read_rides(path)[0]["fare"] == 12.5


def test_extra_column_follows_canonical_columns(tmp_path: Path):
    settings = Settings(data_dir=tmp_path)
    row = empty_row(
        trip_id="extra",
        city="bengaluru",
        mode="metro",
        vehicle_type="train",
        timestamp="2025-08-01T08:00:00+05:30",
        pickup_zone="BYPH",
        status="completed",
        source="bmrcl_rti",
    )
    row["note"] = "station-hour shape"
    path = write_rides([row], settings, "fixture", name="extra")
    loaded = read_rides(path)[0]
    assert list(loaded)[: len(RIDE_COLUMNS)] == RIDE_COLUMNS
    assert loaded["note"] == "station-hour shape"


def test_generic_parquet_helper_uses_staged_path(tmp_path: Path):
    settings = Settings(data_dir=tmp_path)
    path = staged_parquet_path(settings, "helper", "plain")
    table = pa.table({"value": ["ok"]})
    write_parquet(path, table)
    assert read_parquet(path).column("value").to_pylist() == ["ok"]
    assert path == tmp_path / "staged" / "helper" / "plain.parquet"
