"""Counts from the extracts already on disk. No network. No Compose."""

from datetime import date
from pathlib import Path

import pytest

from flowstate.config import Settings
from flowstate.normalize.coverage import sha256_file
from flowstate.normalize.run import TABLES, run

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "data" / "raw" / "provenance" / "manifest.json"

pytestmark = pytest.mark.skipif(
    not MANIFEST.exists(), reason="extracts are not on this disk"
)


def test_normalize_disk(tmp_path):
    settings = Settings(
        data_dir=tmp_path / "data",
        datasets_dir=ROOT / "data" / "raw",
    )
    result = run(settings)
    tables = result["tables"]
    assert list(tables) == list(TABLES)
    for coverage in tables.values():
        path = Path(settings.data_dir) / "normalized" / coverage["parquet"]
        assert sha256_file(path) == coverage["sha256"]
        assert "trip_id" not in _columns(path)

    city = tables["namma_yatri_city_day"]
    assert city["rows"] == 1322
    assert city["min"] == "2022-09-01"
    assert city["max"] == "2026-04-15"
    assert city["missing"] == ["2026-02-14"]
    assert city["counts"]["alignment_rows"] == 536
    assert city["future_dates"] == []
    assert city["pairwise_overlaps"]["city_day_and_metro_in_alignment"] == 372

    metro = tables["metro_daily"]
    assert metro["rows"] == 438
    assert metro["min"] == "2024-10-26"
    assert metro["max"] == "2026-07-15"
    assert metro["counts"]["dropped_after_cutoff"] == 0
    assert metro["counts"]["cutoff"] == "2026-07-15"
    assert metro["counts"]["missing_dates_in_span"] == 190
    assert metro["counts"]["alignment_rows"] == 373
    assert metro["future_dates"] == []
    assert metro["excluded_after_cutoff"] == []

    hour = tables["metro_station_hour_long"]
    assert hour["counts"]["days"] == 48
    assert hour["missing"] == []
    opened = hour["counts"]["line_open"]
    assert opened["line"] == "yellow"
    assert opened["date"] == "2025-08-11"
    assert opened["stations_day_before"] == 69
    assert opened["stations_on_date"] == 83
    assert set(hour["counts"]["measures"]) == {
        "august_hourly",
        "entry",
        "exit",
    }

    geo = tables["station_geography"]
    assert geo["counts"]["codes"] == 95
    assert geo["counts"]["line_stations"] == 14
    assert geo["counts"]["line_name"] == "yellow"
    assert geo["counts"]["coordinate_cells"] == 0
    assert result["geocode"]["needs_review"] == []
    geo_rows = _rows(settings, "station_geography")
    assert all(row["latitude"] is None for row in geo_rows)
    assert all(row["longitude"] is None for row in geo_rows)
    hsli = next(row for row in geo_rows if row["station_code"] == "HSLI")
    assert hsli["name"] == (
        "Sri Balagangadharanatha Swamiji Station, Hosahalli"
    )
    yellow = [row for row in geo_rows if row["line"] == "yellow"]
    assert len(yellow) == 14
    on_opening = [
        row for row in yellow if row["operational_from"] == "2025-08-11"
    ]
    earlier = [
        row for row in yellow if row["operational_from"] < "2025-08-11"
    ]
    assert len(on_opening) == 9
    assert len(earlier) == 5

    airports = tables["airport_geography"]
    assert airports["rows"] == 1
    assert airports["extracts"] == 1
    assert airports["pins"] == 0
    airport_rows = _rows(settings, "airport_geography")
    assert [row["airport_code"] for row in airport_rows] == ["VOBL"]
    assert airport_rows[0]["opensky_extract"] is True
    assert airport_rows[0]["latitude"] is None

    wards = tables["ward_geography"]
    assert wards["matched"] == 243
    assert wards["unmatched"] == 0
    assert wards["out_of_scope"] == 101
    assert wards["coordinate_order"] == "latitude_longitude"
    assert wards["city"] == "bengaluru"
    ward_rows = _rows(settings, "ward_geography")
    assert len(ward_rows) == 344
    by_ward = {row["ward_num"]: row for row in ward_rows}
    assert by_ward["b_1"]["ward_name"] == "Kempegowda Ward"
    assert by_ward["b_84"]["ward_name"] == "Ramamurthy Nagara"
    assert by_ward["b_112"]["ward_name"] == "Varthuru"
    assert by_ward["b_243"]["ward_name"] == "Kudlu"
    assert by_ward["m_1"]["match_status"] == "out_of_scope"
    assert by_ward["w_0"]["match_status"] == "out_of_scope"
    for row in ward_rows:
        if row["match_status"] != "matched":
            assert row["latitude"] is None
            assert row["longitude"] is None
            continue
        assert 12.5 < row["latitude"] < 13.3
        assert 77.3 < row["longitude"] < 77.9

    od = tables["od_profile"]
    subtotals = od["checks"]["sheet_subtotals"]
    assert subtotals["total_row_exact"] == 358
    assert subtotals["total_row_off_by_one"] == 15
    assert subtotals["total_row_other"] == [
        {"interval": "2025-08-15-13Hrs-14hrs", "l1": 2}
    ]
    assert (
        od["rows"] + od["missing_cells"]
        == od["counts"]["origin_rows"] * od["counts"]["destinations"]
    )
    assert od["checks"]["hourly_margin"]["scaled"] is False

    flights = tables["flight_events"]
    assert flights["counts"]["arrivals"] == 72408
    assert flights["counts"]["departures"] == 129531
    assert flights["counts"]["rows"] == 72408 + 129531
    status = flights["counts"]["query_status"]
    assert status["fetched"] == 217 + 268
    assert status["404-empty"] == 52 + 1
    assert status["failed"] == 0
    assert status["quota"] == 0
    assert flights["counts"]["unassigned_events"] == 0
    assert flights["duplicate_count"] == 0
    flight_rows = _rows(settings, "flight_events")
    assert {row["provenance_status"] for row in flight_rows} == {
        "observed_proxy"
    }

    dates = tables["significant_dates_classified"]
    assert dates["rows"] == 137
    assert dates["duplicate_count"] == 0
    assert dates["min"] == "2024-10-31"
    assert set(dates["counts"]["locality"]) == {
        "bengaluru_local",
        "citywide_holiday",
        "nonlocal",
        "uncertain",
    }
    assert all(value > 0 for value in dates["counts"]["locality"].values())
    today = date.today().isoformat()
    for day in dates["future_dates"]:
        assert day > today
    date_rows = _rows(settings, "significant_dates_classified")
    assert len(date_rows) == 137

    assert result["gates"]["G1"]["pass"] is True
    assert result["gates"]["G1"]["shared_dates_in_alignment"] == 372
    assert result["gates"]["G1"]["unresolved_future_dates"] == []
    assert result["gates"]["G2"]["pass"] is True
    assert result["gates"]["G2"]["city"]["n_commits"] == 720
    assert result["gates"]["G2"]["city"]["duplicate_commit_dates"] == 3
    assert result["gates"]["G3"]["pass"] is True
    assert result["gates"]["G3"]["rti_days"] == 48


def _columns(path: Path) -> list[str]:
    import pyarrow.parquet as pq

    return pq.read_schema(path).names


def _rows(settings: Settings, name: str) -> list[dict]:
    import pyarrow.parquet as pq

    path = settings.data_dir / "normalized" / f"{name}.parquet"
    return pq.read_table(path).to_pylist()
