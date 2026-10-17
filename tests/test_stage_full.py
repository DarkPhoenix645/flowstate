"""Checks against the cold extracts. Skipped when those files are absent."""

import json
from pathlib import Path

import pyarrow.compute as pc
import pytest

from flowstate.config import Settings
from flowstate.provenance import require_valid
from flowstate.staged import read_parquet

MANIFEST = Path("data/raw/provenance/manifest.json")
pytestmark = pytest.mark.skipif(
    not MANIFEST.exists(),
    reason="cold extracts are not in this checkout",
)


def test_require_valid_for_the_staged_sources():
    require_valid(
        [
            "namma_yatri_city_day",
            "namma_yatri_city_hour",
            "namma_yatri_ward_hour",
            "namma_metro_daily",
            "bmrcl_rti",
            "bmtc_gtfs",
            "dgca_domestic_city",
        ]
    )


def test_staged_city_day_matches_the_clock():
    from flowstate.stage.ridehail import stage_ridehail

    coverage = stage_ridehail(Settings())
    table = read_parquet(Path("data/staged/namma_yatri/city_day.parquet"))
    assert table.column("date").to_pylist()[0] == "2022-09-01"
    assert table.column("date").to_pylist()[-1] == "2026-04-15"
    assert coverage["missing"] == ["2026-02-14"]
    assert coverage["counts"]["purple_rows_excluded"] > 0
    assert coverage["counts"]["rows"] == 1322
    assert coverage["counts"]["alignment_rows"] == 536
    assert set(table.column("booking_type").to_pylist()) == {"Normal"}
    assert set(table.column("city_code").to_pylist()) == {"BLR"}
    hour = read_parquet(Path("data/staged/namma_yatri/city_hour.parquet"))
    ward = read_parquet(Path("data/staged/namma_yatri/ward_hour.parquet"))
    assert hour.num_rows == 66726
    assert ward.num_rows == 3043468
    assert coverage["hourly_done_ride_ratio"] < 0.05


def test_staged_metro_tables():
    from flowstate.stage.metro import stage_metro

    summary = stage_metro(Settings())
    daily = read_parquet(Path("data/staged/metro/daily.parquet"))
    dates = daily.column("record_date").to_pylist()
    assert dates[0] == "2024-10-26"
    assert dates[-1] == "2026-07-15"
    assert summary["dropped_after_2026_07_15"] == 0
    assert summary["daily_rows"] == 438
    assert summary["alignment_rows"] == 373
    hour = read_parquet(Path("data/staged/metro/station_hour.parquet"))
    days = set(hour.column("service_date").to_pylist())
    assert len(days) == 48
    assert "2025-08-11" in days
    assert "2025-07-31" not in days
    cov = json.loads(
        Path("data/staged/metro/station_hour.coverage.json").read_text()
    )
    line = cov["counts"]["line_open"]
    assert line["stations_day_before"] == 69
    assert line["stations_on_date"] == 83
    last = hour.filter(pc.equal(hour.column("ends_at_last_train"), True))
    assert last.num_rows > 0
    assert last.column("interval_end").null_count == last.num_rows


def test_staged_dgca_is_monthly():
    from flowstate.stage.dgca import stage_dgca

    coverage = stage_dgca(Settings())
    table = read_parquet(Path("data/staged/dgca/city_month.parquet"))
    assert "date" not in table.column_names
    cities = set(table.column("city1").to_pylist()) | set(
        table.column("city2").to_pylist()
    )
    assert "BENGALURU" in cities
    assert coverage["month_min"] == "2015-04"
    assert coverage["month_max"] == "2026-07"
    assert coverage["rows"] == 7812
    assert coverage["duplicate_pair_months"] == 18
    assert coverage["alignment_months"] == 19


def test_staged_gtfs_report():
    from flowstate.stage.bus import stage_bus

    summary = stage_bus(Settings())
    assert summary["feed_timezone"] == "Asia/Kolkata"
    assert summary["rejected_trips"] >= 0
    assert Path("data/staged/bmtc/rejections.parquet").exists()
    assert Path("data/staged/bmtc/scheduled_stop_event.json").exists()
    event = Path("data/staged/bmtc/scheduled_stop_event.json").read_text()
    assert '"status": "derived"' in event
    assert '"status": "synthetic"' in event
    assert '"boardings": null' in event
