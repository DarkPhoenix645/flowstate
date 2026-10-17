"""Stage the #37 metro tables and one station-hour envelope.

The daily and station-hour builders live in ``flowstate.normalize``.
This module only writes them under ``data/staged/`` and the sample event.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from flowstate.config import Settings
from flowstate.normalize.cities import DEFAULT_CITY_ID, load
from flowstate.normalize.tables.metro_daily import from_disk as metro_from_disk
from flowstate.normalize.tables.station_hour import (
    from_disk as station_from_disk,
)
from flowstate.provenance import require_valid
from flowstate.stage.events import metro_station_hour_event
from flowstate.staged import staged_parquet_path, write_parquet


def stage_metro(settings: Settings) -> dict[str, Any]:
    require_valid(["namma_metro_daily", "bmrcl_rti"])
    profile = load(DEFAULT_CITY_ID)
    daily = metro_from_disk(settings.datasets_dir, profile)
    hour = station_from_disk(settings.datasets_dir, profile)
    daily_path = staged_parquet_path(settings, "metro", "daily")
    hour_path = staged_parquet_path(settings, "metro", "station_hour")
    write_parquet(daily_path, daily.table)
    _write_json(daily_path.with_suffix(".coverage.json"), daily.coverage)
    write_parquet(hour_path, hour.table)
    _write_json(hour_path.with_suffix(".coverage.json"), hour.coverage)
    sample = hour.table.to_pylist()[0]
    _write_json(
        hour_path.with_name("station_hour_event.json"),
        metro_station_hour_event(sample),
    )
    daily_counts = daily.coverage["counts"]
    hour_counts = hour.coverage["counts"]
    return {
        "rows": daily_counts["rows"] + hour_counts["rows"],
        "daily_rows": daily_counts["rows"],
        "station_hour_rows": hour_counts["rows"],
        "alignment_rows": daily_counts["alignment_rows"],
        "dropped_after_2026_07_15": daily_counts["dropped_after_cutoff"],
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n")
