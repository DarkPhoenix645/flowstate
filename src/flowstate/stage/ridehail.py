"""Namma Yatri staged tables. City-day comes from the normalizer.

The city-hour and ward-hour archives are not normalizer tables.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.csv as pv

from flowstate.config import Settings
from flowstate.normalize.cities import DEFAULT_CITY_ID, load
from flowstate.normalize.tables.city_day import from_disk as city_day_from_disk
from flowstate.provenance import require_valid
from flowstate.staged import staged_parquet_path, write_parquet


def stage_ridehail(settings: Settings) -> dict[str, Any]:
    require_valid(
        [
            "namma_yatri_city_day",
            "namma_yatri_city_hour",
            "namma_yatri_ward_hour",
        ]
    )
    profile = load(DEFAULT_CITY_ID)
    built = city_day_from_disk(settings.datasets_dir, profile)
    dest = staged_parquet_path(settings, "namma_yatri", "city_day")
    write_parquet(dest, built.table)
    root = settings.datasets_dir / "namma-yatri"
    coverage = dict(built.coverage)

    hour_rows = _stage_csv(
        settings,
        root / "stitched" / "blr_city_hourly.csv",
        "namma_yatri",
        "city_hour",
        root / "stitched" / "blr_city_hourly.coverage.json",
    )
    ward_rows = _stage_csv(
        settings,
        root / "stitched" / "blr_ward_hourly.csv",
        "namma_yatri",
        "ward_hour",
        root / "stitched" / "blr_ward_hourly.coverage.json",
    )
    coverage["city_hour_rows"] = hour_rows
    coverage["ward_hour_rows"] = ward_rows
    coverage["hourly_done_ride_ratio"] = _hourly_ratio(
        root / "stitched" / "blr_city_hourly.csv",
        built.table,
    )
    coverage["veracity"].append(
        "The city-hour archive is a shape prior. "
        "hourly_done_ride_ratio is Normal Auto done_ride in that "
        "archive divided by city-day done_ride on the same dates. "
        "It is not the city volume."
    )
    _write_json(dest.with_suffix(".coverage.json"), coverage)
    coverage["rows"] = coverage["counts"]["rows"]
    return coverage


def _stage_csv(
    settings: Settings,
    src: Path,
    dataset: str,
    name: str,
    coverage_src: Path,
) -> int:
    table = pv.read_csv(src)
    dest = staged_parquet_path(settings, dataset, name)
    write_parquet(dest, table)
    if coverage_src.exists():
        shutil.copy(coverage_src, dest.with_suffix(".coverage.json"))
    return table.num_rows


def _hourly_ratio(path: Path, city_day: pa.Table) -> float | None:
    frame = pv.read_csv(
        path,
        convert_options=pv.ConvertOptions(
            include_columns=[
                "date",
                "booking_type",
                "vehicle_type",
                "done_ride",
            ]
        ),
    ).to_pydict()
    hourly: dict[str, int] = {}
    for date, booking, vehicle, done in zip(
        frame["date"],
        frame["booking_type"],
        frame["vehicle_type"],
        frame["done_ride"],
        strict=True,
    ):
        if done is None:
            continue
        if str(booking) != "Normal" or str(vehicle) != "Auto":
            continue
        key = _date_text(date)
        hourly[key] = hourly.get(key, 0) + int(done)
    dates = [_date_text(value) for value in city_day.column("date").to_pylist()]
    dones = city_day.column("done_ride").to_pylist()
    clock = 0
    thin = 0
    for date, done in zip(dates, dones, strict=True):
        if date not in hourly or done is None:
            continue
        clock += int(done)
        thin += hourly[date]
    if clock == 0:
        return None
    return thin / clock


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n")


def _date_text(value: Any) -> str:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)[:10]

