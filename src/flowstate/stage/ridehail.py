"""Namma Yatri analysis tables. Not ride-grain events."""

from __future__ import annotations

import gzip
import json
import shutil
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.csv as pv

from flowstate.config import Settings
from flowstate.provenance import require_valid
from flowstate.staged import staged_parquet_path, write_parquet

CITY_DAY_COLUMNS = [
    "date",
    "city",
    "city_code",
    "booking_type",
    "srch_rqst",
    "srch_fr_q",
    "srch_which_got_q",
    "srch_which_got_e",
    "booking",
    "done_ride",
    "cancel_ride",
    "earning",
    "earning_per_completed_ride_proxy",
]

# Zero-filled in the feed. Not written on the city-day clock table.
EXCLUDED_CANCEL_SPLIT = ("drvr_cancel", "rider_cancel")

ALIGNMENT_START = "2024-10-26"
ALIGNMENT_END = "2026-04-15"


def load_city_day(path: Path) -> list[dict[str, Any]]:
    raw = path.read_bytes()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    rows = json.loads(raw)
    if not isinstance(rows, list):
        raise ValueError(f"{path} is not a JSON list")
    return rows


def city_day_table(
    rows: list[dict[str, Any]],
) -> tuple[pa.Table, dict[str, Any]]:
    """BLR + Normal only. One row per date. Purple stays out."""
    purple = 0
    other_city = 0
    kept: list[dict[str, Any]] = []
    for row in rows:
        if row.get("city_code") != "BLR":
            other_city += 1
            continue
        if row.get("booking_type") != "Normal":
            if row.get("booking_type") == "Purple":
                purple += 1
            continue
        kept.append(_clock_row(row))
    kept.sort(key=lambda item: item["date"])
    dates = [item["date"] for item in kept]
    if len(dates) != len(set(dates)):
        raise ValueError("BLR Normal city-day has a duplicate date")
    missing = _missing_dates(dates)
    table = pa.Table.from_pylist(kept, schema=_city_day_schema())
    coverage = {
        "rows": len(kept),
        "date_min": dates[0] if dates else None,
        "date_max": dates[-1] if dates else None,
        "missing_dates": missing,
        "purple_rows_excluded": purple,
        "other_city_rows_excluded": other_city,
        "excluded_columns": list(EXCLUDED_CANCEL_SPLIT),
        "alignment_rows": sum(
            1 for d in dates if ALIGNMENT_START <= d <= ALIGNMENT_END
        ),
        "veracity": [
            "Purple is an accessibility product and is not in this table.",
            "drvr_cancel and rider_cancel are zero-filled and are not columns.",
            "The city-day file has no rate columns. None are invented.",
            "earning_per_completed_ride_proxy is earning / done_ride. "
            "It is not an observed fare.",
            "This table is not a booking-grain ride extract.",
        ],
    }
    return table, coverage


def stage_ridehail(settings: Settings) -> dict[str, Any]:
    require_valid(
        [
            "namma_yatri_city_day",
            "namma_yatri_city_hour",
            "namma_yatri_ward_hour",
        ]
    )
    root = settings.datasets_dir / "namma-yatri"
    rows = load_city_day(
        root / "city_agg_cumulative_trends_new_key.json.gz"
    )
    table, coverage = city_day_table(rows)
    dest = staged_parquet_path(settings, "namma_yatri", "city_day")
    write_parquet(dest, table)
    _write_json(dest.with_suffix(".coverage.json"), coverage)

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
        table,
    )
    coverage["veracity"].append(
        "The city-hour archive is a shape prior. "
        "hourly_done_ride_ratio is Normal Auto done_ride in that "
        "archive divided by city-day done_ride on the same dates. "
        "It is not the city volume."
    )
    _write_json(dest.with_suffix(".coverage.json"), coverage)
    coverage["rows"] = coverage["rows"]
    return coverage


def _clock_row(row: dict[str, Any]) -> dict[str, Any]:
    done = _int(row.get("done_ride"))
    earning = _float(row.get("earning"))
    proxy = None
    if done not in (None, 0) and earning is not None:
        proxy = earning / done
    return {
        "date": row.get("date"),
        "city": row.get("city"),
        "city_code": "BLR",
        "booking_type": "Normal",
        "srch_rqst": _int(row.get("srch_rqst")),
        "srch_fr_q": _int(row.get("srch_fr_q")),
        "srch_which_got_q": _int(row.get("srch_which_got_q")),
        "srch_which_got_e": _int(row.get("srch_which_got_e")),
        "booking": _int(row.get("booking")),
        "done_ride": done,
        "cancel_ride": _int(row.get("cancel_ride")),
        "earning": earning,
        "earning_per_completed_ride_proxy": proxy,
    }


def _city_day_schema() -> pa.Schema:
    ints = {
        "srch_rqst",
        "srch_fr_q",
        "srch_which_got_q",
        "srch_which_got_e",
        "booking",
        "done_ride",
        "cancel_ride",
    }
    fields = []
    for name in CITY_DAY_COLUMNS:
        if name in ints:
            fields.append(pa.field(name, pa.int64()))
        elif name in {"earning", "earning_per_completed_ride_proxy"}:
            fields.append(pa.field(name, pa.float64()))
        else:
            fields.append(pa.field(name, pa.string()))
    return pa.schema(fields)


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


def _missing_dates(dates: list[str]) -> list[str]:
    if not dates:
        return []
    from datetime import date, timedelta

    start = date.fromisoformat(dates[0])
    end = date.fromisoformat(dates[-1])
    have = set(dates)
    missing = []
    cursor = start
    while cursor <= end:
        text = cursor.isoformat()
        if text not in have:
            missing.append(text)
        cursor += timedelta(days=1)
    return missing


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n")


def _date_text(value: Any) -> str:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)[:10]


def _int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return float(value)
