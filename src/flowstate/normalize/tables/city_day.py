"""City-day clock for one profile. Not ride-grain rows."""

from __future__ import annotations

import gzip
import json
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow as pa

from flowstate.normalize.cities import dataset_path
from flowstate.normalize.cities.profile import CityProfile
from flowstate.normalize.clock import future_dates, in_window, missing_dates
from flowstate.normalize.coverage import TableBuild
from flowstate.normalize.sources import ensure_sources

COLUMNS = [
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

COUNT_COLUMNS = (
    "srch_rqst",
    "srch_fr_q",
    "srch_which_got_q",
    "srch_which_got_e",
    "booking",
    "done_ride",
    "cancel_ride",
)

# Published names. This extract does not carry them. Do not invent them.
RATE_COLUMNS = ("cnvr_rate", "bkng_cancel_rate", "q_accept_rate")


def load_city_day(path: Path) -> list[dict[str, Any]]:
    raw = path.read_bytes()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    rows = json.loads(raw)
    if not isinstance(rows, list):
        raise ValueError(f"{path} is not a JSON list")
    return rows


def build_city_day(
    rows: list[dict[str, Any]],
    *,
    city_code: str,
    booking_type: str,
    window_start: date,
    window_end: date,
) -> TableBuild:
    """Keep one city code and booking type.

    A missing date is not a zero row.
    """
    purple = 0
    other_city = 0
    other_booking = 0
    cancel_split_nonzero = 0
    booking_residual = 0
    rate_present = sorted(
        {
            key
            for row in rows
            for key in row
            if key in RATE_COLUMNS or "rate" in key.casefold()
        }
    )
    kept: list[dict[str, Any]] = []
    for row in rows:
        if row.get("city_code") != city_code:
            other_city += 1
            continue
        if row.get("booking_type") != booking_type:
            if row.get("booking_type") == "Purple":
                purple += 1
            else:
                other_booking += 1
            continue
        if _int(row.get("drvr_cancel")) or _int(row.get("rider_cancel")):
            cancel_split_nonzero += 1
        booking = _int(row.get("booking"))
        done = _int(row.get("done_ride"))
        cancel = _int(row.get("cancel_ride"))
        if (
            booking is not None
            and done is not None
            and cancel is not None
            and booking != done + cancel
        ):
            booking_residual += 1
        kept.append(_clock_row(row))
    kept.sort(key=lambda item: item["date"])
    dates = [item["date"] for item in kept]
    duplicates = sorted({day for day in dates if dates.count(day) > 1})
    if duplicates:
        raise ValueError(
            f"{city_code} {booking_type} city-day duplicate dates: "
            + ", ".join(duplicates[:5])
        )
    missing = missing_dates(dates)
    table = pa.Table.from_pylist(kept, schema=_schema())
    alignment = [
        day
        for day in dates
        if in_window(day, window_start, window_end)
    ]
    coverage = {
        "min": dates[0] if dates else None,
        "max": dates[-1] if dates else None,
        "counts": {
            "rows": len(kept),
            "alignment_rows": len(alignment),
            "purple_rows_excluded": purple,
            "other_city_rows_excluded": other_city,
            "other_booking_rows_excluded": other_booking,
            "booking_not_done_plus_cancel": booking_residual,
            "cancel_split_nonzero": cancel_split_nonzero,
        },
        "missing": missing,
        "missing_cells": len(missing),
        "duplicate_count": 0,
        "duplicates": [],
        "future_dates": future_dates(dates),
        "rate_columns_present": rate_present,
        "excluded_columns": ["drvr_cancel", "rider_cancel"],
        "veracity": [
            "Purple is excluded. It is an accessibility product.",
            "drvr_cancel and rider_cancel stay off this table.",
            "Rate columns are not in the file and are not invented.",
            "booking is not rewritten to done_ride + cancel_ride.",
            "A missing date is absent. It is not a zero row.",
            "earning_per_completed_ride_proxy is earning / done_ride.",
            "This table is not a ride-grain extract.",
        ],
    }
    return TableBuild(table=table, coverage=coverage, dates=set(dates))


def from_disk(datasets: Path, profile: CityProfile) -> TableBuild:
    ensure_sources([profile.city_day_dataset])
    path = dataset_path(datasets, profile.city_day_parts)
    return build_city_day(
        load_city_day(path),
        city_code=profile.source_city_code,
        booking_type=profile.booking_type,
        window_start=profile.window_start,
        window_end=profile.window_end,
    )


def _clock_row(row: dict[str, Any]) -> dict[str, Any]:
    done = _int(row.get("done_ride"))
    earning = _float(row.get("earning"))
    proxy = None
    if done not in (None, 0) and earning is not None:
        proxy = earning / done
    return {
        "date": row.get("date"),
        "city": row.get("city"),
        "city_code": row.get("city_code"),
        "booking_type": row.get("booking_type"),
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


def _schema() -> pa.Schema:
    fields = []
    for name in COLUMNS:
        if name in COUNT_COLUMNS:
            fields.append(pa.field(name, pa.int64()))
        elif name in {"earning", "earning_per_completed_ride_proxy"}:
            fields.append(pa.field(name, pa.float64()))
        else:
            fields.append(pa.field(name, pa.string()))
    return pa.schema(fields)


def _int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise TypeError(f"bad count {value!r}")
    number = float(value)
    if not number.is_integer():
        raise ValueError(f"count is not an integer: {value!r}")
    return int(number)


def _float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise TypeError(f"bad earning {value!r}")
    return float(value)
