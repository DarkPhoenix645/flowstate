"""Metro daily tracker and the RTI station-hour long table.

Not the rides table. August and September are not summed.
"""

from __future__ import annotations

import json
import re
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa

from flowstate.config import Settings
from flowstate.provenance import require_valid
from flowstate.staged import staged_parquet_path, write_parquet

# Rows dated after this day are upstream corruption. ADR-0011.
METRO_LAST_VALID = date(2026, 7, 15)
YELLOW_LINE = date(2025, 8, 11)
ALIGNMENT_START = date(2024, 10, 26)
ALIGNMENT_END = date(2026, 4, 15)

_AUG_INTERVAL = re.compile(
    r"^(?P<start>\d{2}:\d{2}) Hrs To\s+(?P<end>.+)$"
)
_HOUR = re.compile(r"^H(\d{2})$")


def daily_table(frame: pd.DataFrame) -> tuple[pa.Table, dict[str, Any]]:
    """Drop rows after 2026-07-15. Assert identities. Do not fill gaps."""
    work = frame.copy()
    work["record_date"] = pd.to_datetime(work["Record Date"]).dt.date
    work = work.sort_values("record_date")
    dropped = work[work["record_date"] > METRO_LAST_VALID]
    kept = work[work["record_date"] <= METRO_LAST_VALID].copy()
    _assert_identities(kept)
    if kept["record_date"].duplicated().any():
        raise ValueError("metro daily has a duplicate date")
    dates = list(kept["record_date"])
    missing = _missing(dates)
    out = pd.DataFrame(
        {
            "record_date": [d.isoformat() for d in dates],
            "smart_cards": kept["Smart Cards"].astype("int64"),
            "ncmc": kept["NCMC"].astype("int64"),
            "commute": kept["Commute"].astype("int64"),
            "tokens": kept["Tokens"].astype("int64"),
            "qr": kept["QR"].astype("int64"),
            "group_ticket": kept["Group Ticket"].astype("int64"),
            "casual": kept["Casual"].astype("int64"),
            "total": kept["Total"].astype("int64"),
        }
    )
    table = pa.Table.from_pandas(out, preserve_index=False)
    in_window = [
        d for d in dates if ALIGNMENT_START <= d <= ALIGNMENT_END
    ]
    coverage = {
        "rows": len(out),
        "dropped_after_2026_07_15": int(len(dropped)),
        "date_min": dates[0].isoformat() if dates else None,
        "date_max": dates[-1].isoformat() if dates else None,
        "missing_dates_in_span": len(missing),
        "alignment_rows": len(in_window),
        "veracity": [
            "Use total alone. Do not add commute or casual on top of it.",
            "Rows dated after 2026-07-15 are excluded.",
            "Missing dates stay missing.",
        ],
    }
    return table, coverage


def station_hour_table(
    august: pd.DataFrame,
    september_entry: pd.DataFrame,
    september_exit: pd.DataFrame,
) -> tuple[pa.Table, dict[str, Any]]:
    """Long station-hour rows. Null cells are omitted. Zeroes stay."""
    parts = [
        _melt(august, "august_hourly", "aug_station_hourly"),
        _melt(september_entry, "entry", "Sep-2025 Entry"),
        _melt(september_exit, "exit", "Sep-2025 Exit"),
    ]
    long = pd.concat(parts, ignore_index=True)
    dates = sorted(set(long["service_date"]))
    expected = _rti_dates()
    got = set(dates)
    if got != expected:
        missing = sorted(expected - got)
        extra = sorted(got - expected)
        raise ValueError(
            f"RTI dates are not the 48-day set. missing={missing[:5]} "
            f"extra={extra[:5]}"
        )
    table = pa.Table.from_pandas(long, preserve_index=False)
    by_day = (
        long[long["measure"] == "august_hourly"]
        .groupby("service_date")["station"]
        .nunique()
    )
    coverage = {
        "rows": len(long),
        "days": len(expected),
        "measures": sorted(long["measure"].unique()),
        "stations_2025_08_10": int(by_day.get("2025-08-10", 0)),
        "stations_2025_08_11": int(by_day.get("2025-08-11", 0)),
        "veracity": [
            "August and September are not summed.",
            "August has one measure, august_hourly. It is not entry or exit.",
            "September entry and exit stay separate rows.",
            "A null cell is absent. A zero cell is kept.",
            "23:00 to last train is its own interval. interval_end is null.",
            "yellow_line_open is true from 2025-08-11.",
        ],
    }
    return table, coverage


def stage_metro(settings: Settings) -> dict[str, Any]:
    require_valid(["namma_metro_daily", "bmrcl_rti"])
    root = settings.datasets_dir
    daily = pd.read_csv(
        root / "namma-metro" / "NammaMetro_Ridership_Processed.csv"
    )
    daily_out, daily_cov = daily_table(daily)
    daily_path = staged_parquet_path(settings, "metro", "daily")
    write_parquet(daily_path, daily_out)
    _write_json(daily_path.with_suffix(".coverage.json"), daily_cov)

    rti = root / "bmrcl-rti"
    august = pd.read_excel(rti / "aug_station_hourly.xlsx")
    book = pd.ExcelFile(rti / "sep_station_hourly.xlsx")
    entry = pd.read_excel(book, "Sep-2025 Entry")
    exit_ = pd.read_excel(book, "Sep-2025 Exit")
    hour, hour_cov = station_hour_table(august, entry, exit_)
    hour_path = staged_parquet_path(settings, "metro", "station_hour")
    write_parquet(hour_path, hour)
    _write_json(hour_path.with_suffix(".coverage.json"), hour_cov)

    from flowstate.stage.events import metro_station_hour_event

    sample = hour.to_pylist()[0]
    event_path = hour_path.with_name("station_hour_event.json")
    _write_json(event_path, metro_station_hour_event(sample))
    return {
        "rows": daily_cov["rows"] + hour_cov["rows"],
        "daily_rows": daily_cov["rows"],
        "station_hour_rows": hour_cov["rows"],
        "alignment_rows": daily_cov["alignment_rows"],
        "dropped_after_2026_07_15": daily_cov["dropped_after_2026_07_15"],
    }


def parse_interval(label: str) -> tuple[str, str | None, bool]:
    """Return start, end, ends_at_last_train."""
    hour = _HOUR.match(label)
    if hour:
        start_h = int(hour.group(1))
        return f"{start_h:02d}:00", f"{start_h + 1:02d}:00", False
    match = _AUG_INTERVAL.match(label.strip())
    if not match:
        raise ValueError(f"unparsed interval {label!r}")
    end = match.group("end").strip()
    if end.lower() == "last train":
        return match.group("start"), None, True
    end_match = re.match(r"(\d{2}:\d{2}) Hrs$", end)
    if not end_match:
        raise ValueError(f"unparsed interval end {label!r}")
    return match.group("start"), end_match.group(1), False


def _melt(frame: pd.DataFrame, measure: str, sheet: str) -> pd.DataFrame:
    work = frame.copy()
    work["source_row"] = work.index + 2
    id_cols = ["BUSINESS DATE", "STATION", "source_row"]
    labels = [c for c in work.columns if c not in id_cols and c != "TOTAL"]
    long = work.melt(
        id_vars=id_cols,
        value_vars=labels,
        var_name="interval_label",
        value_name="count",
    )
    long = long.dropna(subset=["count"])
    parsed = long["interval_label"].map(parse_interval)
    long["interval_start"] = parsed.map(lambda item: item[0])
    long["interval_end"] = parsed.map(lambda item: item[1])
    long["ends_at_last_train"] = parsed.map(lambda item: item[2])
    long["service_date"] = pd.to_datetime(long["BUSINESS DATE"]).dt.strftime(
        "%Y-%m-%d"
    )
    long["yellow_line_open"] = long["service_date"] >= YELLOW_LINE.isoformat()
    long["measure"] = measure
    long["source_sheet"] = sheet
    long["station"] = long["STATION"].astype(str)
    long["count"] = long["count"].astype("int64")
    return long[
        [
            "service_date",
            "station",
            "measure",
            "interval_label",
            "interval_start",
            "interval_end",
            "ends_at_last_train",
            "yellow_line_open",
            "count",
            "source_sheet",
            "source_row",
        ]
    ]


def _assert_identities(frame: pd.DataFrame) -> None:
    commute = frame["Smart Cards"] + frame["NCMC"]
    casual = frame["Tokens"] + frame["QR"] + frame["Group Ticket"]
    total = frame["Commute"] + frame["Casual"]
    if not (frame["Commute"] == commute).all():
        raise ValueError("Commute != Smart Cards + NCMC")
    if not (frame["Casual"] == casual).all():
        raise ValueError("Casual != Tokens + QR + Group Ticket")
    if not (frame["Total"] == total).all():
        raise ValueError("Total != Commute + Casual")


def _rti_dates() -> set[str]:
    return _span(date(2025, 8, 1), date(2025, 8, 18)) | _span(
        date(2025, 9, 1), date(2025, 9, 30)
    )


def _span(start: date, end: date) -> set[str]:
    days = set()
    cursor = start
    while cursor <= end:
        days.add(cursor.isoformat())
        cursor += timedelta(days=1)
    return days


def _missing(dates: list[date]) -> list[str]:
    if not dates:
        return []
    have = set(dates)
    missing = []
    cursor = dates[0]
    while cursor <= dates[-1]:
        if cursor not in have:
            missing.append(cursor.isoformat())
        cursor += timedelta(days=1)
    return missing


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n")
