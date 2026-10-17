"""RTI station-hour long table.

August measure is august_hourly. September entry and exit stay separate.
The two months are not summed. A null cell is omitted. A zero is kept.
"""

from __future__ import annotations

import re
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa

from flowstate.normalize.cities import dataset_path
from flowstate.normalize.cities.profile import CityProfile
from flowstate.normalize.clock import dates_covering, future_dates
from flowstate.normalize.coverage import TableBuild
from flowstate.normalize.sources import ensure_sources

_AUG_INTERVAL = re.compile(
    r"^(?P<start>\d{2}:\d{2}) Hrs To\s+(?P<end>.+)$"
)
_HOUR = re.compile(r"^H(\d{2})$")

COLUMNS = [
    "service_date",
    "station",
    "measure",
    "interval_label",
    "interval_start",
    "interval_end",
    "ends_at_last_train",
    "line_open",
    "count",
    "source_sheet",
    "source_row",
]


def parse_interval(label: str) -> tuple[str, str | None, bool]:
    """Return start, end, ends_at_last_train.

    The final August bucket ends at last train. Its end is null.
    It is not folded into the next day.
    """
    text = str(label).strip()
    hour = _HOUR.match(text)
    if hour:
        start_h = int(hour.group(1))
        return f"{start_h:02d}:00", f"{start_h + 1:02d}:00", False
    match = _AUG_INTERVAL.match(text)
    if not match:
        raise ValueError(f"unparsed interval {label!r}")
    end = match.group("end").strip()
    if end.casefold() == "last train":
        return match.group("start"), None, True
    end_match = re.match(r"(\d{2}:\d{2}) Hrs$", end)
    if not end_match:
        raise ValueError(f"unparsed interval end {label!r}")
    return match.group("start"), end_match.group(1), False


def build_station_hour(
    august: pd.DataFrame,
    september_entry: pd.DataFrame,
    september_exit: pd.DataFrame,
    *,
    line_name: str,
    line_open: date,
    service_spans: tuple[tuple[date, date], ...],
    august_sheet: str,
    entry_sheet: str,
    exit_sheet: str,
) -> TableBuild:
    expected = dates_covering(service_spans)
    parts = [
        _melt(august, "august_hourly", august_sheet, line_open),
        _melt(september_entry, "entry", entry_sheet, line_open),
        _melt(september_exit, "exit", exit_sheet, line_open),
    ]
    melted = pd.concat(parts, ignore_index=True)
    missing_cells = int(melted["count"].isna().sum())
    long = melted.dropna(subset=["count"]).copy()
    long["count"] = long["count"].astype("int64")
    negatives = int((long["count"] < 0).sum())
    dates = sorted(set(long["service_date"]))
    got = set(dates)
    missing_days = sorted(expected - got)
    extra_days = sorted(got - expected)
    key = ["service_date", "station", "measure", "interval_label"]
    dup_mask = long.duplicated(key, keep=False)
    duplicates = (
        long.loc[dup_mask, key]
        .drop_duplicates()
        .to_dict(orient="records")
    )
    long = long.sort_values(key).reset_index(drop=True)
    table = pa.Table.from_pandas(
        long[COLUMNS], preserve_index=False
    )
    august_rows = long[long["measure"] == "august_hourly"]
    by_day = august_rows.groupby("service_date")["station"].nunique()
    opened = _line_labels(august, line_open)
    labels = _labels(august, september_entry, september_exit)
    first_seen = _first_seen(august, september_entry, september_exit)
    coverage = {
        "min": dates[0] if dates else None,
        "max": dates[-1] if dates else None,
        "counts": {
            "rows": len(long),
            "days": len(got),
            "expected_days": len(expected),
            "line_open": {
                "line": line_name,
                "date": line_open.isoformat(),
                "day_before": (line_open - timedelta(days=1)).isoformat(),
                "stations_on_date": int(
                    by_day.get(line_open.isoformat(), 0)
                ),
                "stations_day_before": int(
                    by_day.get(
                        (line_open - timedelta(days=1)).isoformat(), 0
                    )
                ),
            },
            "negative_counts": negatives,
            "measures": {
                measure: int((long["measure"] == measure).sum())
                for measure in ("august_hourly", "entry", "exit")
            },
        },
        "missing": missing_days,
        "missing_cells": missing_cells,
        "extra_dates": extra_days,
        "duplicate_count": len(duplicates),
        "duplicates": duplicates[:20],
        "future_dates": future_dates(dates),
        "veracity": [
            "August and September are not summed.",
            "August measure is august_hourly. It is not entry or exit.",
            "September entry and exit stay on separate rows.",
            "A null cell is absent. A zero cell is kept.",
            "23:00 to last train is its own interval. interval_end is null.",
            f"line_open is true from {line_open.isoformat()} "
            f"for {line_name}.",
        ],
    }
    return TableBuild(
        table=table,
        coverage=coverage,
        dates=got,
        extra={
            "line_labels": opened,
            "rti_labels": labels,
            "first_seen": first_seen,
            "august_index": _august_index(august_rows),
        },
    )


def from_disk(datasets: Path, profile: CityProfile) -> TableBuild:
    ensure_sources([profile.rti_dataset])
    root = dataset_path(datasets, profile.rti_dir)
    august = pd.read_excel(root / profile.august_book)
    book = pd.ExcelFile(root / profile.september_book)
    entry = pd.read_excel(book, profile.september_entry_sheet)
    exit_ = pd.read_excel(book, profile.september_exit_sheet)
    return build_station_hour(
        august,
        entry,
        exit_,
        line_name=profile.line_name,
        line_open=profile.line_open,
        service_spans=profile.service_spans,
        august_sheet=profile.august_book.removesuffix(".xlsx"),
        entry_sheet=profile.september_entry_sheet,
        exit_sheet=profile.september_exit_sheet,
    )


def _melt(
    frame: pd.DataFrame, measure: str, sheet: str, line_open: date
) -> pd.DataFrame:
    work = frame.copy()
    work["source_row"] = work.index + 2
    id_cols = ["BUSINESS DATE", "STATION", "source_row"]
    labels = [
        column
        for column in work.columns
        if column not in id_cols and column != "TOTAL"
    ]
    long = work.melt(
        id_vars=id_cols,
        value_vars=labels,
        var_name="interval_label",
        value_name="count",
    )
    parsed = long["interval_label"].map(
        lambda label: parse_interval(str(label))
    )
    long["interval_start"] = parsed.map(lambda item: item[0])
    long["interval_end"] = parsed.map(lambda item: item[1])
    long["ends_at_last_train"] = parsed.map(lambda item: item[2])
    long["service_date"] = pd.to_datetime(long["BUSINESS DATE"]).dt.strftime(
        "%Y-%m-%d"
    )
    long["line_open"] = long["service_date"] >= line_open.isoformat()
    long["measure"] = measure
    long["source_sheet"] = sheet
    long["station"] = long["STATION"].astype(str)
    return long[COLUMNS]


def _labels(*frames: pd.DataFrame) -> list[str]:
    found: set[str] = set()
    for frame in frames:
        found.update(frame["STATION"].dropna().astype(str))
    return sorted(found)


def _first_seen(*frames: pd.DataFrame) -> dict[str, str]:
    first: dict[str, str] = {}
    for frame in frames:
        days = pd.to_datetime(frame["BUSINESS DATE"]).dt.strftime("%Y-%m-%d")
        stations = frame["STATION"]
        for station, day in zip(stations, days, strict=True):
            if pd.isna(station):
                continue
            label = str(station)
            prev = first.get(label)
            if prev is None or day < prev:
                first[label] = day
    return first


def _line_labels(august: pd.DataFrame, line_open: date) -> list[str]:
    """Stations present on the open date and absent the day before."""
    days = pd.to_datetime(august["BUSINESS DATE"]).dt.strftime("%Y-%m-%d")
    stations = august["STATION"].astype(str)
    opened = line_open.isoformat()
    previous = (line_open - timedelta(days=1)).isoformat()
    before = set(stations[days == previous])
    on_day = set(stations[days == opened])
    return sorted(on_day - before)


def _august_index(frame: pd.DataFrame) -> dict[tuple[str, str, str, str], int]:
    """(date, station, start, end) -> count. Last-train end is ''."""
    index: dict[tuple[str, str, str, str], int] = {}
    for row in frame.itertuples(index=False):
        end = row.interval_end or ""
        key = (
            row.service_date,
            row.station,
            row.interval_start,
            end,
        )
        index[key] = int(row.count)
    return index
