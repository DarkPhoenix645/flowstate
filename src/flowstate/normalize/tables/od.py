"""August entry-exit origin-destination (OD) profile.

Null cells are omitted. Zeros stay. The matrix is not scaled to the
station-hour sheet. August hourly is a different measure.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa

from flowstate.normalize.cities import dataset_path
from flowstate.normalize.cities.profile import CityProfile
from flowstate.normalize.clock import future_dates
from flowstate.normalize.coverage import TableBuild
from flowstate.normalize.sources import ensure_sources

_LABEL = re.compile(
    r"^(?P<day>\d{4}-\d{2}-\d{2})-(?P<start>\d{2})Hrs-(?P<end>\d+)hrs$"
)

COLUMNS = [
    "service_date",
    "interval_label",
    "interval_start",
    "interval_end",
    "origin",
    "destination",
    "count",
    "source_row",
]


def build_od_profile(
    frame: pd.DataFrame,
    code_to_rti: dict[str, str],
    august_index: dict[tuple[str, str, str, str], int],
) -> TableBuild:
    work = frame.copy()
    work["source_row"] = work.index + 2
    dest_cols = [
        column
        for column in work.columns
        if column not in {"BUSINESS DATE", "STATION", "source_row"}
    ]
    subtotals = _sheet_subtotals(work, dest_cols)
    null_origin = subtotals["null_station_rows_not_emitted"]
    work = work.dropna(subset=["STATION"])
    origins = set(work["STATION"].astype(str))
    dests = {str(column) for column in dest_cols}
    numeric = work[dest_cols].apply(pd.to_numeric, errors="coerce")
    missing_cells = int(numeric.isna().sum().sum())
    negatives = int((numeric < 0).sum().sum())
    long = work.melt(
        id_vars=["BUSINESS DATE", "STATION", "source_row"],
        value_vars=dest_cols,
        var_name="destination",
        value_name="count",
    )
    long = long.dropna(subset=["count"])
    parsed = long["BUSINESS DATE"].map(_parse_label)
    long["service_date"] = parsed.map(lambda item: item[0])
    long["interval_start"] = parsed.map(lambda item: item[1])
    long["interval_end"] = parsed.map(lambda item: item[2])
    long["interval_label"] = long["BUSINESS DATE"].astype(str)
    long["origin"] = long["STATION"].astype(str)
    long["destination"] = long["destination"].astype(str)
    long["count"] = long["count"].astype("int64")
    key = ["service_date", "origin", "destination", "interval_label"]
    dup_mask = long.duplicated(key, keep=False)
    duplicates = (
        long.loc[dup_mask, key].drop_duplicates().to_dict(orient="records")
    )
    diagonal = int(
        (
            (long["origin"] == long["destination"]) & (long["count"] != 0)
        ).sum()
    )
    long = long.sort_values(
        ["service_date", "origin", "destination", "interval_start"]
    ).reset_index(drop=True)
    table = pa.Table.from_pandas(long[COLUMNS], preserve_index=False)
    dates = sorted(set(long["service_date"]))
    checks = _checks(
        origins,
        dests,
        negatives,
        null_origin,
        subtotals,
        diagonal,
        long,
        code_to_rti,
        august_index,
    )
    coverage = {
        "min": dates[0] if dates else None,
        "max": dates[-1] if dates else None,
        "counts": {
            "rows": len(long),
            "origin_rows": int(len(work)),
            "destinations": len(dests),
            "null_origin_rows": null_origin,
            "negative_cells": negatives,
            "nonzero_diagonal_cells": diagonal,
        },
        "missing": [],
        "missing_cells": missing_cells,
        "duplicate_count": len(duplicates),
        "duplicates": duplicates[:20],
        "future_dates": future_dates(dates),
        "checks": checks,
        "veracity": [
            "Null origin-destination cells are absent. Zero cells are kept.",
            "Null station rows are sheet subtotals, not origins.",
            "A row is not added to its reverse.",
            "August hourly is not entry or exit. Margins are not scaled.",
            "23:00-24:00 is not the last-train bucket.",
        ],
    }
    return TableBuild(
        table=table, coverage=coverage, dates=set(dates)
    )


def from_disk(
    datasets: Path,
    profile: CityProfile,
    code_to_rti: dict[str, str],
    august_index: dict[tuple[str, str, str, str], int],
) -> TableBuild:
    ensure_sources([profile.rti_dataset])
    path = dataset_path(datasets, profile.rti_dir) / profile.od_book
    frame = pd.read_excel(path)
    return build_od_profile(frame, code_to_rti, august_index)


def _parse_label(value: object) -> tuple[str, str, str]:
    match = _LABEL.match(str(value).strip())
    if not match:
        raise ValueError(f"unparsed origin-destination label {value!r}")
    start = f"{int(match.group('start')):02d}:00"
    end = f"{int(match.group('end')):02d}:00"
    return match.group("day"), start, end


def _sheet_subtotals(
    frame: pd.DataFrame, dest_cols: list[str]
) -> dict[str, Any]:
    """Null station rows are interval subtotals. Do not emit them."""
    numeric = frame[dest_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
    labels = frame["BUSINESS DATE"].astype(str)
    exact = 0
    off_one = 0
    other: list[dict[str, Any]] = []
    for label, part in frame.groupby(labels, sort=False):
        named = part.index[part["STATION"].notna()]
        nulls = part.index[part["STATION"].isna()]
        if len(nulls) == 0 or len(named) == 0:
            other.append({"interval": str(label), "l1": None})
            continue
        total = numeric.loc[named].sum()
        best = max(nulls, key=lambda index: float(numeric.loc[index].sum()))
        delta = int((numeric.loc[best] - total).abs().sum())
        if delta == 0:
            exact += 1
        elif delta == 1:
            off_one += 1
        else:
            other.append({"interval": str(label), "l1": delta})
    return {
        "null_station_rows_not_emitted": int(frame["STATION"].isna().sum()),
        "intervals": int(labels.nunique()),
        "total_row_exact": exact,
        "total_row_off_by_one": off_one,
        "total_row_other": other,
        "note": (
            "A null station cell is a sheet subtotal, not an origin. "
            "The largest null row in an interval is the total. "
            "It is not written into the profile."
        ),
    }


def _checks(
    origins: set[str],
    dests: set[str],
    negatives: int,
    null_origin: int,
    subtotals: dict[str, Any],
    diagonal: int,
    long: pd.DataFrame,
    code_to_rti: dict[str, str],
    august_index: dict[tuple[str, str, str, str], int],
) -> dict[str, Any]:
    only_origin = sorted(origins - dests)
    only_dest = sorted(dests - origins)
    compared = 0
    unmatched_code = 0
    unmatched_interval = 0
    unmatched_cell = 0
    sum_od = 0
    sum_hourly = 0
    grouped = long.groupby(
        ["service_date", "origin", "interval_start", "interval_end"],
        sort=False,
    )["count"].sum()
    for (day, origin, start, end), od_sum in grouped.items():
        label = code_to_rti.get(str(origin))
        if label is None:
            unmatched_code += 1
            continue
        if end == "24:00":
            unmatched_interval += 1
            continue
        hourly = august_index.get((day, label, start, end))
        if hourly is None:
            unmatched_cell += 1
            continue
        compared += 1
        sum_od += int(od_sum)
        sum_hourly += int(hourly)
    axis_ok = not only_origin and not only_dest and negatives == 0
    other = subtotals["total_row_other"]
    sheet_ok = all(
        item["l1"] is not None and item["l1"] <= 2 for item in other
    )
    return {
        "pass": axis_ok and sheet_ok,
        "only_origin": only_origin,
        "only_destination": only_dest,
        "negative_cells": negatives,
        "null_origin_rows": null_origin,
        "sheet_subtotals": subtotals,
        "nonzero_diagonal_cells": diagonal,
        "hourly_margin": {
            "compared_origin_intervals": compared,
            "unmatched_code": unmatched_code,
            "unmatched_last_train_interval": unmatched_interval,
            "unmatched_cell": unmatched_cell,
            "od_sum": sum_od,
            "hourly_sum": sum_hourly,
            "scaled": False,
            "note": (
                "August station-hour is august_hourly, not entry or exit. "
                "The margin is a residual, not a scale factor."
            ),
        },
    }
