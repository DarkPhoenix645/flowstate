"""Metro tracker daily table. Rows after the profile cutoff are excluded."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa

from flowstate.normalize.cities import dataset_path
from flowstate.normalize.cities.profile import CityProfile
from flowstate.normalize.clock import future_dates, in_window, missing_dates
from flowstate.normalize.coverage import TableBuild
from flowstate.normalize.sources import ensure_sources

COLUMNS = [
    "record_date",
    "smart_cards",
    "ncmc",
    "commute",
    "tokens",
    "qr",
    "group_ticket",
    "casual",
    "total",
]


def build_metro_daily(
    frame: pd.DataFrame,
    *,
    last_valid: date,
    window_start: date,
    window_end: date,
) -> TableBuild:
    work = frame.copy()
    work["record_date"] = pd.to_datetime(work["Record Date"]).dt.date
    work = work.sort_values("record_date")
    dropped = work[work["record_date"] > last_valid]
    kept = work[work["record_date"] <= last_valid].copy()
    _assert_identities(kept)
    if kept["record_date"].duplicated().any():
        dupes = sorted(
            {
                day.isoformat()
                for day in kept.loc[
                    kept["record_date"].duplicated(), "record_date"
                ]
            }
        )
        raise ValueError(
            "metro daily duplicate dates: " + ", ".join(dupes[:5])
        )
    dates = [day.isoformat() for day in kept["record_date"]]
    missing = missing_dates(dates)
    out = pd.DataFrame(
        {
            "record_date": dates,
            "smart_cards": kept["Smart Cards"].astype("int64").to_numpy(),
            "ncmc": kept["NCMC"].astype("int64").to_numpy(),
            "commute": kept["Commute"].astype("int64").to_numpy(),
            "tokens": kept["Tokens"].astype("int64").to_numpy(),
            "qr": kept["QR"].astype("int64").to_numpy(),
            "group_ticket": kept["Group Ticket"].astype("int64").to_numpy(),
            "casual": kept["Casual"].astype("int64").to_numpy(),
            "total": kept["Total"].astype("int64").to_numpy(),
        }
    )
    table = pa.Table.from_pandas(out, preserve_index=False)
    alignment = [
        day for day in dates if in_window(day, window_start, window_end)
    ]
    excluded = sorted(day.isoformat() for day in dropped["record_date"])
    coverage = {
        "min": dates[0] if dates else None,
        "max": dates[-1] if dates else None,
        "counts": {
            "rows": len(dates),
            "alignment_rows": len(alignment),
            "dropped_after_cutoff": len(excluded),
            "cutoff": last_valid.isoformat(),
            "missing_dates_in_span": len(missing),
        },
        "missing": missing,
        "missing_cells": len(missing),
        "duplicate_count": 0,
        "duplicates": [],
        "future_dates": future_dates(dates),
        "excluded_after_cutoff": excluded,
        "identity": {
            "commute": "Smart Cards + NCMC",
            "casual": "Tokens + QR + Group Ticket",
            "total": "Commute + Casual",
            "holds": True,
        },
        "veracity": [
            "Use total alone. Do not add commute or casual on top of it.",
            f"Rows dated after {last_valid.isoformat()} are excluded.",
            "A missing date stays missing.",
        ],
    }
    return TableBuild(table=table, coverage=coverage, dates=set(dates))


def from_disk(datasets: Path, profile: CityProfile) -> TableBuild:
    ensure_sources([profile.metro_dataset], datasets)
    path = dataset_path(datasets, profile.metro_parts)
    return build_metro_daily(
        pd.read_csv(path),
        last_valid=profile.metro_last_valid,
        window_start=profile.window_start,
        window_end=profile.window_end,
    )


def _assert_identities(frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    commute = frame["Smart Cards"] + frame["NCMC"]
    casual = frame["Tokens"] + frame["QR"] + frame["Group Ticket"]
    total = frame["Commute"] + frame["Casual"]
    if not (frame["Commute"] == commute).all():
        raise ValueError("Commute != Smart Cards + NCMC")
    if not (frame["Casual"] == casual).all():
        raise ValueError("Casual != Tokens + QR + Group Ticket")
    if not (frame["Total"] == total).all():
        raise ValueError("Total != Commute + Casual")


def unresolved_future(dates: set[str], cutoff: date) -> list[str]:
    return sorted(
        day for day in dates if date.fromisoformat(day) > cutoff
    )
