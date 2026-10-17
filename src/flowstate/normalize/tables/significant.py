"""Classify significant-date labels for one city's lexicon.

Classes are exclusive. A label that mixes a local or holiday part with a
nonlocal part is uncertain. Weather and strikes are not in the file.
"""

from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

import pyarrow as pa

from flowstate.normalize.cities import dataset_path
from flowstate.normalize.cities.profile import CityProfile, LocalityLexicon
from flowstate.normalize.clock import future_dates, in_window
from flowstate.normalize.coverage import TableBuild
from flowstate.normalize.sources import ensure_sources

SHARED_CLASSES = ("citywide_holiday", "nonlocal", "uncertain")


def classes(lexicon: LocalityLexicon) -> tuple[str, ...]:
    return (lexicon.local_class, *SHARED_CLASSES)


def classify_label(label: str, lexicon: LocalityLexicon) -> str:
    parts = [part.strip() for part in label.split(";") if part.strip()]
    kinds = {
        _classify_part(part, lexicon) for part in parts
    } or {"uncertain"}
    concrete = kinds - {"uncertain"}
    if "nonlocal" in concrete and concrete - {"nonlocal"}:
        return "uncertain"
    if lexicon.local_class in concrete:
        return lexicon.local_class
    if "citywide_holiday" in concrete:
        return "citywide_holiday"
    if "nonlocal" in concrete:
        return "nonlocal"
    return "uncertain"


def build_significant_dates(
    rows: list[tuple[str, str]],
    lexicon: LocalityLexicon,
    *,
    window_start: date,
    window_end: date,
) -> TableBuild:
    built = [
        {
            "date": day,
            "label": label,
            "locality": classify_label(label, lexicon),
        }
        for day, label in rows
    ]
    built.sort(key=lambda item: item["date"])
    dates = [item["date"] for item in built]
    duplicates = sorted({day for day in dates if dates.count(day) > 1})
    counts = {name: 0 for name in classes(lexicon)}
    for item in built:
        counts[item["locality"]] += 1
    table = pa.Table.from_pylist(built, schema=_schema())
    coverage = {
        "min": dates[0] if dates else None,
        "max": dates[-1] if dates else None,
        "counts": {
            "rows": len(built),
            "labels": len({item["label"] for item in built}),
            "locality": counts,
            "alignment_rows": sum(
                1
                for day in dates
                if in_window(day, window_start, window_end)
            ),
        },
        "missing": [],
        "missing_cells": 0,
        "duplicate_count": len(duplicates),
        "duplicates": duplicates,
        "future_dates": future_dates(dates),
        "veracity": [
            "A row is a label, not a measured demand shock.",
            "Mixed local and nonlocal parts are uncertain.",
            "Future calendar dates stay in the table.",
            "They are not ridership.",
        ],
    }
    return TableBuild(table=table, coverage=coverage, dates=set(dates))


def from_disk(datasets: Path, profile: CityProfile) -> TableBuild:
    ensure_sources([profile.dates_dataset])
    path = dataset_path(datasets, profile.dates_parts)
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        rows = [(row["Date"], row["Event"]) for row in reader]
    return build_significant_dates(
        rows,
        profile.lexicon,
        window_start=profile.window_start,
        window_end=profile.window_end,
    )


def _classify_part(part: str, lexicon: LocalityLexicon) -> str:
    folded = part.casefold()
    if _nonlocal(folded, lexicon):
        return "nonlocal"
    if any(token in folded for token in lexicon.local_tokens):
        return lexicon.local_class
    if any(token in folded for token in lexicon.holiday_tokens):
        return "citywide_holiday"
    return "uncertain"


def _nonlocal(folded: str, lexicon: LocalityLexicon) -> bool:
    if any(phrase in folded for phrase in lexicon.nonlocal_phrases):
        return True
    named_here = any(name in folded for name in lexicon.city_names)
    if any(event in folded for event in lexicon.away_unless_city):
        return not named_here
    return any(place in folded for place in lexicon.other_places)


def _schema() -> pa.Schema:
    return pa.schema(
        [
            ("date", pa.string()),
            ("label", pa.string()),
            ("locality", pa.string()),
        ]
    )
