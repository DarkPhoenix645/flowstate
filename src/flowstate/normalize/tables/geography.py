"""Station geography from station_codes.csv and RTI labels.

The code file has no coordinates.
Pins come from the Places cache, keyed by station name.
A miss stays null.
Names with an unquoted comma (HSLI) keep the full name.
"""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import pyarrow as pa

from flowstate.normalize.cities import dataset_path
from flowstate.normalize.cities.profile import CityProfile
from flowstate.normalize.coverage import TableBuild
from flowstate.normalize.sources import ensure_sources

COLUMNS = [
    "station_code",
    "name",
    "rti_label",
    "line",
    "operational_from",
    "operational_to",
    "latitude",
    "longitude",
    "source",
    "ambiguous_codes",
]

_LEADING_CODE = re.compile(r"^\d+-")


def parse_station_codes(text: str) -> list[tuple[str, str]]:
    """Split code,name. Extra commas belong to the name."""
    rows: list[tuple[str, str]] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.casefold().startswith("code,"):
            continue
        parts = stripped.split(",")
        code = parts[0].strip()
        name = ",".join(parts[1:]).strip()
        if not code or not name:
            raise ValueError(f"bad station code line: {line!r}")
        rows.append((code, name))
    return rows


def norm_name(value: str) -> str:
    text = _LEADING_CODE.sub("", value.strip())
    text = text.replace(".", "")
    text = re.sub(r"\s+", " ", text)
    return text.casefold()


def build_geography(
    codes: list[tuple[str, str]],
    rti_labels: list[str],
    line_labels: list[str],
    first_seen: dict[str, str] | None = None,
    *,
    line_name: str,
    line_open: date,
    aliases: dict[str, str] | None = None,
) -> TableBuild:
    by_name: dict[str, list[tuple[str, str]]] = {}
    by_code = dict(codes)
    for code, name in codes:
        by_name.setdefault(norm_name(name), []).append((code, name))
    alias_map = aliases or {}
    opened = set(line_labels)
    seen = first_seen or {}
    open_day = line_open.isoformat()
    assigned: dict[str, list[str]] = {}
    rows: list[dict[str, object]] = []
    for label in sorted(set(rti_labels)):
        candidates = _candidates(label, by_name, alias_map)
        if len(candidates) == 1:
            assigned.setdefault(candidates[0][0], []).append(label)
            continue
        ambiguous = (
            "|".join(sorted(code for code, _name in candidates))
            if len(candidates) > 1
            else None
        )
        on_line = label in opened
        rows.append(
            _row(
                station_code=None,
                name=_LEADING_CODE.sub("", label),
                rti_label=label,
                line=line_name if on_line else None,
                operational_from=_operational_from(
                    [label], on_line, seen, open_day
                ),
                source="rti",
                ambiguous_codes=ambiguous,
            )
        )
    for code, labels in assigned.items():
        name = by_code[code]
        on_line = any(label in opened for label in labels)
        rows.append(
            _row(
                station_code=code,
                name=name,
                rti_label=_pick_label(labels, name),
                line=line_name if on_line else None,
                operational_from=_operational_from(
                    labels, on_line, seen, open_day
                ),
                source="station_codes+rti",
                ambiguous_codes=None,
            )
        )
    for code, name in sorted(codes):
        if code in assigned:
            continue
        rows.append(
            _row(
                station_code=code,
                name=name,
                rti_label=None,
                line=None,
                operational_from=None,
                source="station_codes",
                ambiguous_codes=None,
            )
        )
    rows.sort(
        key=lambda item: (
            item["rti_label"] or "",
            item["station_code"] or "",
            item["name"] or "",
        )
    )
    table = pa.Table.from_pylist(rows, schema=_schema())
    matched = sum(1 for row in rows if row["source"] == "station_codes+rti")
    coverage = {
        "min": None,
        "max": None,
        "counts": {
            "rows": len(rows),
            "codes": len(codes),
            "rti_labels": len(set(rti_labels)),
            "matched": matched,
            "line_stations": len(opened),
            "line_name": line_name,
            "line_open": open_day,
            "line_first_seen_before_opening": sum(
                1
                for label in opened
                if seen.get(label, open_day) < open_day
            ),
            "code_only": sum(
                1 for row in rows if row["source"] == "station_codes"
            ),
            "rti_only": sum(1 for row in rows if row["source"] == "rti"),
            "coordinate_cells": sum(
                row["latitude"] is not None for row in rows
            ),
        },
        "missing": [],
        "missing_cells": 0,
        "duplicate_count": 0,
        "duplicates": [],
        "future_dates": [],
        "veracity": [
            "station_codes.csv has no coordinates.",
            "A pin comes from Places. A miss stays null.",
            "A name is matched only when the normalized name is unique.",
            f"{line_name} stations are present on {open_day} "
            "and absent the day before.",
            "An alias joins an RTI label onto the station code row.",
            "operational_from is the first RTI date for that station.",
            f"It is not rewritten to {open_day} when an earlier row exists.",
            "A station that is only in station_codes.csv stays null.",
        ],
    }
    code_to_label = {
        row["station_code"]: row["rti_label"]
        for row in rows
        if row["station_code"] and row["rti_label"]
    }
    return TableBuild(
        table=table,
        coverage=coverage,
        dates=set(),
        extra={"code_to_rti": code_to_label},
    )


def from_disk(
    datasets: Path,
    profile: CityProfile,
    rti_labels: list[str],
    line_labels: list[str],
    first_seen: dict[str, str] | None = None,
) -> TableBuild:
    ensure_sources([profile.rti_dataset])
    path = (
        dataset_path(datasets, profile.rti_dir) / profile.station_codes_name
    )
    codes = parse_station_codes(path.read_text())
    return build_geography(
        codes,
        rti_labels,
        line_labels,
        first_seen,
        line_name=profile.line_name,
        line_open=profile.line_open,
        aliases=dict(profile.station_aliases),
    )


def apply_station_pins(
    built: TableBuild,
    pins: dict[str, tuple[float, float] | None],
) -> TableBuild:
    """Set latitude and longitude from a name-keyed pin map."""
    rows = built.table.to_pylist()
    for row in rows:
        point = pins.get(str(row["name"]))
        if point is None:
            row["latitude"] = None
            row["longitude"] = None
            continue
        row["latitude"], row["longitude"] = point
    table = pa.Table.from_pylist(rows, schema=_schema())
    coverage = dict(built.coverage)
    counts = dict(coverage["counts"])
    counts["coordinate_cells"] = sum(
        row["latitude"] is not None for row in rows
    )
    coverage["counts"] = counts
    return TableBuild(
        table=table,
        coverage=coverage,
        dates=built.dates,
        extra=built.extra,
    )


def _candidates(
    label: str,
    by_name: dict[str, list[tuple[str, str]]],
    aliases: dict[str, str],
) -> list[tuple[str, str]]:
    bare = _LEADING_CODE.sub("", label).strip()
    keys = {norm_name(bare)}
    if bare in aliases:
        keys.add(norm_name(aliases[bare]))
    for key, target in aliases.items():
        if target == bare or norm_name(target) == norm_name(bare):
            keys.add(norm_name(key))
            keys.add(norm_name(target))
    found: dict[str, tuple[str, str]] = {}
    for key in keys:
        for code, name in by_name.get(key, []):
            found[code] = (code, name)
    return list(found.values())


def _pick_label(labels: list[str], name: str) -> str:
    wanted = norm_name(name)
    for label in sorted(labels):
        bare = _LEADING_CODE.sub("", label).strip()
        if norm_name(bare) == wanted:
            return label
    return sorted(labels)[0]


def _operational_from(
    labels: list[str],
    on_line: bool,
    first_seen: dict[str, str],
    line_open: str,
) -> str | None:
    dates = [first_seen[label] for label in labels if label in first_seen]
    if dates:
        return min(dates)
    if on_line:
        return line_open
    return None


def _row(
    station_code: str | None,
    name: str,
    rti_label: str | None,
    line: str | None,
    operational_from: str | None,
    source: str,
    ambiguous_codes: str | None,
) -> dict[str, object]:
    return {
        "station_code": station_code,
        "name": name,
        "rti_label": rti_label,
        "line": line,
        "operational_from": operational_from,
        "operational_to": None,
        "latitude": None,
        "longitude": None,
        "source": source,
        "ambiguous_codes": ambiguous_codes,
    }


def _schema() -> pa.Schema:
    return pa.schema(
        [
            ("station_code", pa.string()),
            ("name", pa.string()),
            ("rti_label", pa.string()),
            ("line", pa.string()),
            ("operational_from", pa.string()),
            ("operational_to", pa.string()),
            ("latitude", pa.float64()),
            ("longitude", pa.float64()),
            ("source", pa.string()),
            ("ambiguous_codes", pa.string()),
        ]
    )
