"""Pre-emptive event checks. Flag nonsense before anything ships.

Two layers: per-row checks run while batches are generated and again
when the written parquet is read back, and parent checks compare
read-back sums against the recorded parent totals. ``amplify`` fails
loudly on any issue; a clean corpus is a claim, not a default.

The checks mirror ADR-0010 and the plan §10.3 minimum tests: no
synthetic record labelled observed, no aggregate counted with its
children, no event at a station before it exists, allocator
conservation, no duplicates, flags present, and no synthetic
aviation inside an observed window.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pyarrow.parquet as pq

from flowstate.resample.bundle import FARE_BASIS, ModelBundle
from flowstate.resample.unfold import (
    EVIDENCE_GRADES,
    EXTRA_COLUMNS,
    METRIC_EVENT,
    PARENT_SOURCES,
    PROVENANCE_STATUSES,
)
from flowstate.schema import MODES, RIDE_COLUMNS

_EVENT_METRIC = {event: metric for metric, event in METRIC_EVENT.items()}

MAX_ISSUES = 200


class CorpusError(RuntimeError):
    """Raised when the corpus fails validation."""


class _Issues:
    def __init__(self) -> None:
        self.items: list[str] = []

    def add(self, message: str) -> None:
        if len(self.items) < MAX_ISSUES:
            self.items.append(message)

    def raise_if_any(self, header: str) -> None:
        if self.items:
            shown = "\n".join(self.items)
            extra = ""
            if len(self.items) >= MAX_ISSUES:
                extra = "\n... and more"
            raise CorpusError(f"{header}:\n{shown}{extra}")


def check_rows(
    rows: list[dict[str, Any]],
    bundle: ModelBundle,
    issues: _Issues,
    seen_ids: set[str],
) -> None:
    wards = {
        ward
        for hour in range(24)
        for ward in bundle.ward_shares[hour][0]
    }
    cap = (
        bundle.config.assumed_bus_capacity
        * bundle.config.assumed_max_load_factor
    )
    window = (bundle.spec.window_start, bundle.spec.window_end)
    for row in rows:
        try:
            _check_row(row, bundle, wards, cap, window, issues, seen_ids)
        except (KeyError, TypeError, ValueError) as error:
            issues.add(f"{row.get('trip_id', '?')}: malformed row: {error}")


def _check_row(
    row: dict[str, Any],
    bundle: ModelBundle,
    wards: set[str],
    cap: float,
    window: Any,
    issues: _Issues,
    seen_ids: set[str],
) -> None:
    event_id = row["trip_id"]
    if event_id in seen_ids:
        issues.add(f"{event_id}: duplicate event id")
    seen_ids.add(event_id)
    missing = [
        name
        for name in (*RIDE_COLUMNS, *EXTRA_COLUMNS)
        if name not in row
    ]
    if missing:
        issues.add(f"{event_id}: missing columns {missing}")
        return
    mode = row["mode"]
    if mode not in MODES:
        issues.add(f"{event_id}: unknown mode {mode!r}")
        return
    if row["provenance_status"] == "observed":
        # The corpus is resampler output. Observed rows belong to
        # the source tables, never to rides_amplified.
        issues.add(f"{event_id}: corpus row labelled observed")
    if row["provenance_status"] not in PROVENANCE_STATUSES:
        issues.add(f"{event_id}: bad provenance {row['provenance_status']!r}")
    if row["evidence_grade"] not in EVIDENCE_GRADES:
        issues.add(f"{event_id}: bad evidence grade {row['evidence_grade']!r}")
    if row["parent_source"] not in PARENT_SOURCES:
        issues.add(f"{event_id}: bad parent source {row['parent_source']!r}")
    if row["model_bundle_version"] != bundle.version:
        issues.add(f"{event_id}: bundle version mismatch")
    if row["seed_id"] != bundle.seed_id:
        issues.add(f"{event_id}: seed id mismatch")
    if row["scenario_id"] is not None:
        issues.add(f"{event_id}: scenario id on a non-scenario corpus row")
    if row["provenance_status"] == "synthetic" and row["source"] != "synthetic":
        issues.add(f"{event_id}: synthetic row claims source {row['source']!r}")
    moment = datetime.fromisoformat(row["timestamp"])
    if moment.tzinfo is None:
        issues.add(f"{event_id}: timestamp without offset")
        return
    if int(row["hour"]) != moment.hour:
        issues.add(
            f"{event_id}: hour column {row['hour']} != timestamp hour "
            f"{moment.hour}"
        )
    outside = row["outside_training_window"] == "true"
    in_window = window[0] <= moment.date() <= window[1]
    if outside == in_window:
        issues.add(
            f"{event_id}: outside_training_window is {outside} but the "
            f"date {moment.date()} window membership is {in_window}"
        )
    if mode == "ride_hail":
        _check_ridehail(row, wards, issues, event_id)
    elif mode == "metro":
        _check_metro(row, bundle, issues, event_id, moment.date())
    elif mode == "bus":
        _check_bus(row, cap, issues, event_id)
    elif mode == "aviation":
        _check_aviation(row, bundle, issues, event_id)


def _check_ridehail(
    row: dict[str, Any], wards: set[str], issues: _Issues, event_id: str
) -> None:
    if row["vehicle_type"] not in ("auto", "cab"):
        issues.add(
            f"{event_id}: ride_hail vehicle_type "
            f"{row['vehicle_type']!r}"
        )
    if row["pickup_zone"] not in wards or row["drop_zone"] not in wards:
        issues.add(f"{event_id}: unknown ward id in zones")
    event_type = row["event_type"]
    if event_type not in METRIC_EVENT.values():
        issues.add(f"{event_id}: ride_hail event_type {event_type!r}")
    if event_type == "completed_ride":
        if row["status"] != "completed":
            issues.add(f"{event_id}: completed_ride status {row['status']!r}")
        if row["cancel_reason"] is not None:
            issues.add(f"{event_id}: completed ride carries cancel_reason")
        fare = row[FARE_BASIS]
        if fare is not None and (
            not isinstance(fare, int) or fare < 0 or fare > 10_000
        ):
            issues.add(f"{event_id}: implausible {FARE_BASIS} {fare!r}")
    elif event_type == "cancelled_ride":
        if row["status"] != "cancelled":
            issues.add(f"{event_id}: cancelled_ride status {row['status']!r}")
        if row["cancel_reason"] != "unknown":
            issues.add(
                f"{event_id}: cancel_reason {row['cancel_reason']!r}; "
                "the source split is zero-filled so it stays unknown"
            )
        if row[FARE_BASIS] is not None:
            issues.add(f"{event_id}: cancelled ride carries a fare")
    elif event_type == "booking_created":
        if row["status"] is not None:
            issues.add(
                f"{event_id}: booking_created is not a ride disposition; "
                f"status must be null, got {row['status']!r}"
            )
        if row[FARE_BASIS] is not None:
            issues.add(f"{event_id}: booking carries a fare")
    if row["fare"] is not None:
        issues.add(
            f"{event_id}: canonical fare must stay null; the synthetic "
            f"fare lives in {FARE_BASIS}"
        )


def _check_metro(
    row: dict[str, Any],
    bundle: ModelBundle,
    issues: _Issues,
    event_id: str,
    day: Any,
) -> None:
    if row["event_type"] != "station_hour":
        issues.add(f"{event_id}: metro event_type {row['event_type']!r}")
    entries = row["entries_count"]
    exits = row["exits_count"]
    if not isinstance(entries, int) or not isinstance(exits, int):
        issues.add(f"{event_id}: metro counts must be integers")
        return
    if entries < 0 or exits < 0:
        issues.add(f"{event_id}: negative metro count")
    if row["count"] != entries + exits:
        issues.add(f"{event_id}: count != entries + exits")
    station = row["pickup_zone"]
    if station not in bundle.metro.active(day):
        issues.add(
            f"{event_id}: station {station!r} is not active on {day}"
        )
    if row["drop_zone"] is not None:
        issues.add(f"{event_id}: station-hour record has one place")
    if row["status"] != "completed":
        issues.add(f"{event_id}: metro status {row['status']!r}")


def _check_bus(
    row: dict[str, Any], cap: float, issues: _Issues, event_id: str
) -> None:
    if row["event_type"] != "scheduled_stop":
        issues.add(f"{event_id}: bus event_type {row['event_type']!r}")
    for name in ("boardings", "alightings", "onboard_leaving"):
        if not isinstance(row[name], int) or row[name] < 0:
            issues.add(f"{event_id}: bad {name} {row[name]!r}")
            return
    if row["onboard_leaving"] > cap:
        issues.add(
            f"{event_id}: onboard {row['onboard_leaving']} exceeds the "
            f"capacity cap {cap}"
        )
    if row["ridership_status"] != "synthetic":
        issues.add(f"{event_id}: ridership must stay synthetic")
    if row["ridership_grade"] != "assumption_driven":
        issues.add(f"{event_id}: ridership grade must stay assumption_driven")
    if row["provenance_status"] != "derived":
        issues.add(
            f"{event_id}: stop-call provenance must stay derived, got "
            f"{row['provenance_status']!r}"
        )
    if row["source"] != "bmtc_gtfs":
        issues.add(f"{event_id}: bus source {row['source']!r}")


def _check_aviation(
    row: dict[str, Any], bundle: ModelBundle, issues: _Issues, event_id: str
) -> None:
    direction = row["direction"]
    if direction not in ("arrival", "departure"):
        issues.add(f"{event_id}: direction {direction!r}")
        return
    if row["event_type"] != direction:
        issues.add(f"{event_id}: event_type != direction")
    airport = bundle.spec.airport_code
    if direction == "arrival":
        if row["drop_zone"] != airport or row["pickup_zone"] is not None:
            issues.add(f"{event_id}: arrival zones must be -> {airport}")
    else:
        if row["pickup_zone"] != airport or row["drop_zone"] is not None:
            issues.add(f"{event_id}: departure zones must be {airport} ->")
    if row["provenance_status"] == "observed_proxy":
        if not row["icao24"]:
            issues.add(f"{event_id}: observed_proxy flight without icao24")
        if row["source"] != "opensky":
            issues.add(f"{event_id}: observed_proxy source {row['source']!r}")
    elif row["provenance_status"] == "synthetic":
        if not row["icao24"] or not str(row["icao24"]).startswith("fs"):
            issues.add(
                f"{event_id}: synthetic icao24 must carry the fs marker"
            )


def check_parents(
    parents: dict[str, dict[tuple[str, str], int]],
    issues: _Issues,
) -> None:
    """Conservation: read-back sums equal the recorded parent totals."""
    for (parent_id, metric), recorded in parents["ridehail"].items():
        actual = parents["ridehail_readback"].get((parent_id, metric), 0)
        if actual != recorded:
            issues.add(
                f"ride_hail {parent_id} {metric}: emitted {actual} rows "
                f"but parent total is {recorded}"
            )
    for parent_id, recorded in parents["entries"].items():
        actual = parents["entries_readback"].get(parent_id, 0)
        if actual != recorded:
            issues.add(
                f"metro {parent_id}: emitted entries {actual} != parent "
                f"{recorded}"
            )
    for parent_id, recorded in parents["exits"].items():
        actual = parents["exits_readback"].get(parent_id, 0)
        if actual != recorded:
            issues.add(
                f"metro {parent_id}: emitted exits {actual} != parent "
                f"{recorded}"
            )
    boardings: dict[str, int] = {}
    alightings: dict[str, int] = {}
    for (parent_id, metric), value in parents["bus_readback"].items():
        if metric == "boardings":
            boardings[parent_id] = value
        elif metric == "alightings":
            alightings[parent_id] = value
    for parent_id, value in boardings.items():
        if alightings.get(parent_id) != value:
            issues.add(
                f"bus {parent_id}: boardings {value} != alightings "
                f"{alightings.get(parent_id)}"
            )
        recorded = parents["bus"].get((parent_id, "boardings"), 0)
        if value != recorded:
            issues.add(
                f"bus {parent_id}: emitted boardings {value} != allocated "
                f"{recorded}"
            )
    observed_parents = {
        parent_id
        for (parent_id, metric), value in parents["aviation"].items()
        if metric == "observed_rows" and value
    }
    for (parent_id, metric), value in parents["aviation_readback"].items():
        if (
            metric == "synthetic_rows"
            and value
            and parent_id in observed_parents
        ):
            issues.add(
                f"aviation {parent_id}: synthetic rows emitted for an "
                "observed window (double count)"
            )


def validate_written(
    parts: list[Any],
    bundle: ModelBundle,
    parents: dict[str, dict[tuple[str, str], int]],
) -> list[str]:
    """Read every written part back and run the full check set."""
    issues = _Issues()
    seen_ids: set[str] = set()
    readback = {
        "ridehail_readback": {},
        "entries_readback": {},
        "exits_readback": {},
        "bus_readback": {},
        "aviation_readback": {},
    }
    modes: set[str] = set()
    total = 0
    for part in parts:
        parquet = pq.ParquetFile(part)
        for batch in parquet.iter_batches(batch_size=50_000):
            rows = batch.to_pylist()
            check_rows(rows, bundle, issues, seen_ids)
            for row in rows:
                modes.add(row["mode"])
                parent_id = row["parent_id"]
                if row["mode"] == "ride_hail":
                    key = (parent_id, _EVENT_METRIC[row["event_type"]])
                    readback["ridehail_readback"][key] = (
                        readback["ridehail_readback"].get(key, 0) + 1
                    )
                elif row["mode"] == "metro":
                    readback["entries_readback"][parent_id] = (
                        readback["entries_readback"].get(parent_id, 0)
                        + int(row["entries_count"])
                    )
                    readback["exits_readback"][parent_id] = (
                        readback["exits_readback"].get(parent_id, 0)
                        + int(row["exits_count"])
                    )
                elif row["mode"] == "bus":
                    key = (parent_id, "boardings")
                    readback["bus_readback"][key] = (
                        readback["bus_readback"].get(key, 0)
                        + int(row["boardings"])
                    )
                    key = (parent_id, "alightings")
                    readback["bus_readback"][key] = (
                        readback["bus_readback"].get(key, 0)
                        + int(row["alightings"])
                    )
                elif row["mode"] == "aviation":
                    key = (
                        parent_id,
                        "synthetic_rows"
                        if row["provenance_status"] == "synthetic"
                        else "observed_rows",
                    )
                    readback["aviation_readback"][key] = (
                        readback["aviation_readback"].get(key, 0) + 1
                    )
            total += len(rows)
    for mode in MODES:
        if mode not in modes:
            issues.add(f"mode {mode} missing from the corpus")
    merged = {**parents, **readback}
    check_parents(merged, issues)
    if total != parents.get("total_rows", total):
        issues.add(
            f"read-back row count {total} != manifest "
            f"{parents.get('total_rows', total)}"
        )
    issues.raise_if_any("corpus validation failed")
    return issues.items
