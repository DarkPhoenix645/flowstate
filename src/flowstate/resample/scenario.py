"""Scenario policy and the one fork.

ADR-0016 and ADR-0017. One command schema. ``scenario_type`` selects
the policy row. Effects touch synthetic rows after the command time.
Historical rows are copied onto the fork and not edited. A flight's
ride-hail lifecycles attach only to flights this fork injects.

``metro_disruption`` scales one station's riders. It then moves a share
of the removed riders to ride-hail and bus. That needs ``pins`` (the
station-to-ward crosswalk and the stops near the station) and the
plan's ``metro_scale``. Without ``pins`` the station is scaled and the
shift is off, and the summary says so.
"""

from __future__ import annotations

import json
import math
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
from pydantic import BaseModel, ConfigDict, Field

from flowstate.resample.bundle import ModelBundle
from flowstate.resample.pins import PinBook
from flowstate.resample.profiles import to_date
from flowstate.resample.unfold import (
    _base_row,
    _ridehail_event,
    largest_remainder,
    rng_for,
)
from flowstate.schema import resolve_vehicle_type

POLICY_PATH = Path(__file__).with_name("scenario_policy.json")
_DURATION = re.compile(
    r"^PT(?:(?P<h>\d+)H)?(?:(?P<m>\d+)M)?(?:(?P<s>\d+)S)?$"
)


class LagProfile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    min_minutes: int
    max_minutes: int


class ScenarioSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    scenario_type: str
    enabled: bool
    affected_modes: list[str]
    time_lag_profile: LagProfile
    intensity_multiplier: float
    shift_fraction: float
    maximum_multiplier: float
    evidence_grade: str
    evidence_window: str | None = None
    notes: str = ""
    rides_per_arrival: int | None = None
    rides_per_departure: int | None = None
    # Share of the removed riders that reaches each mode. The rest is
    # not modelled (walk, own vehicle, adjacent station, trip dropped).
    shift_shares: dict[str, float] | None = None


class ScenarioPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: str
    kia_catchment_wards: list[str]
    notes: str = ""
    multiplier_floor: float = Field(default=0.2, gt=0)
    scenarios: dict[str, ScenarioSpec]


class ScenarioCommand(BaseModel):
    """One ``flowstate.scenario.command.v1`` record."""

    model_config = ConfigDict(extra="forbid")

    mode: str
    scenario_type: str
    location_ref: str
    start: str
    duration: str
    multiplier: float | None = None
    scenario_id: str = Field(min_length=1)


def load_policy(path: Path | None = None) -> ScenarioPolicy:
    payload = json.loads((path or POLICY_PATH).read_text())
    return ScenarioPolicy.model_validate(payload)


def command_json_schema() -> dict[str, Any]:
    schema = ScenarioCommand.model_json_schema()
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    return schema


def parse_duration(text: str) -> timedelta:
    match = _DURATION.fullmatch(text)
    if match is None or not any(match.groups()):
        raise ValueError(f"duration must look like PT1H or PT30M, got {text}")
    hours = int(match.group("h") or 0)
    minutes = int(match.group("m") or 0)
    seconds = int(match.group("s") or 0)
    span = timedelta(hours=hours, minutes=minutes, seconds=seconds)
    if span.total_seconds() <= 0:
        raise ValueError("duration must be positive")
    return span


def command_window(
    command: ScenarioCommand, zone: Any
) -> tuple[datetime, datetime]:
    start = datetime.fromisoformat(command.start)
    if start.tzinfo is None:
        raise ValueError("command start needs a timezone offset")
    start = start.astimezone(zone)
    return start, start + parse_duration(command.duration)


def observed_activity_per_day(
    city_day: pa.Table, window_start: Any, window_end: Any
) -> float:
    """Mean city-day sum of booking + done_ride + cancel_ride.

    This is the amplification denominator. It replaces the completion
    clock of 137319.
    """
    frame = city_day.select(
        ["date", "booking", "done_ride", "cancel_ride"]
    ).to_pydict()
    totals: list[int] = []
    for day, booking, done, cancel in zip(
        frame["date"],
        frame["booking"],
        frame["done_ride"],
        frame["cancel_ride"],
        strict=True,
    ):
        if not window_start <= to_date(str(day)[:10]) <= window_end:
            continue
        totals.append(
            int(booking or 0) + int(done or 0) + int(cancel or 0)
        )
    if not totals:
        raise ValueError("no city-day activity in the window")
    return sum(totals) / len(totals)


def amplification_factor(
    ridehail_rows_per_day: float, observed_per_day: float
) -> float:
    if observed_per_day <= 0:
        raise ValueError("observed activity per day must be positive")
    return ridehail_rows_per_day / observed_per_day


def lifecycles_per_ward(
    injected: int, rides_per_flight: int, factor: float, n_wards: int
) -> float:
    """ADR-0016 expectation. Not an integer event count."""
    if n_wards <= 0:
        raise ValueError("n_wards must be positive")
    return injected * rides_per_flight * factor / n_wards


def lifecycle_total(
    injected: int, rides_per_flight: int, factor: float
) -> int:
    return int(math.floor(injected * rides_per_flight * factor + 0.5))


def applied_multiplier(
    command: ScenarioCommand, spec: ScenarioSpec, floor: float
) -> float:
    raw = (
        command.multiplier
        if command.multiplier is not None
        else spec.intensity_multiplier
    )
    if command.scenario_type == "metro_disruption":
        if raw <= 0 or raw > 1:
            raise ValueError(
                "metro_disruption multiplier must be in (0, 1]"
            )
        return max(raw, floor)
    return min(max(raw, floor), spec.maximum_multiplier)


def emitted_ward_ids(bundle: ModelBundle) -> set[str]:
    wards: set[str] = set()
    for ids, _weights in bundle.ward_shares.values():
        wards.update(ids)
    return wards


def ward_probabilities(
    bundle: ModelBundle, hour: int, widen: float
) -> tuple[list[str], Any]:
    """Hour shares. ``widen`` blends toward uniform outside the window."""
    wards, weights = bundle.ward_shares[hour]
    shares = weights / weights.sum()
    if widen:
        shares = (1.0 - widen) * shares + widen / len(shares)
    return list(wards), shares


def check_commands(
    commands: list[ScenarioCommand],
    policy: ScenarioPolicy,
    bundle: ModelBundle,
) -> None:
    if not commands:
        return
    ids = {command.scenario_id for command in commands}
    if len(ids) > 1:
        raise ValueError(
            "a replay accepts one scenario_id besides null"
        )
    wards = emitted_ward_ids(bundle)
    for command in commands:
        spec = policy.scenarios.get(command.scenario_type)
        if spec is None or not spec.enabled:
            raise ValueError(f"{command.scenario_type} is not enabled")
        if command.mode not in spec.affected_modes:
            raise ValueError(
                f"{command.scenario_type} does not affect {command.mode}"
            )
        command_window(command, bundle.spec.timezone)
        if command.scenario_type.startswith("flight_"):
            if command.location_ref != bundle.spec.airport_code:
                raise ValueError(
                    "flight location_ref must be the bundle airport"
                )
            missing = [
                ward
                for ward in policy.kia_catchment_wards
                if ward not in wards
            ]
            if not policy.kia_catchment_wards or missing:
                raise ValueError(
                    "kia_catchment_wards must be non-empty emitted ward ids"
                    + (f"; unknown {missing}" if missing else "")
                )


def apply_fork(
    baseline: list[dict[str, Any]],
    commands: list[ScenarioCommand],
    bundle: ModelBundle,
    policy: ScenarioPolicy,
    *,
    seed: int,
    factor: float,
    cancel_share: float,
    pins: PinBook | None = None,
    scale: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Full fork stream. Baseline objects are not mutated.

    ``pins`` supplies the station-to-ward crosswalk and the stops near a
    station. ``scale`` is the window plan summary (``metro_scale``). A
    metro disruption needs both to move riders to ride-hail and bus.
    """
    check_commands(commands, policy, bundle)
    if not commands:
        return [dict(row) for row in baseline], {
            "scenario_id": None,
            "injected_flights": 0,
            "lifecycles": 0,
            "shift_applied": False,
        }
    scenario_id = commands[0].scenario_id
    fork = [_retag(row, scenario_id) for row in baseline]
    summary = {
        "scenario_id": scenario_id,
        "injected_flights": 0,
        "lifecycles": 0,
        "shift_applied": False,
        "metro_rows_removed": 0,
        "crosswalk": pins is not None,
        "metro_riders_removed": 0,
        "shift_ride_hail_lifecycles": 0,
        "shift_bus_boardings": 0,
        "shift_note": None,
    }
    ordered = sorted(
        commands, key=lambda item: (item.start, item.scenario_type)
    )
    scale_book = scale or {}
    for command in ordered:
        spec = policy.scenarios[command.scenario_type]
        scale = applied_multiplier(
            command, spec, policy.multiplier_floor
        )
        if command.scenario_type in ("flight_arrival", "flight_departure"):
            added, cycles = _inject_flights(
                fork,
                command,
                spec,
                bundle,
                policy,
                seed=seed,
                factor=factor,
                cancel_share=cancel_share,
                scale=scale,
            )
            summary["injected_flights"] += added
            summary["lifecycles"] += cycles
        elif command.scenario_type == "metro_disruption":
            effect = _disrupt_metro(
                fork,
                command,
                spec,
                bundle,
                scenario_id,
                seed=seed,
                factor=factor,
                cancel_share=cancel_share,
                keep=scale,
                pins=pins,
                scale=scale_book,
            )
            summary["metro_rows_removed"] += effect["rows_removed"]
            summary["metro_riders_removed"] += effect["riders_removed"]
            summary["shift_ride_hail_lifecycles"] += effect["lifecycles"]
            summary["shift_bus_boardings"] += effect["bus_boardings"]
            summary["shift_applied"] = bool(
                summary["shift_ride_hail_lifecycles"]
                or summary["shift_bus_boardings"]
            )
            summary["shift_note"] = effect["note"]
        elif command.scenario_type == "custom_shock":
            fork = _scale_synthetic(fork, command, bundle, scale, scenario_id)
        else:
            raise ValueError(f"unhandled {command.scenario_type}")
    return fork, summary


def _retag(row: dict[str, Any], scenario_id: str) -> dict[str, Any]:
    clone = dict(row)
    clone["scenario_id"] = scenario_id
    clone["trip_id"] = f"{scenario_id}:{row['trip_id']}"
    return clone


def _in_command(
    row: dict[str, Any], start: datetime, end: datetime
) -> bool:
    moment = datetime.fromisoformat(row["timestamp"])
    return start <= moment < end


def _at_location(row: dict[str, Any], mode: str, location_ref: str) -> bool:
    if row["mode"] != mode:
        return False
    if mode == "aviation":
        airport = (
            row["drop_zone"]
            if row.get("direction") == "arrival"
            else row["pickup_zone"]
        )
        return airport == location_ref
    return row["pickup_zone"] == location_ref


def _scale_synthetic(
    rows: list[dict[str, Any]],
    command: ScenarioCommand,
    bundle: ModelBundle,
    scale: float,
    scenario_id: str,
) -> list[dict[str, Any]]:
    start, end = command_window(command, bundle.spec.timezone)
    matched = [
        index
        for index, row in enumerate(rows)
        if row["provenance_status"] == "synthetic"
        and _at_location(row, command.mode, command.location_ref)
        and _in_command(row, start, end)
    ]
    target = int(round(len(matched) * scale))
    ordered = sorted(matched, key=lambda index: rows[index]["trip_id"])
    if target == len(matched):
        return rows
    if target < len(matched):
        drop = set(ordered[target:])
        return [row for index, row in enumerate(rows) if index not in drop]
    additions: list[dict[str, Any]] = []
    for offset in range(target - len(matched)):
        source = rows[ordered[offset % len(ordered)]]
        clone = dict(source)
        clone["trip_id"] = f"{source['trip_id']}:x{offset}"
        clone["scenario_id"] = scenario_id
        clone["evidence_grade"] = "assumption_driven"
        additions.append(clone)
    return rows + additions


def _disrupt_metro(
    rows: list[dict[str, Any]],
    command: ScenarioCommand,
    spec: ScenarioSpec,
    bundle: ModelBundle,
    scenario_id: str,
    *,
    seed: int,
    factor: float,
    cancel_share: float,
    keep: float,
    pins: PinBook | None,
    scale: dict[str, Any],
) -> dict[str, Any]:
    """Suppress one station, then move a share of the riders.

    ``keep`` is the fraction of station-hour riders that remain. The
    removed entries are real riders only after dividing by the day's
    metro thinning (``people`` over the tracker total). ``shift_shares``
    sends a share of them to ride-hail and to bus. Ride-hail
    lifecycles scale with ``factor``, like the flight response. Bus
    boardings scale with the day's bus thinning. The rest is not modelled.
    """
    start, end = command_window(command, bundle.spec.timezone)
    removed_by_hour: dict[tuple[str, int], int] = {}
    kept: list[dict[str, Any]] = []
    out = {
        "rows_removed": 0,
        "riders_removed": 0,
        "lifecycles": 0,
        "bus_boardings": 0,
        "note": None,
    }
    for row in rows:
        hit = (
            row["mode"] == "metro"
            and row["provenance_status"] == "synthetic"
            and row["pickup_zone"] == command.location_ref
            and _in_command(row, start, end)
        )
        if not hit:
            kept.append(row)
            continue
        entries = int(row["entries_count"])
        exits = int(row["exits_count"])
        new_entries = int(round(entries * keep))
        new_exits = int(round(exits * keep))
        removed = entries - new_entries
        day = row["timestamp"][:10]
        hour = int(row["hour"])
        removed_by_hour[(day, hour)] = (
            removed_by_hour.get((day, hour), 0) + removed
        )
        out["riders_removed"] += removed
        if new_entries + new_exits == 0:
            out["rows_removed"] += 1
            continue
        row["entries_count"] = new_entries
        row["exits_count"] = new_exits
        row["count"] = new_entries + new_exits
        row["evidence_grade"] = "assumption_driven"
        kept.append(row)
    rows[:] = kept
    shares = spec.shift_shares or {"ride_hail": spec.shift_fraction}
    to_ride = shares.get("ride_hail", 0.0)
    to_bus = shares.get("bus", 0.0)
    if not removed_by_hour:
        out["note"] = "no station-hour rows in the window"
        return out
    ward = None if pins is None else pins.station_ward(command.location_ref)
    if ward is None:
        out["note"] = "station has no ward: no crosswalk"
        return out
    expected: dict[tuple[str, int], float] = {}
    real_by_hour: dict[tuple[str, int], float] = {}
    for key, removed in removed_by_hour.items():
        book = (scale.get("metro_scale") or {}).get(key[0])
        if not book or not book["real_total"]:
            continue
        thin = book["people"] / book["real_total"]
        real = removed / thin
        real_by_hour[key] = real
        expected[key] = real * to_ride * factor
    if not expected:
        out["note"] = "no metro thinning for the window days"
        return out
    out["lifecycles"] = _shift_ride_hail(
        rows,
        bundle,
        scenario_id,
        command.location_ref,
        ward,
        expected,
        seed=seed,
        cancel_share=cancel_share,
    )
    if to_bus > 0 and pins is not None:
        out["bus_boardings"] = _shift_bus(
            rows,
            bundle,
            command,
            pins,
            real_by_hour,
            to_bus,
        )
    return out


def _shift_ride_hail(
    rows: list[dict[str, Any]],
    bundle: ModelBundle,
    scenario_id: str,
    station: str,
    ward: str,
    expected: dict[tuple[str, int], float],
    *,
    seed: int,
    cancel_share: float,
) -> int:
    total = int(round(sum(expected.values())))
    if total <= 0:
        return 0
    keys = sorted(expected)
    counts = largest_remainder(total, [expected[key] for key in keys])
    vehicle = resolve_vehicle_type(
        "ride_hail", bundle.config.clock_vehicle_type
    )
    rng = rng_for(seed, scenario_id, "metro_shift", station)
    made = 0
    for (day, hour), count in zip(keys, counts, strict=True):
        wards, weights = bundle.ward_shares[hour]
        shares = weights / weights.sum()
        for index in range(count):
            second = int(rng.integers(0, 3600))
            drop = str(rng.choice(wards, p=shares))
            end_metric = (
                "cancel_ride"
                if float(rng.random()) < cancel_share
                else "done_ride"
            )
            for leg, metric in (("booking", "booking"), ("end", end_metric)):
                row = _ridehail_event(
                    bundle,
                    day=day,
                    hour=hour,
                    second=second,
                    seq=made,
                    metric=metric,
                    pickup=ward,
                    drop=drop,
                    is_cab=False,
                    fare=None,
                    vehicle=vehicle,
                    parent_source="sampled",
                    parent_id=f"scenario:{scenario_id}:metro:{station}",
                )
                row["trip_id"] = (
                    f"{scenario_id}:fs-rh-metro-{leg}-{day}-"
                    f"{hour:02d}-{index:05d}"
                )
                row["scenario_id"] = scenario_id
                row["evidence_grade"] = "assumption_driven"
                row["source_ids"] = "scenario_policy"
                row["outside_training_window"] = (
                    "true"
                    if not (
                        bundle.spec.window_start
                        <= to_date(day)
                        <= bundle.spec.window_end
                    )
                    else "false"
                )
                rows.append(row)
        made += count
    return total


def _shift_bus(
    rows: list[dict[str, Any]],
    bundle: ModelBundle,
    command: ScenarioCommand,
    pins: PinBook,
    real_by_hour: dict[tuple[str, int], float],
    to_bus: float,
) -> int:
    """Add boardings on stop calls within 1 km of the station.

    The added riders alight at the last sampled stop call of the same
    trip, so each trip keeps boardings equal to alightings. Onboard
    never passes the capacity cap. When no sampled stop call is near the
    station, nothing is added and the summary count says so.
    """
    stops = set(pins.stops_near_station(command.location_ref))
    if not stops:
        return 0
    assumed = (
        bundle.config.assumed_weekday_boardings
        * bundle.config.metro_substitution_multiplier
    )
    capacity = (
        bundle.config.assumed_bus_capacity
        * bundle.config.assumed_max_load_factor
    )
    trips: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if row["mode"] == "bus":
            trips.setdefault(row["parent_id"], []).append(row)
    for group in trips.values():
        group.sort(key=lambda item: int(item["trip_id"].rsplit("-", 1)[1]))
    day_boardings: dict[str, int] = {}
    for group in trips.values():
        for row in group:
            day = row["timestamp"][:10]
            day_boardings[day] = day_boardings.get(day, 0) + int(
                row["boardings"]
            )
    added = 0
    for (day, hour), real in sorted(real_by_hour.items()):
        thin = day_boardings.get(day, 0) / assumed if assumed else 0.0
        want = int(round(real * to_bus * thin))
        if want <= 0:
            continue
        near = [
            (group, index)
            for group in trips.values()
            for index, row in enumerate(group[:-1])
            if row["pickup_zone"] in stops
            and row["timestamp"][:10] == day
            and int(row["hour"]) == hour
        ]
        if not near:
            continue
        counts = largest_remainder(want, [1.0] * len(near))
        for (group, index), extra in zip(near, counts, strict=True):
            span = group[index:-1]
            peak = max(int(r["onboard_leaving"]) for r in span)
            headroom = int(capacity - peak)
            take = min(extra, headroom)
            if take <= 0:
                continue
            group[index]["boardings"] = int(group[index]["boardings"]) + take
            group[-1]["alightings"] = int(group[-1]["alightings"]) + take
            for row in span:
                row["onboard_leaving"] = int(row["onboard_leaving"]) + take
            for row in (group[index], group[-1], *span):
                row["evidence_grade"] = "assumption_driven"
            added += take
    return added


def _inject_flights(
    fork: list[dict[str, Any]],
    command: ScenarioCommand,
    spec: ScenarioSpec,
    bundle: ModelBundle,
    policy: ScenarioPolicy,
    *,
    seed: int,
    factor: float,
    cancel_share: float,
    scale: float,
) -> tuple[int, int]:
    direction = (
        "arrival" if command.scenario_type == "flight_arrival" else "departure"
    )
    rides_per = (
        spec.rides_per_arrival
        if direction == "arrival"
        else spec.rides_per_departure
    )
    if not rides_per:
        raise ValueError(f"{command.scenario_type} has no yield")
    start, end = command_window(command, bundle.spec.timezone)
    present = [
        row
        for row in fork
        if row["mode"] == "aviation"
        and row.get("direction") == direction
        and _at_location(row, "aviation", command.location_ref)
        and _in_command(row, start, end)
    ]
    if present:
        target = int(round(len(present) * scale))
        injected_n = max(0, target - len(present))
    else:
        hours = max((end - start).total_seconds() / 3600.0, 1.0 / 60.0)
        injected_n = int(round(scale * hours))
    if injected_n <= 0:
        return 0, 0
    flights = _flight_rows(
        bundle,
        command,
        direction,
        injected_n,
        start,
        end,
        seed,
    )
    cycles = lifecycle_total(injected_n, rides_per, factor)
    rides = _lifecycle_rows(
        bundle,
        policy,
        command,
        flights,
        cycles,
        cancel_share,
        seed,
    )
    fork.extend(flights)
    fork.extend(rides)
    return injected_n, cycles


def _flight_rows(
    bundle: ModelBundle,
    command: ScenarioCommand,
    direction: str,
    count: int,
    start: datetime,
    end: datetime,
    seed: int,
) -> list[dict[str, Any]]:
    span = max((end - start).total_seconds(), 1.0)
    airport = bundle.spec.airport_code
    dataset = (
        bundle.spec.opensky_arrivals_id
        if direction == "arrival"
        else bundle.spec.opensky_departures_id
    )
    rows = []
    rng = rng_for(seed, command.scenario_id, "flight", direction)
    for index in range(count):
        moment = start + timedelta(seconds=(index + 0.5) * span / count)
        outside = not (
            bundle.spec.window_start
            <= moment.date()
            <= bundle.spec.window_end
        )
        row = _base_row(
            bundle,
            trip_id=(
                f"{command.scenario_id}:fs-av-{direction}-{index:05d}"
            ),
            mode="aviation",
            event_type=direction,
            hour=moment.hour,
            timestamp=moment.isoformat(),
            pickup_zone=None if direction == "arrival" else airport,
            drop_zone=airport if direction == "arrival" else None,
            status="completed",
            vehicle_type=resolve_vehicle_type("aviation"),
            source="synthetic",
            provenance_status="synthetic",
            evidence_grade="assumption_driven",
            source_ids=[dataset],
            parent_source="sampled",
            parent_id=f"scenario:{command.scenario_id}:injected:{index}",
            outside=outside,
        )
        row["scenario_id"] = command.scenario_id
        row["direction"] = direction
        row["icao24"] = f"fs{int(rng.integers(0, 0xFFFFF)):05x}"
        row["callsign"] = f"SYN{int(rng.integers(1000, 9999))}"
        rows.append(row)
    return rows


def _lifecycle_rows(
    bundle: ModelBundle,
    policy: ScenarioPolicy,
    command: ScenarioCommand,
    flights: list[dict[str, Any]],
    cycles: int,
    cancel_share: float,
    seed: int,
) -> list[dict[str, Any]]:
    if cycles <= 0 or not flights:
        return []
    wards = list(policy.kia_catchment_wards)
    per_ward = largest_remainder(cycles, [1] * len(wards))
    assigned: list[str] = []
    for ward, count in zip(wards, per_ward, strict=True):
        assigned.extend([ward] * count)
    if cancel_share <= 0:
        n_cancel = 0
    elif cancel_share >= 1:
        n_cancel = cycles
    else:
        n_cancel = int(round(cycles * cancel_share))
    n_done = cycles - n_cancel
    lag = policy.scenarios[command.scenario_type].time_lag_profile
    vehicle = resolve_vehicle_type(
        "ride_hail", bundle.config.clock_vehicle_type
    )
    rng = rng_for(seed, command.scenario_id, "lifecycle")
    rows: list[dict[str, Any]] = []
    for index, ward in enumerate(assigned):
        flight = flights[index % len(flights)]
        flight_at = datetime.fromisoformat(flight["timestamp"])
        draw = float(rng.random())
        minutes = lag.min_minutes + draw * (
            lag.max_minutes - lag.min_minutes
        )
        moment = flight_at + timedelta(minutes=minutes)
        metric = "done_ride" if index < n_done else "cancel_ride"
        outside = not (
            bundle.spec.window_start
            <= moment.date()
            <= bundle.spec.window_end
        )
        for leg, leg_metric in (
            ("booking", "booking"),
            ("end", metric),
        ):
            row = _ridehail_event(
                bundle,
                day=moment.date().isoformat(),
                hour=moment.hour,
                second=moment.minute * 60 + moment.second,
                seq=index,
                metric=leg_metric,
                pickup=ward,
                drop=ward,
                is_cab=False,
                fare=None,
                vehicle=vehicle,
                parent_source="sampled",
                parent_id=flight["parent_id"],
            )
            row["trip_id"] = (
                f"{command.scenario_id}:fs-rh-{leg}-{index:05d}"
            )
            row["scenario_id"] = command.scenario_id
            row["evidence_grade"] = "assumption_driven"
            row["outside_training_window"] = (
                "true" if outside else "false"
            )
            row["source_ids"] = "scenario_policy"
            rows.append(row)
    return rows
