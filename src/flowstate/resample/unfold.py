"""Layer 3 event unfolding and the pre-emptive sanity checks.

Every allocator preserves integer parent totals exactly. Every
event carries provenance, evidence grade, bundle version, and seed
id. ``validate_events`` flags events that do not make sense before
anything ships: bad provenance, broken conservation, impossible
hours, inactive stations, capacity violations, double-counted
aviation windows.

No city date or code lives here. The bundle's ``CitySpec`` carries
the timezone, the window, the line-open date, and the airport code.
"""

from __future__ import annotations

import math
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from typing import Any

import numpy as np
import pyarrow as pa

from flowstate.resample.bundle import (
    FARE_BASIS,
    ModelBundle,
    ResampleConfig,
    bus_prior_regime,
)
from flowstate.resample.clocks import aviation_mean_for
from flowstate.resample.nb2 import nb_quantile
from flowstate.resample.profiles import is_weekend, to_date
from flowstate.schema import (
    resolve_vehicle_type,
)

PROVENANCE_STATUSES = ("observed", "observed_proxy", "derived", "synthetic")
EVIDENCE_GRADES = (
    "aggregate_calibrated",
    "shape_prior",
    "assumption_driven",
)
PARENT_SOURCES = ("observed", "sampled", "scaled", "subset", "assumed")
RIDE_METRICS = ("booking", "done_ride", "cancel_ride")
METRIC_EVENT = {
    "booking": "booking_created",
    "done_ride": "completed_ride",
    "cancel_ride": "cancelled_ride",
}
BATCH_ROWS = 20_000


def rng_for(seed: int, *parts: object) -> Any:
    """A stable child RNG for one run/bucket pair.

    numpy refuses mixed string/int seed sequences, so the tag parts
    are hashed together with the run seed (plan §9.4: seeds are
    explicit and stable per run, scenario, and bucket). The same
    seed and bucket always give the same stream, whatever the order
    of generation around it.
    """
    import hashlib
    import json

    payload = json.dumps([seed, *parts], default=str, sort_keys=True)
    digest = hashlib.sha256(payload.encode()).digest()
    return np.random.default_rng(
        np.frombuffer(digest, dtype=np.uint32)
    )

# Extra columns after the canonical ride columns. staged.py keeps
# extras as strings or numbers, so booleans never appear here.
EXTRA_COLUMNS: tuple[str, ...] = (
    "event_type",
    "hour",
    "count",
    "entries_count",
    "exits_count",
    "boardings",
    "alightings",
    "onboard_leaving",
    "ridership_status",
    "ridership_grade",
    FARE_BASIS,
    "provenance_status",
    "evidence_grade",
    "source_ids",
    "parent_source",
    "parent_id",
    "model_bundle_version",
    "seed_id",
    "outside_training_window",
    "scenario_id",
    "direction",
    "icao24",
    "callsign",
    "station_label",
)


@dataclass
class ParentTotals:
    """Running parent totals for the conservation check."""

    entries: dict[str, int] = field(default_factory=dict)
    exits: dict[str, int] = field(default_factory=dict)
    ridehail: dict[tuple[str, str], int] = field(default_factory=dict)
    bus: dict[tuple[str, str], int] = field(default_factory=dict)
    aviation: dict[tuple[str, str], int] = field(default_factory=dict)

    def add_entries(self, parent_id: str, value: int) -> None:
        self.entries[parent_id] = self.entries.get(parent_id, 0) + value

    def add_exits(self, parent_id: str, value: int) -> None:
        self.exits[parent_id] = self.exits.get(parent_id, 0) + value

    def add_ridehail(self, parent_id: str, metric: str, value: int) -> None:
        key = (parent_id, metric)
        self.ridehail[key] = self.ridehail.get(key, 0) + value

    def add_bus(self, trip_key: str, metric: str, value: int) -> None:
        key = (trip_key, metric)
        self.bus[key] = self.bus.get(key, 0) + value

    def add_aviation(
        self, parent_id: str, kind: str, value: int
    ) -> None:
        key = (parent_id, kind)
        self.aviation[key] = self.aviation.get(key, 0) + value


def largest_remainder(total: int, weights: Any) -> list[int]:
    """Integer split of ``total`` by ``weights``. Exact sum."""
    array = np.asarray(weights, dtype=float)
    if total < 0:
        raise ValueError("total must be >= 0")
    if array.sum() <= 0:
        raise ValueError("weights have no mass")
    shares = array / array.sum()
    floors = np.floor(shares * total).astype(np.int64)
    remainder = int(total - floors.sum())
    fractions = shares * total - floors
    order = sorted(range(len(floors)), key=lambda i: (-fractions[i], i))
    for index in order[:remainder]:
        floors[index] += 1
    return [int(v) for v in floors]


def allocate_days(
    total: int, days: list[str], weights: Any
) -> list[tuple[str, int]]:
    """Split ``total`` across ``days``. Days with budget 0 are dropped."""
    if not days:
        return []
    array = np.asarray(weights, dtype=float)
    if len(array) != len(days):
        raise ValueError("days and weights length mismatch")
    split = largest_remainder(total, array)
    return [
        (day, count)
        for day, count in zip(days, split, strict=True)
        if count > 0
    ]


def weighted_without_replacement(
    rng: np.random.Generator, weights: Any, count: int
) -> np.ndarray:
    """Distinct indices drawn with probability proportional to weight."""
    array = np.asarray(weights, dtype=float)
    size = len(array)
    if size == 0 or count <= 0:
        return np.zeros(0, dtype=np.int64)
    idx = np.flatnonzero(array > 0)
    if len(idx) == 0:
        k = min(int(count), size)
        return rng.choice(size, size=k, replace=False).astype(np.int64)
    k = min(int(count), len(idx))
    p = array[idx]
    p = p / p.sum()
    picked = rng.choice(idx, size=k, replace=False, p=p)
    return np.asarray(picked, dtype=np.int64)


def dm_allocate(
    rng: np.random.Generator, total: int, weights: Any, kappa: float
) -> np.ndarray:
    """Dirichlet-multinomial allocation. Children sum to ``total``."""
    if total <= 0:
        return np.zeros(len(np.asarray(weights).ravel()), dtype=np.int64)
    profile = np.asarray(weights, dtype=float)
    conc = np.maximum(profile / max(profile.sum(), 1e-12) * kappa, 1e-9)
    p = rng.dirichlet(conc)
    return rng.multinomial(int(total), p)


def _base_row(
    bundle: ModelBundle,
    *,
    trip_id: str,
    mode: str,
    event_type: str,
    hour: int,
    timestamp: str,
    pickup_zone: str | None,
    drop_zone: str | None,
    status: str | None,
    vehicle_type: str,
    source: str,
    provenance_status: str,
    evidence_grade: str,
    source_ids: list[str],
    parent_source: str,
    parent_id: str,
    count: int = 1,
    outside: bool = False,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "trip_id": trip_id,
        "city": bundle.spec.id,
        "mode": mode,
        "vehicle_type": vehicle_type,
        "timestamp": timestamp,
        "pickup_zone": pickup_zone,
        "drop_zone": drop_zone,
        "status": status,
        "fare": None,
        "cancel_reason": None,
        "source": source,
        "event_type": event_type,
        "hour": hour,
        "count": count,
        "entries_count": None,
        "exits_count": None,
        "boardings": None,
        "alightings": None,
        "onboard_leaving": None,
        "ridership_status": None,
        "ridership_grade": None,
        FARE_BASIS: None,
        "provenance_status": provenance_status,
        "evidence_grade": evidence_grade,
        "source_ids": ",".join(source_ids),
        "parent_source": parent_source,
        "parent_id": parent_id,
        "model_bundle_version": bundle.version,
        "seed_id": bundle.seed_id,
        "outside_training_window": "true" if outside else "false",
        "scenario_id": None,
        "direction": None,
        "icao24": None,
        "callsign": None,
        "station_label": None,
    }
    return row


def _timestamp(bundle: ModelBundle, day: str, hour: int, seconds: int) -> str:
    moment = datetime.combine(
        to_date(day), time(hour=0), tzinfo=bundle.spec.timezone
    ) + timedelta(hours=hour, seconds=int(seconds))
    return moment.isoformat()


def ridehail_day_events(
    bundle: ModelBundle,
    rng: np.random.Generator,
    day: str,
    metrics: dict[str, int],
    earning_proxy: float | None,
    parents: ParentTotals,
    *,
    parent_source: str = "observed",
    partial_rows: int | None = None,
    widen: float = 0.0,
) -> Iterator[list[dict[str, Any]]]:
    """One ride-hail day as activity events, hour-mixed by the prior.

    ``metrics`` are booking / done_ride / cancel_ride day totals.
    A partial (budget-limited) day splits ``partial_rows`` across
    the metrics by their day shares; conservation still holds on
    the emitted totals.
    """
    if partial_rows is not None and partial_rows >= 0:
        metric_values = largest_remainder(
            partial_rows,
            [metrics.get(m) or 0 for m in RIDE_METRICS],
        )
        metrics = dict(zip(RIDE_METRICS, metric_values, strict=True))
    vehicle = resolve_vehicle_type(
        "ride_hail", bundle.config.clock_vehicle_type
    )
    parent_id = f"namma_yatri_city_day:{day}"
    profile = bundle.hour_profile.weights(is_weekend(day))
    kappa = bundle.hour_profile.kappa
    fare_rate = None
    if earning_proxy is not None and earning_proxy > 0:
        fare_rate = math.log(earning_proxy)
    batch: list[dict[str, Any]] = []
    for metric in RIDE_METRICS:
        total = int(metrics.get(metric) or 0)
        if total <= 0:
            continue
        parents.add_ridehail(parent_id, metric, total)
        hours = dm_allocate(rng, total, profile, kappa)
        seq_base = 0
        for hour in range(24):
            per_hour = int(hours[hour])
            if per_hour <= 0:
                continue
            wards, weights = bundle.ward_shares[hour]
            shares = weights / weights.sum()
            if widen:
                shares = (1.0 - widen) * shares + widen / len(shares)
            seconds = rng.integers(0, 3600, size=per_hour)
            pickups = rng.choice(wards, size=per_hour, p=shares)
            drops = rng.choice(wards, size=per_hour, p=shares)
            cabs = (
                rng.random(per_hour) < bundle.config.cab_share
                if metric == "done_ride"
                else np.zeros(per_hour, dtype=bool)
            )
            fares = (
                rng.lognormal(fare_rate, bundle.config.fare_sigma, per_hour)
                if fare_rate is not None and metric == "done_ride"
                else None
            )
            for index in range(per_hour):
                batch.append(
                    _ridehail_event(
                        bundle,
                        day=day,
                        hour=hour,
                        second=int(seconds[index]),
                        seq=seq_base + index,
                        metric=metric,
                        pickup=str(pickups[index]),
                        drop=str(drops[index]),
                        is_cab=bool(cabs[index]),
                        fare=(
                            int(round(fares[index]))
                            if fares is not None
                            else None
                        ),
                        vehicle=vehicle,
                        parent_source=parent_source,
                        parent_id=parent_id,
                    )
                )
                if len(batch) >= BATCH_ROWS:
                    yield batch
                    batch = []
            seq_base += per_hour
    if batch:
        yield batch


def _ridehail_event(
    bundle: ModelBundle,
    *,
    day: str,
    hour: int,
    second: int,
    seq: int,
    metric: str,
    pickup: str,
    drop: str,
    is_cab: bool,
    fare: int | None,
    vehicle: str,
    parent_source: str,
    parent_id: str,
) -> dict[str, Any]:
    timestamp = _timestamp(bundle, day, hour, second)
    status = None
    cancel_reason = None
    if metric == "done_ride":
        status = "completed"
    elif metric == "cancel_ride":
        status = "cancelled"
        cancel_reason = "unknown"
    row = _base_row(
        bundle,
        trip_id=f"fs-rh-{day}-{metric}-{seq:09d}",
        mode="ride_hail",
        event_type=METRIC_EVENT[metric],
        hour=hour,
        timestamp=timestamp,
        pickup_zone=pickup,
        drop_zone=drop,
        status=status,
        vehicle_type="cab" if is_cab else vehicle,
        source="synthetic",
        provenance_status="synthetic",
        evidence_grade="aggregate_calibrated",
        source_ids=["namma_yatri_city_day", "namma_yatri_city_hour"],
        parent_source=parent_source,
        parent_id=parent_id,
    )
    row["cancel_reason"] = cancel_reason
    row[FARE_BASIS] = fare
    return row


def metro_day_events(
    bundle: ModelBundle,
    rng: np.random.Generator,
    day: str,
    total: int,
    parents: ParentTotals,
    *,
    parent_source: str = "observed",
    partial_records: int | None = None,
) -> Iterator[list[dict[str, Any]]]:
    """One metro day as station-hour records.

    A ride is one gate entry and one gate exit, so the observed daily
    total allocates once as entries and once as exits. Station-hour
    weight is station share x hour shape; the station universe is
    the one active on that date. Records with no activity are not
    emitted.
    """
    moment = to_date(day)
    active = bundle.metro.active(moment)
    if not active or total <= 0:
        return
    regime = bundle.metro.regime(moment)
    share_values = np.asarray(
        [bundle.metro.shares[regime].get(station, 0.0) for station in active]
    )
    entry_shape = np.asarray(bundle.metro.hour_entry.weights(is_weekend(day)))
    exit_shape = np.asarray(bundle.metro.hour_exit.weights(is_weekend(day)))
    id_to_label = {}
    for label, station_id in bundle.metro.label_to_id.items():
        id_to_label.setdefault(station_id, label)
    parent_id = f"namma_metro_daily:{day}"
    if partial_records is not None:
        rows = _pack_metro_records(
            bundle,
            rng,
            day=day,
            active=active,
            share_values=share_values,
            entry_shape=entry_shape,
            exit_shape=exit_shape,
            people=total,
            n_records=int(partial_records),
            parent_source=parent_source,
            id_to_label=id_to_label,
        )
    else:
        grid_entry = (share_values[:, None] * entry_shape[None, :]).ravel()
        grid_exit = (share_values[:, None] * exit_shape[None, :]).ravel()
        entries = dm_allocate(
            rng, total, grid_entry, bundle.metro.hour_entry.kappa
        ).reshape(len(active), 24)
        exits = dm_allocate(
            rng, total, grid_exit, bundle.metro.hour_exit.kappa
        ).reshape(len(active), 24)
        rows = []
        for station_index, station in enumerate(active):
            for hour in range(24):
                entries_count = int(entries[station_index, hour])
                exits_count = int(exits[station_index, hour])
                if entries_count + exits_count <= 0:
                    continue
                rows.append(
                    _metro_record(
                        bundle,
                        day=day,
                        hour=hour,
                        station_id=station,
                        station_label=id_to_label.get(station, station),
                        entries_count=entries_count,
                        exits_count=exits_count,
                        parent_source=parent_source,
                    )
                )
    for row in rows:
        parents.add_entries(parent_id, int(row["entries_count"]))
        parents.add_exits(parent_id, int(row["exits_count"]))
    if rows:
        yield rows


def _pack_metro_records(
    bundle: ModelBundle,
    rng: np.random.Generator,
    *,
    day: str,
    active: tuple[str, ...],
    share_values: np.ndarray,
    entry_shape: np.ndarray,
    exit_shape: np.ndarray,
    people: int,
    n_records: int,
    parent_source: str,
    id_to_label: dict[str, str],
) -> list[dict[str, Any]]:
    """Put ``people`` entries and exits on ``n_records`` station-hours."""
    if n_records <= 0 or people <= 0 or not active:
        return []
    grid_entry = (share_values[:, None] * entry_shape[None, :]).ravel()
    grid_exit = (share_values[:, None] * exit_shape[None, :]).ravel()
    pick_w = grid_entry + grid_exit
    picked = weighted_without_replacement(rng, pick_w, n_records)
    if len(picked) == 0:
        return []
    entry_w = np.maximum(grid_entry[picked], 1e-12)
    exit_w = np.maximum(grid_exit[picked], 1e-12)
    entries = largest_remainder(int(people), entry_w)
    exits = largest_remainder(int(people), exit_w)
    rows = []
    n_hours = 24
    for cell, entries_count, exits_count in zip(
        picked, entries, exits, strict=True
    ):
        if entries_count + exits_count <= 0:
            continue
        station_index, hour = divmod(int(cell), n_hours)
        station = active[station_index]
        rows.append(
            _metro_record(
                bundle,
                day=day,
                hour=hour,
                station_id=station,
                station_label=id_to_label.get(station, station),
                entries_count=int(entries_count),
                exits_count=int(exits_count),
                parent_source=parent_source,
            )
        )
    return rows


def _metro_record(
    bundle: ModelBundle,
    *,
    day: str,
    hour: int,
    station_id: str,
    station_label: str,
    entries_count: int,
    exits_count: int,
    parent_source: str,
) -> dict[str, Any]:
    row = _base_row(
        bundle,
        trip_id=f"fs-metro-{day}-{station_id}-{hour:02d}",
        mode="metro",
        event_type="station_hour",
        hour=hour,
        timestamp=_timestamp(bundle, day, hour, 0),
        pickup_zone=station_id,
        drop_zone=None,
        status="completed",
        vehicle_type=resolve_vehicle_type("metro"),
        source="synthetic",
        provenance_status="synthetic",
        evidence_grade="aggregate_calibrated",
        source_ids=["namma_metro_daily", "bmrcl_rti"],
        parent_source=parent_source,
        parent_id=f"namma_metro_daily:{day}",
        count=entries_count + exits_count,
    )
    row["entries_count"] = entries_count
    row["exits_count"] = exits_count
    row["station_label"] = station_label
    return row


def bus_events(
    bundle: ModelBundle,
    rng: np.random.Generator,
    stop_times: pa.Table,
    *,
    feed_date: str,
    budget_rows: int,
    parents: ParentTotals,
    id_prefix: str = "",
) -> tuple[Iterator[list[dict[str, Any]]], dict[str, Any]]:
    """Scheduled stop calls with synthetic ridership (plan §7.4).

    ``feed_date`` is the service day the stop calls are stamped on.
    Amplify passes the GTFS feed date. The producer passes the replay
    day (ADR-0017). ``id_prefix`` keeps the event id and the parent
    key unique when one trip is drawn on several days.

    Whole trips are sampled so per-trip conservation stays
    checkable. Boardings concentrate on early stops, alightings on
    late stops, and onboard leaving never exceeds the capacity cap.
    The ridership numbers are the #40 assumption set (ADR-0012),
    not BMTC measurements. The summary carries the ``prior`` audit
    block: the env values in force, the evidence class, and the
    target year or regime.
    """
    from flowstate.stage.bus import gtfs_seconds

    config = bundle.config
    if config.bus_hour_profile != "metro_ridehail_blend":
        raise ValueError(
            "unknown FLOWSTATE_BUS_HOUR_PROFILE "
            f"{config.bus_hour_profile!r}: v1 supports only "
            "metro_ridehail_blend until a versioned 24-hour BMTC "
            "table exists"
        )
    frame = stop_times.to_pandas()
    frame["trip_id"] = frame["trip_id"].astype(str)
    frame["stop_id"] = frame["stop_id"].astype(str)
    frame = frame.sort_values(["trip_id", "stop_sequence"])
    stops_per_trip = frame.groupby("trip_id", sort=False).size()
    trip_ids = list(stops_per_trip.index)
    first_stops = frame.groupby("trip_id", sort=False).first()
    daytype_weekend = is_weekend(feed_date)
    metro_shape = np.asarray(
        bundle.metro.hour_exit.weights(daytype_weekend)
    )
    ride_shape = np.asarray(
        bundle.hour_profile.weights(daytype_weekend)
    )
    blend = (
        config.bus_hour_weight_metro * metro_shape
        + config.bus_hour_weight_ridehail * ride_shape
    )
    if blend.sum() <= 0:
        raise ValueError("bus hour blend has no mass")
    blend = blend / blend.sum()
    trip_weight: dict[str, float] = {}
    for trip_id in trip_ids:
        clock = str(first_stops.loc[trip_id, "arrival_time"])
        seconds = gtfs_seconds(clock)
        hour = 0 if seconds is None else (seconds // 3600) % 24
        trip_weight[trip_id] = float(blend[hour])
    total_weight = sum(trip_weight.values())
    if total_weight <= 0:
        raise ValueError("bus trips have no hour weight")

    sizes = np.asarray(
        [int(stops_per_trip.loc[trip_id]) for trip_id in trip_ids],
        dtype=np.int64,
    )
    trip_weights = np.asarray(
        [trip_weight[trip_id] for trip_id in trip_ids], dtype=float
    )
    remaining = np.ones(len(trip_ids), dtype=bool)
    sampled: list[str] = []
    used = 0
    while used < budget_rows:
        room = budget_rows - used
        fit = remaining.copy()
        if sampled:
            fit &= sizes <= room
        idx = np.flatnonzero(fit)
        if len(idx) == 0:
            break
        local = weighted_without_replacement(rng, trip_weights[idx], 1)
        if len(local) == 0:
            break
        trip_index = int(idx[int(local[0])])
        sampled.append(trip_ids[trip_index])
        used += int(sizes[trip_index])
        remaining[trip_index] = False
    if not sampled:
        sampled = [trip_ids[int(rng.integers(0, len(trip_ids)))]]
        used = int(stops_per_trip.loc[sampled[0]])
    sampled_weight = sum(trip_weight[t] for t in sampled)
    boardings_total = largest_remainder(
        int(
            round(
                config.assumed_weekday_boardings
                * config.metro_substitution_multiplier
                * sampled_weight
                / total_weight
            )
        ),
        [trip_weight[t] for t in sampled],
    )
    capacity = (
        config.assumed_bus_capacity * config.assumed_max_load_factor
    )
    capped_trips = 0
    displaced = 0
    summary: dict[str, Any] = {
        "feed_date": feed_date,
        "sampled_trips": len(sampled),
        "stop_calls": used,
        "budget": budget_rows,
        "capacity_cap": capacity,
        "boardings_allocated": sum(boardings_total),
    }
    rows_by_trip: dict[str, list[dict[str, Any]]] = {}
    sampled_set = set(sampled)
    by_trip = {
        trip: group
        for trip, group in frame.groupby("trip_id", sort=False)
        if trip in sampled_set
    }
    for trip_id, trip_boardings in zip(sampled, boardings_total, strict=True):
        group = by_trip[trip_id]
        n = len(group)
        peak_fraction = _peak_fraction(n)
        cap_for_trip = int(math.floor(capacity / max(peak_fraction, 1e-9)))
        if trip_boardings > cap_for_trip:
            displaced += trip_boardings - cap_for_trip
            trip_boardings = cap_for_trip
            capped_trips += 1
        rows, cap_displaced = _bus_trip_rows(
            bundle,
            rng,
            trip_id=trip_id,
            group=group,
            feed_date=feed_date,
            boardings=trip_boardings,
            capacity=capacity,
            parents=parents,
            id_prefix=id_prefix,
        )
        displaced += cap_displaced
        if cap_displaced:
            capped_trips += 1
        rows_by_trip[trip_id] = rows
    summary["capped_trips"] = capped_trips
    summary["boardings_displaced_by_cap"] = displaced
    summary["boardings_emitted"] = sum(
        parents.bus.get((f"bmtc_gtfs:{id_prefix}{t}", "boardings"), 0)
        for t in sampled
    )
    summary["prior"] = _bus_prior_audit(
        config, rows_by_trip, sampled
    )

    def batches() -> Iterator[list[dict[str, Any]]]:
        batch: list[dict[str, Any]] = []
        for trip_id in sampled:
            batch.extend(rows_by_trip[trip_id])
            while len(batch) >= BATCH_ROWS:
                yield batch[:BATCH_ROWS]
                batch = batch[BATCH_ROWS:]
        if batch:
            yield batch

    return batches(), summary


def _bus_prior_audit(
    config: ResampleConfig,
    rows_by_trip: dict[str, list[dict[str, Any]]],
    sampled: list[str],
) -> dict[str, Any]:
    """The #40 audit line: env values, evidence class, regime.

    The combined peak windows (08:00-11:00 and 17:00-20:00) are
    compared against the published 47-51% band. The blend profile
    is a fallback, so an off-band share is a warning here, not a
    build failure. A versioned BMTC curve mode would enforce the
    band; none exists yet.
    """
    by_hour = [0] * 24
    total = 0
    for trip_id in sampled:
        for row in rows_by_trip[trip_id]:
            count = int(row["boardings"])
            by_hour[int(row["hour"])] += count
            total += count
    peak = sum(by_hour[8:11]) + sum(by_hour[17:20])
    share = peak / total if total else 0.0
    audit: dict[str, Any] = {
        "env": {
            "FLOWSTATE_ASSUMED_WEEKDAY_BOARDINGS": (
                config.assumed_weekday_boardings
            ),
            "FLOWSTATE_ASSUMED_BUS_CAPACITY": (
                config.assumed_bus_capacity
            ),
            "FLOWSTATE_ASSUMED_MAX_LOAD_FACTOR": (
                config.assumed_max_load_factor
            ),
            "FLOWSTATE_METRO_SUBSTITUTION_MULTIPLIER": (
                config.metro_substitution_multiplier
            ),
            "FLOWSTATE_BUS_HOUR_PROFILE": config.bus_hour_profile,
            "FLOWSTATE_BUS_PEAK_SHARE_LOW": config.bus_peak_share_low,
            "FLOWSTATE_BUS_PEAK_SHARE_HIGH": config.bus_peak_share_high,
            "FLOWSTATE_BUS_TRIP_LENGTH_KM_LOW": (
                config.bus_trip_length_km_low
            ),
            "FLOWSTATE_BUS_TRIP_LENGTH_KM_HIGH": (
                config.bus_trip_length_km_high
            ),
        },
        "evidence_class": "assumption_driven",
        "target_regime": bus_prior_regime(
            config.assumed_weekday_boardings
        ),
        "peak_window_share": share,
        "trip_length_soft_target_km": [
            config.bus_trip_length_km_low,
            config.bus_trip_length_km_high,
        ],
        "note": (
            "Boardings are synthetic assumption-driven values, not "
            "BMTC ridership. Do not add them to observed metro or "
            "ride-hail totals."
        ),
    }
    within = (
        config.bus_peak_share_low <= share <= config.bus_peak_share_high
    )
    audit["peak_share_within_band"] = within
    if not within:
        audit["peak_share_warning"] = (
            f"generated combined-peak share {share:.3f} falls outside "
            f"the published band [{config.bus_peak_share_low}, "
            f"{config.bus_peak_share_high}] under "
            f"{config.bus_hour_profile}; the published band is a "
            "check for a versioned BMTC curve, not this fallback "
            "blend"
        )
    return audit


def _peak_fraction(n: int) -> float:
    """Peak of the onboard shape i(n-i) over the trip's stops."""
    if n <= 1:
        return 1.0
    peak = max(i * (n - i) for i in range(1, n))
    norm = n * (n + 1) / 2.0
    return peak / norm


def _bus_trip_rows(
    bundle: ModelBundle,
    rng: np.random.Generator,
    *,
    trip_id: str,
    group: Any,
    feed_date: str,
    boardings: int,
    capacity: float,
    parents: ParentTotals,
    id_prefix: str = "",
) -> tuple[list[dict[str, Any]], int]:
    """Rows plus the count of boardings displaced by the cap.

    Passengers board on early stops and alight on later stops. The
    capacity cap is enforced exactly, not in expectation: after
    sampling, peak-spanning passengers are dropped one at a time
    until onboard_leaving fits, and each drop is counted as
    displaced demand.
    """
    n = len(group)
    board_weights = [n - i for i in range(n)]
    alight_weights = [i + 1 for i in range(n)]
    pairs: list[tuple[int, int]] = []
    for _ in range(boardings):
        board_index = _weighted_index(rng, board_weights)
        allowed = alight_weights[board_index + 1 :]
        if sum(allowed) <= 0:
            board_index = max(0, n - 2)
            allowed = alight_weights[board_index + 1 :]
        alight_index = board_index + 1 + _weighted_index(rng, allowed)
        pairs.append((board_index, alight_index))
    boards, alights, onboard_values = _stop_counts(pairs, n)
    displaced = 0
    while max(onboard_values, default=0) > capacity:
        peak_stop = onboard_values.index(max(onboard_values))
        for position, (board_index, alight_index) in enumerate(pairs):
            if board_index <= peak_stop < alight_index:
                del pairs[position]
                displaced += 1
                break
        else:  # pragma: no cover - peak > 0 implies a spanning pair
            break
        boards, alights, onboard_values = _stop_counts(pairs, n)
    parent_key = f"bmtc_gtfs:{id_prefix}{trip_id}"
    parents.add_bus(parent_key, "boardings", sum(boards))
    parents.add_bus(parent_key, "alightings", sum(alights))
    service_day = to_date(feed_date)
    outside = not (
        bundle.spec.window_start <= service_day <= bundle.spec.window_end
    )
    rows = []
    for position, (_, stop) in enumerate(group.iterrows()):
        clock = str(stop["arrival_time"])
        seconds = _gtfs_seconds_or_zero(clock)
        moment = (
            datetime.combine(
                to_date(feed_date), time(hour=0), tzinfo=bundle.spec.timezone
            )
            + timedelta(seconds=seconds)
        )
        row = _base_row(
            bundle,
            trip_id=(
                f"bmtc-{id_prefix}{trip_id}-"
                f"{int(stop['stop_sequence']):05d}"
            ),
            mode="bus",
            event_type="scheduled_stop",
            hour=moment.hour,
            timestamp=moment.isoformat(),
            pickup_zone=str(stop["stop_id"]),
            drop_zone=None,
            status="completed",
            vehicle_type=resolve_vehicle_type("bus"),
            source="bmtc_gtfs",
            provenance_status="derived",
            evidence_grade="shape_prior",
            source_ids=["bmtc_gtfs"],
            parent_source="assumed",
            parent_id=parent_key,
            outside=outside,
        )
        row["boardings"] = boards[position]
        row["alightings"] = alights[position]
        row["onboard_leaving"] = onboard_values[position]
        row["ridership_status"] = "synthetic"
        row["ridership_grade"] = "assumption_driven"
        rows.append(row)
    return rows, displaced


def _stop_counts(
    pairs: list[tuple[int, int]], n: int
) -> tuple[list[int], list[int], list[int]]:
    boards = [0] * n
    alights = [0] * n
    for board_index, alight_index in pairs:
        boards[board_index] += 1
        alights[alight_index] += 1
    onboard_values = []
    onboard = 0
    for stop in range(n):
        onboard += boards[stop] - alights[stop]
        onboard_values.append(onboard)
    return boards, alights, onboard_values


def _weighted_index(rng: np.random.Generator, weights: list[int]) -> int:
    total = sum(weights)
    if total <= 0:
        return len(weights) - 1
    pick = rng.random() * total
    for index, weight in enumerate(weights):
        pick -= weight
        if pick <= 0:
            return index
    return len(weights) - 1


def _gtfs_seconds_or_zero(clock: str) -> int:
    from flowstate.stage.bus import gtfs_seconds

    seconds = gtfs_seconds(clock)
    return seconds if seconds is not None else 0


# Half the median healthy day is the coverage gate. The checked
# 2024-10-26 departure window is about 11% of a healthy day, so it
# fails. A day near the median passes. Failed and quota windows
# fail even when they contain rows.
SPARSE_FRACTION = 0.5
FAILED_QUERY_STATUS = frozenset({"failed", "quota"})


def _sparse_direction_keys(
    observed: dict[tuple[str, str], list[tuple[int, str | None, str | None]]],
    skip: set[tuple[str, str]],
) -> set[tuple[str, str]]:
    by_direction: dict[str, list[tuple[tuple[str, str], int]]] = {}
    for key, events in observed.items():
        if key in skip or not events:
            continue
        by_direction.setdefault(key[1], []).append((key, len(events)))
    sparse: set[tuple[str, str]] = set()
    for pairs in by_direction.values():
        median = float(np.median([count for _, count in pairs]))
        for key, count in pairs:
            if median > 0 and count < SPARSE_FRACTION * median:
                sparse.add(key)
    return sparse


def aviation_events(
    bundle: ModelBundle,
    rng: np.random.Generator,
    flight_events: pa.Table,
    *,
    day_pool: list[str],
    budget_rows: int,
    parents: ParentTotals,
) -> tuple[Iterator[list[dict[str, Any]]], dict[str, Any]]:
    """Observed-proxy flights inside healthy fetched windows.

    A missing day/direction is synthetic. A failed, quota, or sparse
    window is synthetic too: its thin rows are not emitted as
    ``observed_proxy``. A day/direction is never emitted both ways.
    """
    columns = [
        "direction",
        "local_date",
        "event_time_unix",
        "icao24",
        "callsign",
    ]
    has_status = "query_status" in flight_events.column_names
    if has_status:
        columns.append("query_status")
    frame = flight_events.select(columns).to_pydict()
    observed: dict[
        tuple[str, str], list[tuple[int, str | None, str | None]]
    ] = {}
    day_counts: dict[str, int] = {}
    failed: set[tuple[str, str]] = set()
    statuses = (
        frame["query_status"]
        if has_status
        else [None] * len(frame["direction"])
    )
    for direction, day, unix, icao, callsign, status in zip(
        frame["direction"],
        frame["local_date"],
        frame["event_time_unix"],
        frame["icao24"],
        frame["callsign"],
        statuses,
        strict=True,
    ):
        if not day:
            continue
        key = (str(day)[:10], str(direction))
        observed.setdefault(key, [])
        observed[key].append((int(unix or 0), icao, callsign))
        day_counts[key[0]] = day_counts.get(key[0], 0) + 1
        if status in FAILED_QUERY_STATUS:
            failed.add(key)
    pool = [day for day in day_pool if day in day_counts]
    if not pool:
        raise ValueError("aviation day pool has no observed days")
    weights = [float(day_counts.get(day, 0)) for day in pool]
    assigned = allocate_days(budget_rows, pool, weights)
    span = bundle.span_days()
    rows: list[dict[str, Any]] = []
    synthetic_days: list[str] = []
    gated = set(failed)
    gated.update(_sparse_direction_keys(observed, failed))
    days = [day for day, _ in assigned]
    for day, need in assigned:
        arr = list(observed.get((day, "arrival"), []))
        dep = list(observed.get((day, "departure"), []))
        n_arr, n_dep = largest_remainder(
            need, [max(len(arr), 1e-9), max(len(dep), 1e-9)]
        )
        for direction, events, take in (
            ("arrival", arr, n_arr),
            ("departure", dep, n_dep),
        ):
            if take <= 0:
                continue
            if (day, direction) in gated:
                events = []
            if events:
                if len(events) > take:
                    picked = rng.choice(
                        len(events), size=take, replace=False
                    )
                    events = [events[int(i)] for i in sorted(picked)]
                elif len(events) < take:
                    rows.extend(
                        _aviation_observed_rows(
                            bundle, day, direction, events, parents
                        )
                    )
                    gap = take - len(events)
                    synthetic_days.append(f"{day}:{direction}")
                    rows.extend(
                        _aviation_synthetic_rows(
                            bundle,
                            rng,
                            day=day,
                            direction=direction,
                            parents=parents,
                            span=span,
                            count=gap,
                        )
                    )
                    continue
                rows.extend(
                    _aviation_observed_rows(
                        bundle, day, direction, events, parents
                    )
                )
            else:
                synthetic_days.append(f"{day}:{direction}")
                rows.extend(
                    _aviation_synthetic_rows(
                        bundle,
                        rng,
                        day=day,
                        direction=direction,
                        parents=parents,
                        span=span,
                        count=take,
                    )
                )
    emitted = len(rows)
    if emitted > budget_rows > 0:
        picked = rng.choice(len(rows), size=budget_rows, replace=False)
        rows = [rows[int(index)] for index in sorted(picked)]
        emitted = budget_rows
        parents.aviation.clear()
        for row in rows:
            parents.add_aviation(
                row["parent_id"],
                "observed_rows"
                if row["provenance_status"] == "observed_proxy"
                else "synthetic_rows",
                1,
            )
    elif emitted < budget_rows:
        gap = budget_rows - emitted
        fill_day = days[-1] if days else pool[-1]
        rows.extend(
            _aviation_synthetic_rows(
                bundle,
                rng,
                day=fill_day,
                direction="departure",
                parents=parents,
                span=span,
                count=gap,
            )
        )
        emitted = len(rows)
    summary = {
        "days": days,
        "observed_rows": sum(
            1 for row in rows if row["provenance_status"] == "observed_proxy"
        ),
        "synthetic_rows": emitted
        - sum(
            1 for row in rows if row["provenance_status"] == "observed_proxy"
        ),
        "synthetic_day_directions": synthetic_days,
        "rows": emitted,
    }

    def batches() -> Iterator[list[dict[str, Any]]]:
        for start in range(0, len(rows), BATCH_ROWS):
            yield rows[start : start + BATCH_ROWS]

    return batches(), summary


def _aviation_observed_rows(
    bundle: ModelBundle,
    day: str,
    direction: str,
    events: list[tuple[int, str | None, str | None]],
    parents: ParentTotals,
) -> list[dict[str, Any]]:
    rows = []
    dataset_id = (
        bundle.spec.opensky_arrivals_id
        if direction == "arrival"
        else bundle.spec.opensky_departures_id
    )
    for index, (unix, icao, callsign) in enumerate(events):
        moment = datetime.fromtimestamp(unix, tz=bundle.spec.timezone)
        row = _base_row(
            bundle,
            trip_id=f"opensky-{direction}-{unix}-{index:05d}",
            mode="aviation",
            event_type=direction,
            hour=moment.hour,
            timestamp=moment.isoformat(),
            pickup_zone=bundle.spec.airport_code
            if direction == "departure"
            else None,
            drop_zone=bundle.spec.airport_code
            if direction == "arrival"
            else None,
            status="completed",
            vehicle_type=resolve_vehicle_type("aviation"),
            source="opensky",
            provenance_status="observed_proxy",
            evidence_grade="aggregate_calibrated",
            source_ids=[dataset_id],
            parent_source="observed",
            parent_id=f"flight_events:{day}:{direction}",
        )
        row["direction"] = direction
        row["icao24"] = icao
        row["callsign"] = callsign
        parents.add_aviation(row["parent_id"], "observed_rows", 1)
        rows.append(row)
    return rows


def _aviation_synthetic_rows(
    bundle: ModelBundle,
    rng: np.random.Generator,
    *,
    day: str,
    direction: str,
    parents: ParentTotals,
    span: int,
    count: int | None = None,
) -> list[dict[str, Any]]:
    fit = bundle.clocks.aviation[direction]
    if count is None:
        mu = aviation_mean_for(fit, day, span, bundle.spec.window_start)
        count = nb_quantile(float(rng.random()), mu, fit.phi)
    count = int(count)
    if count <= 0:
        return []
    profile = np.asarray(
        bundle.aviation[direction].weights(is_weekend(day))
    )
    hours = dm_allocate(
        rng, count, profile, bundle.aviation[direction].kappa
    )
    parent_id = f"flight_events:{day}:{direction}"
    parents.add_aviation(parent_id, "synthetic_rows", count)
    rows = []
    seq = 0
    for hour in range(24):
        for _ in range(int(hours[hour])):
            second = int(rng.integers(0, 3600))
            row = _base_row(
                bundle,
                trip_id=f"fs-av-{direction}-{day}-{seq:07d}",
                mode="aviation",
                event_type=direction,
                hour=hour,
                timestamp=_timestamp(bundle, day, hour, second),
                pickup_zone=bundle.spec.airport_code
                if direction == "departure"
                else None,
                drop_zone=bundle.spec.airport_code
                if direction == "arrival"
                else None,
                status="completed",
                vehicle_type=resolve_vehicle_type("aviation"),
                source="synthetic",
                provenance_status="synthetic",
                evidence_grade="aggregate_calibrated",
                source_ids=[
                    bundle.spec.opensky_arrivals_id
                    if direction == "arrival"
                    else bundle.spec.opensky_departures_id
                ],
                parent_source="sampled",
                parent_id=parent_id,
            )
            row["direction"] = direction
            # fs marker: a real icao24 is six hex characters, and
            # "fs" cannot appear in one, so synthetic ids can never
            # join onto a real aircraft.
            row["icao24"] = f"fs{rng.integers(0, 0xFFFFF):05x}"
            row["callsign"] = f"SYN{int(rng.integers(1000, 9999))}"
            rows.append(row)
            seq += 1
    return rows
