"""Layer 2 shape priors. Hour, station, ward, and aviation profiles.

Everything here is shape, never scale: the stitched city-hour archive
is ~0.37% of city-day volume (checked 2026-10-14) and is used only as
an hour-of-day mix. Profiles are JSON-serializable so the model
bundle can carry them with a version and a hash.

All builders take pyarrow tables so tests can pass fixture tables.
Missing cells stay absent. Nothing is zero-filled to look complete.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import numpy as np
import pyarrow as pa

KAPPA_GRID: tuple[float, ...] = (5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 600.0)
_TRAIN_FRACTION = 0.8


@dataclass(frozen=True)
class HourProfile:
    """A 24-bin hour-of-day mix per day type. Each sums to 1."""

    weekday: tuple[float, ...]
    weekend: tuple[float, ...]
    kappa: float
    note: str = ""

    def weights(self, day_is_weekend: bool) -> tuple[float, ...]:
        return self.weekend if day_is_weekend else self.weekday


@dataclass(frozen=True)
class MetroProfiles:
    """Station universe, station shares, and entry/exit hour shapes.

    ``stations`` keys are regime words ``pre`` and ``post`` (the
    Yellow Line opened 2025-08-11). A station id is the station code
    when the geography table has one, else the RTI label. The
    station-hour weight is the product share x hour, the global
    fallback level of the ADR-0010 hierarchy.
    """

    stations: dict[str, tuple[str, ...]]
    shares: dict[str, dict[str, float]]
    hour_entry: HourProfile
    hour_exit: HourProfile
    label_to_id: dict[str, str]
    line_open: date

    def active(self, on: date) -> tuple[str, ...]:
        return self.stations.get(self.regime(on), ())

    def regime(self, on: date) -> str:
        return "post" if on >= self.line_open else "pre"


def to_date(day: str) -> date:
    year, month, day_of_month = (int(part) for part in day.split("-"))
    return date(year, month, day_of_month)


def freeze_day(day: str, window_start: date, window_end: date) -> str:
    """The same weekday inside the window, at the terminal level.

    A day after the window takes the latest same-weekday day on or
    before ``window_end``. A day before it takes the earliest one on or
    after ``window_start``. The trend term stops at the window edge.
    """
    moment = to_date(day)
    if moment > window_end:
        weeks = -(-(moment - window_end).days // 7)
        return (moment - timedelta(days=7 * weeks)).isoformat()
    if moment < window_start:
        weeks = -(-(window_start - moment).days // 7)
        return (moment + timedelta(days=7 * weeks)).isoformat()
    return day


def is_weekend(day: str) -> bool:
    return to_date(day).weekday() >= 5


def _normalize(values: Any) -> tuple[float, ...]:
    array = np.asarray(values, dtype=float)
    total = float(array.sum())
    if total <= 0:
        raise ValueError("profile has no mass")
    return tuple(float(v) for v in array / total)


def _flat_profile(note: str) -> HourProfile:
    flat = _normalize([1.0] * 24)
    return HourProfile(
        weekday=flat,
        weekend=flat,
        kappa=KAPPA_GRID[0],
        note=note,
    )


def _hour(day_counts: dict[int, float], offset: int = 0) -> np.ndarray:
    array = np.zeros(24)
    for hour, count in day_counts.items():
        array[max(0, min(23, hour + offset))] += float(count)
    return array


def _fit_two_shapes(
    days: list[str],
    counts_for: Any,
) -> tuple[tuple[float, ...], tuple[float, ...]]:
    """Pool per-day 24-bin counts into weekday and weekend shapes."""
    shapes = {"weekday": np.zeros(24), "weekend": np.zeros(24)}
    for day in days:
        shapes["weekend" if is_weekend(day) else "weekday"] += _hour(
            counts_for(day)
        )
    weekday = _normalize(shapes["weekday"])
    weekend = _normalize(shapes["weekend"])
    return weekday, weekend


def _dm_loglik(
    counts: np.ndarray, profile: tuple[float, ...], kappa: float
) -> float:
    """Dirichlet-multinomial predictive log-likelihood of one day."""
    total = int(counts.sum())
    if total == 0:
        return 0.0
    conc = np.maximum(np.asarray(profile, dtype=float) * kappa, 1e-12)
    loglik = math.lgamma(kappa + total) - math.lgamma(kappa)
    for count, weight in zip(counts, conc, strict=True):
        if count <= 0:
            continue
        loglik += (
            math.lgamma(count + weight)
            - math.lgamma(weight)
            - math.lgamma(count + 1.0)
        )
    return loglik


def _select_kappa(
    days: list[str],
    counts_for: Any,
    weekday: tuple[float, ...],
    weekend: tuple[float, ...],
) -> float:
    """Pick kappa by held-out Dirichlet-multinomial likelihood.

    The later ``1 - train fraction`` of dates are the evaluation
    dates. Test dates never fit the profile they are scored on
    (ADR-0010 gate G6 at the profile level).
    """
    days = sorted(days)
    split = max(1, int(len(days) * _TRAIN_FRACTION))
    evaluation = days[split:]
    if not evaluation:
        return KAPPA_GRID[3]
    best_kappa = KAPPA_GRID[3]
    best_score = -math.inf
    for kappa in KAPPA_GRID:
        score = 0.0
        for day in evaluation:
            profile = weekend if is_weekend(day) else weekday
            score += _dm_loglik(
                _hour(counts_for(day)), profile, kappa
            )
        if score > best_score:
            best_score = score
            best_kappa = kappa
    return float(best_kappa)


def build_ridehail_hour_profile(
    city_hour: pa.Table,
    *,
    booking_type: str,
    vehicle_type: str,
) -> HourProfile:
    """The clock series' rows in the stitched hour archive, by hour.

    ``booking_type`` is the city profile's clock booking type and
    ``vehicle_type`` the archive token for the clock vehicle
    (Namma Yatri spells it ``Auto``). The archive covers
    2024-02-23 -> 2026-05-10 with last-write-wins overwrites. It is
    a shape prior only.
    """
    frame = city_hour.select(
        ["date", "hour", "done_ride", "booking_type", "vehicle_type"]
    ).to_pydict()
    per_day: dict[str, dict[int, float]] = {}
    for day, hour, done, booking, vehicle in zip(
        frame["date"],
        frame["hour"],
        frame["done_ride"],
        frame["booking_type"],
        frame["vehicle_type"],
        strict=True,
    ):
        if booking != booking_type or str(vehicle).strip() != vehicle_type:
            continue
        if done is None:
            continue
        key = str(day)[:10]
        bucket = per_day.setdefault(key, {})
        bucket[int(hour)] = bucket.get(int(hour), 0.0) + float(done)
    if not per_day:
        return _flat_profile("no Normal Auto city-hour rows")
    days = sorted(per_day)
    weekday, weekend = _fit_two_shapes(days, lambda day: per_day[day])
    kappa = _select_kappa(days, lambda day: per_day[day], weekday, weekend)
    return HourProfile(
        weekday=weekday,
        weekend=weekend,
        kappa=kappa,
        note=(
            f"stitched city-hour {booking_type} {vehicle_type} "
            "done_ride; shape only"
        ),
    )


def build_ward_shares(
    ward_hour: pa.Table, prefix: str | None = None
) -> dict[int, tuple[list[str], np.ndarray]]:
    """Ward activity share by hour from the stitched ward-hour archive.

    A spatial allocation prior only. This table has ward ids, not
    coordinates. The producer copies the geography pin onto the
    envelope.

    ``prefix`` is the city's ward prefix (``CitySpec.ward_prefix``).
    Ids with another prefix are out of scope (ADR-0017) and carry no
    pin. They are dropped here, so the shares renormalise over the
    city wards and no emitted event lacks a place on the map.
    """
    frame = ward_hour.select(["ward_num", "hour", "done_ride"]).to_pydict()
    per_hour: dict[int, dict[str, float]] = {h: {} for h in range(24)}
    global_share: dict[str, float] = {}
    for ward, hour, done in zip(
        frame["ward_num"], frame["hour"], frame["done_ride"], strict=True
    ):
        if done is None:
            continue
        if prefix is not None and not str(ward).startswith(f"{prefix}_"):
            continue
        name = str(ward)
        bucket = per_hour[int(hour)]
        bucket[name] = bucket.get(name, 0.0) + float(done)
        global_share[name] = global_share.get(name, 0.0) + float(done)
    global_total = sum(global_share.values())
    result: dict[int, tuple[list[str], np.ndarray]] = {}
    for hour in range(24):
        bucket = per_hour.get(hour) or {}
        wards = sorted(set(bucket) or set(global_share))
        if not wards:
            raise ValueError("ward-hour archive has no wards")
        # Shrink each hour's mix toward the all-hours mix so thin
        # hours do not invent sharp ward spikes.
        anchor = global_total / 24.0
        weights = np.asarray(
            [
                bucket.get(ward, 0.0)
                + anchor * global_share.get(ward, 0.0) / max(global_total, 1.0)
                for ward in wards
            ],
            dtype=float,
        )
        result[hour] = (wards, weights / weights.sum())
    return result


def _label_to_id(geography: pa.Table) -> dict[str, str]:
    """RTI label -> station id. Code when the table has one, else label."""
    mapping: dict[str, str] = {}
    for row in geography.to_pylist():
        label = row["rti_label"]
        if not label:
            continue
        mapping[label] = row["station_code"] or label
    return mapping


def build_metro_profiles(
    station_hour: pa.Table, geography: pa.Table, line_open: date
) -> MetroProfiles:
    """Regime station sets, station shares, entry/exit hour shapes.

    September supplies Entry and Exit separately (hour grain from
    ``interval_start``). August is footfall (``august_hourly``) and
    contributes pre-Yellow-Line station shares only.
    """
    label_to_id = _label_to_id(geography)
    frame = station_hour.select(
        ["service_date", "station", "measure", "interval_start", "count"]
    ).to_pydict()
    regime_sets: dict[str, set[str]] = {"pre": set(), "post": set()}
    share_pre: dict[str, float] = {}
    share_post: dict[str, float] = {}
    entry_days: dict[str, dict[int, float]] = {}
    exit_days: dict[str, dict[int, float]] = {}
    for day, station, measure, interval, count in zip(
        frame["service_date"],
        frame["station"],
        frame["measure"],
        frame["interval_start"],
        frame["count"],
        strict=True,
    ):
        if count is None:
            continue
        station_id = label_to_id.get(station, station)
        regime = "post" if to_date(str(day)) >= line_open else "pre"
        regime_sets[regime].add(station_id)
        hour = int(str(interval)[:2])
        value = float(count)
        if measure == "august_hourly":
            share_pre[station_id] = share_pre.get(station_id, 0.0) + value
        elif measure == "entry":
            share_post[station_id] = share_post.get(station_id, 0.0) + value
            entry_days.setdefault(str(day), {})
            entry_days[str(day)][hour] = (
                entry_days[str(day)].get(hour, 0.0) + value
            )
        elif measure == "exit":
            share_post[station_id] = share_post.get(station_id, 0.0) + value
            exit_days.setdefault(str(day), {})
            exit_days[str(day)][hour] = (
                exit_days[str(day)].get(hour, 0.0) + value
            )
    if not regime_sets["pre"] and not regime_sets["post"]:
        raise ValueError("station-hour table has no RTI stations")
    hour_entry = _hour_profile_or_flat(
        entry_days, "September entry station-hours; system hour shape"
    )
    hour_exit = _hour_profile_or_flat(
        exit_days, "September exit station-hours; system hour shape"
    )
    shares = {
        "pre": _share_or_flat(share_pre),
        "post": _share_or_flat(share_post),
    }
    return MetroProfiles(
        stations={
            regime: tuple(sorted(regime_sets[regime]))
            for regime in ("pre", "post")
        },
        shares=shares,
        hour_entry=hour_entry,
        hour_exit=hour_exit,
        label_to_id=label_to_id,
        line_open=line_open,
    )


def _hour_profile_or_flat(
    days: dict[str, dict[int, float]], note: str
) -> HourProfile:
    if not days:
        return _flat_profile(f"no {note}")
    day_list = sorted(days)
    weekday, weekend = _fit_two_shapes(day_list, lambda day: days[day])
    kappa = _select_kappa(day_list, lambda day: days[day], weekday, weekend)
    return HourProfile(weekday=weekday, weekend=weekend, kappa=kappa, note=note)


def _share_or_flat(shares: dict[str, float]) -> dict[str, float]:
    if not shares:
        raise ValueError("no station shares")
    total = sum(shares.values())
    return {station: value / total for station, value in sorted(shares.items())}


def build_aviation_profiles(
    flight_events: pa.Table,
    window_start: date,
    window_end: date,
) -> dict[str, HourProfile]:
    """Hour-of-day shape per direction from observed in-window events.

    Observed-proxy events keep their source timestamps. This profile
    conditions synthetic draws outside fetched windows only.
    """
    frame = flight_events.select(
        ["direction", "local_date", "local_hour"]
    ).to_pydict()
    per_direction: dict[str, dict[str, dict[int, float]]] = {}
    for direction, day, hour in zip(
        frame["direction"],
        frame["local_date"],
        frame["local_hour"],
        strict=True,
    ):
        if not day:
            continue
        if not window_start <= to_date(str(day)) <= window_end:
            continue
        bucket = per_direction.setdefault(str(direction), {})
        day_key = str(day)
        bucket.setdefault(day_key, {})
        bucket[day_key][int(hour)] = (
            bucket[day_key].get(int(hour), 0.0) + 1.0
        )
    result: dict[str, HourProfile] = {}
    for direction in ("arrival", "departure"):
        days = per_direction.get(direction, {})
        if not days:
            result[direction] = _flat_profile(f"no observed {direction} events")
            continue
        day_list = sorted(days)
        weekday, weekend = _fit_two_shapes(
            day_list, lambda day: days[day]
        )
        kappa = _select_kappa(
            day_list, lambda day: days[day], weekday, weekend
        )
        result[direction] = HourProfile(
            weekday=weekday,
            weekend=weekend,
            kappa=kappa,
            note=f"observed {direction} events in the window; shape only",
        )
    return result
