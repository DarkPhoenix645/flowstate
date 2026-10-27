"""The model bundle. Layer 1 + Layer 2 fitted, versioned, hashed.

One bundle serves ``make amplify`` and the producer (ADR-0010: one
model bundle, seed policy, and profiles for both). Nothing here
publishes or writes parquet; ``writer.py`` owns output.

City independence: the resampler reads a :class:`CitySpec`, a small
interface of city facts. :func:`city_spec` adapts the registered
``CityProfile``; no city date or code is copied into a builder. A
second city registers a profile and the adapter maps it.

``load_inputs`` is the only function that touches real paths, and it
gates every dataset read through
``flowstate.provenance.require_valid``. An incomplete OpenSky
extract is never valid. Tests build bundles from fixture tables and
a fixture spec, skipping the gate on purpose.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from datetime import date
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from flowstate.config import Settings
from flowstate.config.model import ResampleConfig
from flowstate.normalize.cities import DEFAULT_CITY_ID, load
from flowstate.normalize.cities.profile import CityProfile
from flowstate.provenance import require_valid
from flowstate.resample.clocks import (
    Clocks,
    Marginal,
    build_clocks,
)
from flowstate.resample.profiles import (
    HourProfile,
    MetroProfiles,
    build_aviation_profiles,
    build_metro_profiles,
    build_ridehail_hour_profile,
    build_ward_shares,
)

BUNDLE_VERSION = "resampler-1.0.0"
PROFILE_VERSION = "profiles-1.0.0"
FARE_BASIS = "synthetic_fare_inr"


@dataclass(frozen=True)
class CitySpec:
    """City facts the resampler reads.

    ``holdout_start``/``holdout_end`` are the station-hour service
    spans: the observed hourly window the evaluation scores on.
    ``airport_code`` is the OpenSky-extract airport. ``local_class``
    is the significant-date class that counts as a verified local
    event. ``booking_type`` is the ride-hail clock booking type.
    """

    id: str
    timezone: ZoneInfo
    window_start: date
    window_end: date
    line_open: date
    holdout_start: date
    holdout_end: date
    booking_type: str
    local_class: str
    airport_code: str
    opensky_arrivals_id: str
    opensky_departures_id: str
    # Ward ids with this prefix are the city. Other ids in the
    # ward-hour archive are out of scope and are never emitted.
    ward_prefix: str


def city_spec(profile: CityProfile) -> CitySpec:
    """Adapt one registered city profile. The single city seam."""
    if not profile.service_spans:
        raise ValueError(f"{profile.id} has no station-hour service spans")
    airports = [a for a in profile.airports if a.opensky_extract]
    if not airports:
        raise ValueError(f"{profile.id} has no OpenSky extract airport")
    return CitySpec(
        id=profile.id,
        timezone=profile.timezone,
        window_start=profile.window_start,
        window_end=profile.window_end,
        line_open=profile.line_open,
        holdout_start=min(span[0] for span in profile.service_spans),
        holdout_end=max(span[1] for span in profile.service_spans),
        booking_type=profile.booking_type,
        local_class=profile.lexicon.local_class,
        airport_code=airports[0].code,
        opensky_arrivals_id=profile.opensky_dataset_arrivals,
        opensky_departures_id=profile.opensky_dataset_departures,
        ward_prefix=profile.ward.in_scope_prefix,
    )


def bus_prior_regime(assumed_weekday_boardings: int) -> str:
    """Target year / regime the boarding scale claims (#40).

    The centre is a model choice inside the press-reported 2025
    scale. The audited CAG FY2019-20 figure is the strict replay.
    """
    if assumed_weekday_boardings == 3_310_000:
        return "FY2019-20 replay (CAG audited base, pre-Shakti)"
    return "2025/26 citywide scenario (model choice, post-Shakti)"



@dataclass(frozen=True)
class BundleInputs:
    """Tables the bundle fits from. Fixture-friendly."""

    city_day: pa.Table
    city_hour: pa.Table
    ward_hour: pa.Table
    metro_daily: pa.Table
    station_hour: pa.Table
    geography: pa.Table
    flight_events: pa.Table
    significant: pa.Table
    stop_times: pa.Table
    bus_trips: pa.Table


@dataclass(frozen=True)
class ModelBundle:
    """Fitted bundle. Immutable in spirit; the hash pins content."""

    version: str
    profile_version: str
    seed: int
    spec: CitySpec
    config: ResampleConfig
    clocks: Clocks
    hour_profile: HourProfile
    ward_shares: dict[int, tuple[list[str], np.ndarray]]
    metro: MetroProfiles
    aviation: dict[str, HourProfile]
    coverage: dict[str, Any]
    config_hash: str = ""

    @property
    def seed_id(self) -> str:
        return f"seed-{self.seed}"

    def span_days(self) -> int:
        return max(1, (self.spec.window_end - self.spec.window_start).days + 1)


def load_inputs(settings: Settings) -> BundleInputs:
    """Read the staged and normalized tables, gated by the manifest."""
    profile = load(DEFAULT_CITY_ID)
    require_valid(
        list(profile.dataset_ids) + ["bmtc_gtfs"],
        settings.datasets_dir,
    )
    normalized = settings.data_dir / "normalized"

    def staged(dataset: str, name: str) -> pa.Table:
        path = settings.staged_dir / dataset / f"{name}.parquet"
        if not path.is_file():
            raise FileNotFoundError(
                f"{path} is missing. Run make stage and make normalize."
            )
        return pq.read_table(path)

    def analysis(name: str) -> pa.Table:
        path = normalized / f"{name}.parquet"
        if not path.is_file():
            raise FileNotFoundError(
                f"{path} is missing. Run make normalize."
            )
        return pq.read_table(path)

    return BundleInputs(
        city_day=staged("namma_yatri", "city_day"),
        city_hour=staged("namma_yatri", "city_hour"),
        ward_hour=staged("namma_yatri", "ward_hour"),
        metro_daily=staged("metro", "daily"),
        station_hour=staged("metro", "station_hour"),
        geography=analysis("station_geography"),
        flight_events=analysis("flight_events"),
        significant=analysis("significant_dates_classified"),
        stop_times=staged("bmtc", "stop_times"),
        bus_trips=staged("bmtc", "trips"),
    )


def _table_fingerprint(table: pa.Table) -> str:
    payload = json.dumps(
        {"rows": table.num_rows, "columns": list(table.column_names)},
        sort_keys=True,
    ).encode()
    return hashlib.sha256(payload + b"\n" + str(table).encode()).hexdigest()


def build_bundle(
    inputs: BundleInputs,
    spec: CitySpec,
    config: ResampleConfig | None = None,
    *,
    seed: int = 42,
    config_hash: str = "",
) -> ModelBundle:
    """Fit Layer 1 and Layer 2 from tables. Deterministic, no I/O."""
    config = config or ResampleConfig()
    clocks = build_clocks(
        inputs.city_day,
        inputs.metro_daily,
        inputs.flight_events,
        inputs.significant,
        window_start=spec.window_start,
        window_end=spec.window_end,
        holdout_start=spec.holdout_start,
        holdout_end=spec.holdout_end,
        line_open=spec.line_open,
        local_class=spec.local_class,
        rule=config.coverage,
    )
    hour_profile = build_ridehail_hour_profile(
        inputs.city_hour,
        booking_type=spec.booking_type,
        vehicle_type=config.clock_vehicle_type,
    )
    ward_shares = build_ward_shares(
        inputs.ward_hour, prefix=spec.ward_prefix
    )
    metro = build_metro_profiles(
        inputs.station_hour, inputs.geography, spec.line_open
    )
    aviation = build_aviation_profiles(
        inputs.flight_events, spec.window_start, spec.window_end
    )
    coverage = {
        "city": spec.id,
        "timezone": str(spec.timezone),
        "window": [
            spec.window_start.isoformat(),
            spec.window_end.isoformat(),
        ],
        "holdout": [
            spec.holdout_start.isoformat(),
            spec.holdout_end.isoformat(),
        ],
        "line_open": spec.line_open.isoformat(),
        "airport_code": spec.airport_code,
        "tables": {
            name: {
                "rows": table.num_rows,
                "fingerprint": _table_fingerprint(table),
            }
            for name, table in (
                ("city_day", inputs.city_day),
                ("city_hour", inputs.city_hour),
                ("ward_hour", inputs.ward_hour),
                ("metro_daily", inputs.metro_daily),
                ("station_hour", inputs.station_hour),
                ("station_geography", inputs.geography),
                ("flight_events", inputs.flight_events),
                ("significant_dates", inputs.significant),
                ("bmtc_stop_times", inputs.stop_times),
                ("bmtc_trips", inputs.bus_trips),
            )
        },
        "veracity": [
            "The city-hour archive is a shape prior only "
            "(~0.37% of city-day volume on the checked files).",
            "Ward shares allocate events to ward ids. The Kafka "
            "envelope copies the geography pin. It does not geocode "
            "the ward id.",
            "Pickup and drop wards are independent shape-prior "
            "draws. No auto origin-destination evidence exists in "
            "the accepted sources.",
            "Metro hour shapes come from the entry and exit "
            "station-hours. Footfall rows supply the pre-opening "
            "station shares.",
            "Aviation hour shapes come from observed in-window "
            "events. They condition synthetic draws only.",
        ],
    }
    return ModelBundle(
        version=BUNDLE_VERSION,
        profile_version=PROFILE_VERSION,
        seed=seed,
        spec=spec,
        config=config,
        clocks=clocks,
        hour_profile=hour_profile,
        ward_shares=ward_shares,
        metro=metro,
        aviation=aviation,
        coverage=coverage,
        config_hash=config_hash,
    )


def bundle_json(bundle: ModelBundle) -> dict[str, Any]:
    """Serializable form of the bundle for the run manifest."""

    def marginal(marginal: Marginal) -> dict[str, Any]:
        return {
            "mode": marginal.mode,
            "keep_aviation_terms": marginal.keep_flights,
            "phi": marginal.fit_final.phi,
            "beta": [float(v) for v in marginal.fit_final.beta],
            "names": list(marginal.fit_final.names),
        }

    def hour_json(profile: HourProfile) -> dict[str, Any]:
        return {
            "weekday": list(profile.weekday),
            "weekend": list(profile.weekend),
            "kappa": profile.kappa,
            "note": profile.note,
        }

    return {
        "schema_version": bundle.version,
        "profile_version": bundle.profile_version,
        "seed": bundle.seed,
        "seed_id": bundle.seed_id,
        "config": bundle.config.model_dump(mode="json"),
        "city": bundle.spec.id,
        "window": [
            bundle.spec.window_start.isoformat(),
            bundle.spec.window_end.isoformat(),
        ],
        "holdout": [
            bundle.spec.holdout_start.isoformat(),
            bundle.spec.holdout_end.isoformat(),
        ],
        "line_open": bundle.spec.line_open.isoformat(),
        "ride_hail": marginal(bundle.clocks.ride_hail),
        "metro": marginal(bundle.clocks.metro),
        "copula": {
            "rho": bundle.clocks.copula.rho,
            "kept": bundle.clocks.copula.kept,
            "shared_dates": bundle.clocks.copula.shared_dates,
        },
        "aviation_daily": {
            direction: {"phi": fit.phi}
            for direction, fit in bundle.clocks.aviation.items()
        },
        "hour_profile": hour_json(bundle.hour_profile),
        "metro_profiles": {
            "stations": {
                regime: list(stations)
                for regime, stations in bundle.metro.stations.items()
            },
            "hour_entry": hour_json(bundle.metro.hour_entry),
            "hour_exit": hour_json(bundle.metro.hour_exit),
            "note": "station-hour weight = station share x hour shape",
        },
        "aviation_profiles": {
            direction: hour_json(profile)
            for direction, profile in bundle.aviation.items()
        },
        "coverage": bundle.coverage,
    }


def fit_cache_key(
    inputs: BundleInputs,
    seed: int,
    version: str,
    config: ResampleConfig,
) -> str:
    """Fit inputs only. A policy edit does not change this key.

    The coverage rule and ``clock_vehicle_type`` are the
    ``resample_config`` fields the fit reads.
    """
    tables = {
        name: _table_fingerprint(table)
        for name, table in (
            ("city_day", inputs.city_day),
            ("city_hour", inputs.city_hour),
            ("ward_hour", inputs.ward_hour),
            ("metro_daily", inputs.metro_daily),
            ("station_hour", inputs.station_hour),
            ("station_geography", inputs.geography),
            ("flight_events", inputs.flight_events),
            ("significant_dates", inputs.significant),
            ("bmtc_stop_times", inputs.stop_times),
            ("bmtc_trips", inputs.bus_trips),
        )
    }
    payload = {
        "tables": tables,
        "seed": seed,
        "version": version,
        "clock_vehicle_type": config.clock_vehicle_type,
        "coverage": config.coverage.model_dump(mode="json"),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()


class BundleCache:
    """Reuse a fit when only non-fit config changes."""

    def __init__(self) -> None:
        self._bundles: dict[str, ModelBundle] = {}
        self.builds = 0

    def bundle(
        self,
        inputs: BundleInputs,
        spec: CitySpec,
        config: ResampleConfig,
        *,
        seed: int,
        config_hash: str,
    ) -> ModelBundle:
        key = fit_cache_key(inputs, seed, BUNDLE_VERSION, config)
        cached = self._bundles.get(key)
        if cached is None:
            self.builds += 1
            cached = build_bundle(
                inputs,
                spec,
                config,
                seed=seed,
                config_hash=config_hash,
            )
            self._bundles[key] = cached
            return cached
        if cached.config_hash == config_hash and cached.config == config:
            return cached
        # Same fit, new hash. Do not refit.
        rebuilt = replace(
            cached, config=config, config_hash=config_hash, seed=seed
        )
        return rebuilt


def bundle_hash(
    bundle: ModelBundle, config_hash: str | None = None
) -> str:
    """Content hash. Includes ``config_hash``, not the policy file bytes."""
    payload = bundle_json(bundle)
    payload["config_hash"] = (
        bundle.config_hash if config_hash is None else config_hash
    )
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode()
    ).hexdigest()
