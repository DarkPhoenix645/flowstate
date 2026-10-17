"""Build the canonical analysis tables from files already on disk."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from flowstate.config import Settings
from flowstate.normalize.cities import DEFAULT_CITY_ID, load
from flowstate.normalize.cities.profile import CityProfile
from flowstate.normalize.coverage import TableBuild, write_json, write_table
from flowstate.normalize.gates import evaluate
from flowstate.normalize.places.geocode import (
    canonical_names,
    lookup_airports,
    lookup_stations,
    places_cache_path,
)
from flowstate.normalize.sources import (
    ensure_sources,
    reset_source_cache,
    source_hashes,
)
from flowstate.normalize.tables.airports import (
    from_profile as airports_from_profile,
)
from flowstate.normalize.tables.city_day import from_disk as city_day_from_disk
from flowstate.normalize.tables.flights import from_disk as flights_from_disk
from flowstate.normalize.tables.geography import apply_station_pins
from flowstate.normalize.tables.geography import (
    from_disk as geography_from_disk,
)
from flowstate.normalize.tables.metro_daily import from_disk as metro_from_disk
from flowstate.normalize.tables.od import from_disk as od_from_disk
from flowstate.normalize.tables.significant import from_disk as dates_from_disk
from flowstate.normalize.tables.station_hour import (
    from_disk as station_from_disk,
)
from flowstate.normalize.tables.wards import from_disk as wards_from_disk

TABLES = load(DEFAULT_CITY_ID).tables


def run(
    settings: Settings,
    city_id: str = DEFAULT_CITY_ID,
    *,
    geocode: bool = False,
) -> dict[str, Any]:
    """Read ingest outputs for one city.

    Write data/normalized. Do not download source extracts.
    ``geocode`` true sends station and airport cache misses to
    Places API (New). A miss opens a human review.
    """
    profile = load(city_id)
    reset_source_cache()
    ensure_sources(list(profile.dataset_ids))
    datasets = settings.datasets_dir
    directory = settings.data_dir / "normalized"
    builds = _build(datasets, profile)
    geocode_stats = _attach_pins(settings, profile, builds, geocode)
    hashes = source_hashes(list(profile.dataset_ids))
    for built in builds.values():
        built.coverage["source_files"] = hashes
        built.coverage["city"] = profile.id
    gates = evaluate(builds, datasets, profile)
    written = {
        name: write_table(directory, name, builds[name])
        for name in profile.tables
    }
    write_json(directory / "gates.json", gates)
    manifest = {
        "city": profile.id,
        "timezone": str(profile.timezone),
        "tables": {
            name: {"rows": item["rows"], "sha256": item["sha256"]}
            for name, item in written.items()
        },
        "gates": {name: gate["pass"] for name, gate in gates.items()},
        "source_files": hashes,
    }
    write_json(directory / "manifest.json", manifest)
    return {
        "dir": str(directory),
        "city": profile.id,
        "tables": written,
        "gates": gates,
        "geocode": geocode_stats,
    }


def _attach_pins(
    settings: Settings,
    profile: CityProfile,
    builds: dict[str, TableBuild],
    geocode: bool,
) -> dict[str, Any]:
    geography = builds["station_geography"]
    rows = geography.table.to_pylist()
    names = [str(row["name"]) for row in rows]
    aliases = dict(profile.station_aliases)
    review = settings.data_dir / "geocode" / f"{profile.id}.review.json"
    station_pins, stats = lookup_stations(
        names,
        aliases=aliases,
        canonical=canonical_names(rows, aliases),
        query_template=profile.station_geocode,
        path=places_cache_path(settings.data_dir, profile.id),
        review=review,
        bounds=profile.geocode_bounds,
        api_key=settings.google_maps_api_key,
        fetch=geocode,
    )
    pinned = apply_station_pins(geography, station_pins)
    pinned.coverage["geocode"] = stats
    builds["station_geography"] = pinned
    cache = places_cache_path(settings.data_dir, profile.id)
    airport_pins, airport_stats = lookup_airports(
        [(airport.code, airport.query) for airport in profile.airports],
        path=cache,
        review=review,
        bounds=profile.geocode_bounds,
        api_key=settings.google_maps_api_key,
        fetch=geocode,
    )
    airports = airports_from_profile(profile, airport_pins)
    airports.coverage["geocode"] = airport_stats
    builds["airport_geography"] = airports
    merged = {
        "cached": stats["cached"] + airport_stats["cached"],
        "fetched": stats["fetched"] + airport_stats["fetched"],
        "missed": stats["missed"] + airport_stats["missed"],
        "needs_review": [
            *stats["needs_review"],
            *airport_stats["needs_review"],
        ],
    }
    return merged


def _build(
    datasets: Path, profile: CityProfile
) -> dict[str, TableBuild]:
    builds: dict[str, TableBuild] = {
        "namma_yatri_city_day": city_day_from_disk(datasets, profile),
        "metro_daily": metro_from_disk(datasets, profile),
        "significant_dates_classified": dates_from_disk(datasets, profile),
        "flight_events": flights_from_disk(datasets, profile),
    }
    station = station_from_disk(datasets, profile)
    builds["metro_station_hour_long"] = station
    geography = geography_from_disk(
        datasets,
        profile,
        station.extra["rti_labels"],
        station.extra["line_labels"],
        station.extra["first_seen"],
    )
    builds["station_geography"] = geography
    builds["ward_geography"] = wards_from_disk(datasets, profile)
    builds["od_profile"] = od_from_disk(
        datasets,
        profile,
        geography.extra["code_to_rti"],
        station.extra["august_index"],
    )
    return builds


def output_dir(settings: Settings) -> Path:
    return settings.data_dir / "normalized"
