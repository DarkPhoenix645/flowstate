"""City profile.

v1 registers one city. That city is Bengaluru.
A second city is out of scope for v1.
Tests do not run a second city.
Fill the fields below when that work starts.

Add a module beside ``bengaluru.py``.
Register the profile in ``CITIES``.
Builders read the profile.
Do not copy dates or codes into a builder.

Field guide:

- ``id``: city id. The geocode cache file uses this id.
- ``timezone``: local zone for flight date bins.
- ``source_city_code``: ride-hail city code to keep.
- ``booking_type``: ride-hail booking type to keep.
- ``window_start`` and ``window_end``: inclusive study window.
- ``metro_last_valid``: drop metro rows after this date.
- ``line_name`` and ``line_open``: line that opens on that date.
- ``service_spans``: inclusive date spans of the station-hour source.
- ``airports``: one ``Airport`` for each airport in the city.
  A city can have more than one airport.
  Bengaluru has one airport.
- ``geocode_bounds``: Places search rectangle for this city.
- ``station_geocode``: Places text query. ``{name}`` is the
  canonical station name. Do not use this template for an airport.
  An airport uses ``Airport.query`` and included type ``airport``.
- ``station_aliases``: ``(alias, canonical name)``. Name mapping
  only. No coordinates. A name that is neither a canonical station
  name nor an alias is not sent to Places.
- ``ward``: ward-id join. See ``WardJoin``.
  ``geojson_parts`` is the polygon path under the datasets dir.
  A missing file is downloaded from ``source_url`` into that path.
- ``city_hour`` and ``ward_hour_stem``: archive checks for this extract.
- ``lexicon``: tokens for significant-date labels.
- Dataset ids, table names, and path tuples name files on disk.

``make normalize`` writes stations and airports to
``data/geocode/<id>.places.json``.
A stored query is not sent again.
A Places miss is cached as a miss and listed in
``data/geocode/<id>.review.json`` until a human finishes that entry.
Pytest does not call Places.
Ward pins and bus-stop pins do not use this cache.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class Airport:
    """One airport pin.

    ``code`` is the ICAO code.
    ``query`` is the Places text query.
    Set ``opensky_extract`` only when flight files for that code are on disk.
    """

    code: str
    query: str
    opensky_extract: bool


@dataclass(frozen=True)
class GeoBounds:
    """Rectangle passed to Places as the search limit."""

    min_latitude: float
    max_latitude: float
    min_longitude: float
    max_longitude: float

    def contains(self, latitude: float, longitude: float) -> bool:
        return (
            self.min_latitude <= latitude <= self.max_latitude
            and self.min_longitude <= longitude <= self.max_longitude
        )


@dataclass(frozen=True)
class WardJoin:
    """How a ward id becomes a map pin."""

    city_id: str
    in_scope_prefix: str
    number_field: str
    name_field: str
    axis: str
    source_url: str
    catalogue_url: str
    scheme_year: str
    licence: str
    geojson_parts: tuple[str, ...]
    geojson_sha256: str
    dataset_id: str
    feed_parts: tuple[str, ...]


@dataclass(frozen=True)
class CityHourCheck:
    """Archive arithmetic the city-hour stitch must meet."""

    stem: str
    n_commits: int
    distinct_commit_dates: int
    calendar_days_in_commit_span: int
    missing_commit_dates: int
    duplicate_commit_dates: int
    empty_snapshots: int
    rows_after_dedupe: int
    repeated_keys: int
    changed_keys: int


@dataclass(frozen=True)
class LocalityLexicon:
    """Tokens for one city's significant-date labels."""

    local_class: str
    local_tokens: tuple[str, ...]
    holiday_tokens: tuple[str, ...]
    other_places: tuple[str, ...]
    city_names: tuple[str, ...]
    nonlocal_phrases: tuple[str, ...]
    away_unless_city: tuple[str, ...]


@dataclass(frozen=True)
class CityProfile:
    id: str
    timezone: ZoneInfo
    source_city_code: str
    booking_type: str
    window_start: date
    window_end: date
    metro_last_valid: date
    line_name: str
    line_open: date
    service_spans: tuple[tuple[date, date], ...]
    # One record per airport. v1 lists one. A later city can list many.
    airports: tuple[Airport, ...]
    geocode_bounds: GeoBounds
    # Example: "{name} metro station"
    station_geocode: str
    # (alias, canonical name). No coordinates.
    station_aliases: tuple[tuple[str, str], ...]
    dataset_ids: tuple[str, ...]
    tables: tuple[str, ...]
    ward: WardJoin
    city_hour: CityHourCheck
    ward_hour_stem: str
    stitched_parts: tuple[str, ...]
    lexicon: LocalityLexicon
    city_day_dataset: str
    city_day_parts: tuple[str, ...]
    metro_dataset: str
    metro_parts: tuple[str, ...]
    rti_dataset: str
    rti_dir: tuple[str, ...]
    august_book: str
    september_book: str
    september_entry_sheet: str
    september_exit_sheet: str
    station_codes_name: str
    od_book: str
    opensky_dataset_arrivals: str
    opensky_dataset_departures: str
    opensky_dir: tuple[str, ...]
    dates_dataset: str
    dates_parts: tuple[str, ...]
