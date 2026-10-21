"""Fixture inputs for the resampler tests. No network, no Compose.

The fixture city is not Bengaluru: id ``testcity``, local-event class
``local_event``, airport ``TEST``, OpenSky ids ``test_arrivals`` /
``test_departures``. That proves the builders read the spec instead
of Bengaluru literals.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pyarrow as pa

from flowstate.resample.bundle import BundleInputs, CitySpec

ZONE = ZoneInfo("Asia/Kolkata")
WINDOW_START = date(2025, 1, 1)
WINDOW_END = date(2025, 12, 31)
LINE_OPEN = date(2025, 8, 11)
HOLDOUT_START = date(2025, 8, 1)
HOLDOUT_END = date(2025, 9, 30)

STATIONS_PRE = ("ST1", "ST2", "ST3")
STATION_POST_ONLY = "ST4"
WARDS = ("b_1", "b_2", "b_3", "b_4")


def fixture_spec() -> CitySpec:
    return CitySpec(
        id="testcity",
        timezone=ZONE,
        window_start=WINDOW_START,
        window_end=WINDOW_END,
        line_open=LINE_OPEN,
        holdout_start=HOLDOUT_START,
        holdout_end=HOLDOUT_END,
        booking_type="Normal",
        local_class="local_event",
        airport_code="TEST",
        opensky_arrivals_id="test_arrivals",
        opensky_departures_id="test_departures",
    )


def _dates(start: date, end: date) -> list[date]:
    return [
        start + timedelta(days=offset)
        for offset in range((end - start).days + 1)
    ]


def _city_day_table() -> pa.Table:
    days = _dates(WINDOW_START, WINDOW_END)
    rng = np.random.default_rng(11)
    rows = []
    for day in days:
        if day.isoformat() == "2025-06-15":
            continue  # a missing date stays missing
        weekday = 1.0 if day.weekday() < 5 else 0.7
        done = int(rng.negative_binomial(60, 60 / (60 + 2400 * weekday)))
        cancel = int(
            rng.negative_binomial(60, 60 / (60 + 600 * weekday))
        )
        booking = done + cancel + int(rng.integers(0, 40))
        rows.append(
            {
                "date": day.isoformat(),
                "city": "testcity",
                "city_code": "TST",
                "booking_type": "Normal",
                "booking": booking,
                "done_ride": done,
                "cancel_ride": cancel,
                "earning": float(done) * 40.0,
                "earning_per_completed_ride_proxy": 40.0,
            }
        )
    return pa.Table.from_pylist(rows)


def _metro_daily_table() -> pa.Table:
    rng = np.random.default_rng(12)
    rows = []
    for day in _dates(WINDOW_START, WINDOW_END):
        if day.weekday() == 6 and day.month % 2 == 0:
            continue  # tracker dates go missing
        weekday = 1.0 if day.weekday() < 5 else 0.6
        total = int(
            rng.negative_binomial(50, 50 / (50 + 18000 * weekday))
        )
        rows.append({"record_date": day.isoformat(), "total": total})
    return pa.Table.from_pylist(rows)


def _station_hour_table() -> pa.Table:
    rng = np.random.default_rng(13)
    rows = []
    # August footfall (pre-line shares), entry/exit split on September.
    august_days = _dates(date(2025, 8, 1), date(2025, 8, 18))
    september_days = _dates(date(2025, 9, 1), date(2025, 9, 30))
    for day in august_days:
        for station in STATIONS_PRE:
            for hour in range(24):
                rows.append(
                    {
                        "service_date": day.isoformat(),
                        "station": station,
                        "measure": "august_hourly",
                        "interval_start": f"{hour:02d}:00",
                        "count": int(
                            rng.poisson(200 * (1.5 if 7 <= hour < 20 else 0.4))
                        ),
                    }
                )
    for day in september_days:
        stations = (
            (*STATIONS_PRE, STATION_POST_ONLY)
            if day >= LINE_OPEN
            else STATIONS_PRE
        )
        for station in stations:
            for hour in range(24):
                weight = 1.5 if 7 <= hour < 20 else 0.4
                rows.append(
                    {
                        "service_date": day.isoformat(),
                        "station": station,
                        "measure": "entry",
                        "interval_start": f"{hour:02d}:00",
                        "count": int(rng.poisson(90 * weight)),
                    }
                )
                rows.append(
                    {
                        "service_date": day.isoformat(),
                        "station": station,
                        "measure": "exit",
                        "interval_start": f"{hour:02d}:00",
                        "count": int(rng.poisson(90 * weight)),
                    }
                )
    return pa.Table.from_pylist(rows)


def _geography_table() -> pa.Table:
    rows = [
        {
            "station_code": f"S{index + 1}",
            "rti_label": station,
            "line": "test",
            "operational_from": None,
        }
        for index, station in enumerate((*STATIONS_PRE, STATION_POST_ONLY))
    ]
    return pa.Table.from_pylist(rows)


def _city_hour_table() -> pa.Table:
    rng = np.random.default_rng(14)
    rows = []
    for day in _dates(date(2025, 3, 1), date(2025, 10, 30)):
        for hour in range(24):
            done = int(rng.poisson(30 * (2.0 if 8 <= hour < 21 else 0.5)))
            rows.append(
                {
                    "date": day.isoformat(),
                    "hour": hour,
                    "done_ride": done,
                    "booking_type": "Normal",
                    "vehicle_type": "Auto",
                }
            )
    return pa.Table.from_pylist(rows)


def _ward_hour_table() -> pa.Table:
    rng = np.random.default_rng(15)
    rows = []
    for day in _dates(date(2025, 3, 1), date(2025, 4, 30)):
        for ward in WARDS:
            for hour in range(24):
                rows.append(
                    {
                        "ward_num": ward,
                        "hour": hour,
                        "done_ride": int(
                            rng.poisson(4 if ward != "b_1" else 8)
                        ),
                    }
                )
    return pa.Table.from_pylist(rows)


def _flight_events_table() -> pa.Table:
    rng = np.random.default_rng(16)
    rows = []
    unix_start = int(datetime(2025, 1, 1, 0, 0, tzinfo=ZONE).timestamp())
    for day in _dates(WINDOW_START, WINDOW_END):
        if day.isoformat() == "2025-07-04":
            continue  # a missing flight day stays missing
        for hour in range(24):
            for _ in range(int(rng.poisson(1.5 if hour < 21 else 0.3))):
                unix = unix_start + int(
                    (day - WINDOW_START).days * 86400 + hour * 3600
                    + rng.integers(0, 3600)
                )
                rows.append(
                    {
                        "direction": "arrival",
                        "local_date": day.isoformat(),
                        "local_hour": hour,
                        "event_time_unix": unix,
                        "icao24": f"{rng.integers(0, 0xFFFFFF):06x}",
                        "callsign": "TST100",
                    }
                )
            for _ in range(int(rng.poisson(1.8 if 5 <= hour < 23 else 0.4))):
                unix = unix_start + int(
                    (day - WINDOW_START).days * 86400 + hour * 3600
                    + rng.integers(0, 3600)
                )
                rows.append(
                    {
                        "direction": "departure",
                        "local_date": day.isoformat(),
                        "local_hour": hour,
                        "event_time_unix": unix,
                        "icao24": f"{rng.integers(0, 0xFFFFFF):06x}",
                        "callsign": "TST200",
                    }
                )
    # A few arrival days with no arrivals: synthetic fill territory.
    return pa.Table.from_pylist(rows)


def _significant_table() -> pa.Table:
    rows = [
        {"date": "2025-01-26", "locality": "local_event"},
        {"date": "2025-05-01", "locality": "local_event"},
        {"date": "2025-08-15", "locality": "somewhere_else"},
    ]
    return pa.Table.from_pylist(rows)


def _stop_times_table() -> pa.Table:
    rows = []
    for trip in range(1, 11):
        for stop in range(1, 21):
            hour = 6 + trip % 12
            rows.append(
                {
                    "trip_id": str(trip),
                    "arrival_time": f"{hour:02d}:{(stop * 3) % 60:02d}:00",
                    "departure_time": f"{hour:02d}:{(stop * 3) % 60:02d}:00",
                    "stop_id": f"stop_{stop:03d}",
                    "stop_sequence": stop,
                }
            )
    return pa.Table.from_pylist(rows)


def _bus_trips_table() -> pa.Table:
    return pa.Table.from_pylist(
        [
            {
                "trip_id": str(trip),
                "route_id": "r1",
                "service_id": "s1",
                "direction_id": "0",
                "shape_id": "sh1",
                "trip_headsign": "loop",
            }
            for trip in range(1, 11)
        ]
    )


def fixture_inputs() -> BundleInputs:
    return BundleInputs(
        city_day=_city_day_table(),
        city_hour=_city_hour_table(),
        ward_hour=_ward_hour_table(),
        metro_daily=_metro_daily_table(),
        station_hour=_station_hour_table(),
        geography=_geography_table(),
        flight_events=_flight_events_table(),
        significant=_significant_table(),
        stop_times=_stop_times_table(),
        bus_trips=_bus_trips_table(),
    )
