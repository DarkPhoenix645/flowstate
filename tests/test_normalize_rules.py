"""Table rules that do not need the full extracts."""

from dataclasses import replace
from datetime import date

import pandas as pd
import pytest

from flowstate.normalize.cities import load
from flowstate.normalize.cities.bengaluru import BENGALURU
from flowstate.normalize.cities.profile import Airport, LocalityLexicon
from flowstate.normalize.clock import future_dates
from flowstate.normalize.tables.airports import build_airport_geography
from flowstate.normalize.tables.city_day import build_city_day
from flowstate.normalize.tables.geography import (
    apply_station_pins,
    build_geography,
    parse_station_codes,
)
from flowstate.normalize.tables.metro_daily import build_metro_daily
from flowstate.normalize.tables.significant import classify_label
from flowstate.normalize.tables.station_hour import (
    build_station_hour,
    parse_interval,
)
from flowstate.normalize.tables.wards import build_rows, contains
from flowstate.provenance import ProvenanceError


def test_parse_interval_keeps_last_train():
    start, end, last = parse_interval("23:00 Hrs To Last train")
    assert (start, end, last) == ("23:00", None, True)
    assert parse_interval("H23") == ("23:00", "24:00", False)


def test_hsli_name_keeps_the_comma():
    codes = parse_station_codes(
        "code,name\n"
        "HSLI,Sri Balagangadharanatha Swamiji Station, Hosahalli\n"
        "BYPH,Baiyappanahalli\n"
        "BNSK,Banashankari\n"
        "BSNK,Banashankari\n"
    )
    names = dict(codes)
    assert names["HSLI"] == (
        "Sri Balagangadharanatha Swamiji Station, Hosahalli"
    )
    built = build_geography(
        codes,
        ["11-Baiyappanahalli", "8-Banashankari", "113-Hosahalli"],
        ["113-Hosahalli"],
        {"11-Baiyappanahalli": "2025-08-01"},
        line_name="yellow",
        line_open=date(2025, 8, 11),
        aliases={
            "Hosahalli": (
                "Sri Balagangadharanatha Swamiji Station, Hosahalli"
            ),
        },
    )
    rows = built.table.to_pylist()
    by_label = {row["rti_label"]: row for row in rows if row["rti_label"]}
    assert by_label["11-Baiyappanahalli"]["station_code"] == "BYPH"
    assert by_label["11-Baiyappanahalli"]["operational_from"] == (
        "2025-08-01"
    )
    ambiguous = by_label["8-Banashankari"]
    assert ambiguous["station_code"] is None
    assert ambiguous["ambiguous_codes"] == "BNSK|BSNK"
    hosahalli = [
        row for row in rows if row["station_code"] == "HSLI"
    ]
    assert len(hosahalli) == 1
    assert hosahalli[0]["rti_label"] == "113-Hosahalli"
    assert hosahalli[0]["line"] == "yellow"
    assert hosahalli[0]["operational_from"] == "2025-08-11"
    assert hosahalli[0]["latitude"] is None
    assert "trip_id" not in built.table.column_names


def test_industry_is_goraguntepalya_not_yeshwanthpur():
    codes = parse_station_codes(
        "code,name\n"
        "GTPL,Goraguntepalya\n"
        "YPM,Yeshwantpur\n"
    )
    built = build_geography(
        codes,
        ["Yeshwanthpur Industry", "Yeshwanthpur"],
        [],
        {
            "Yeshwanthpur Industry": "2025-08-01",
            "Yeshwanthpur": "2025-08-01",
        },
        line_name="yellow",
        line_open=date(2025, 8, 11),
        aliases={
            "Yeshwanthpur Industry": "Goraguntepalya",
            "Yeshwantpur": "Yeshwanthpur",
        },
    )
    rows = built.table.to_pylist()
    names = [row["name"] for row in rows]
    assert names.count("Goraguntepalya") == 1
    assert names.count("Yeshwanthpur") == 0
    assert "Yeshwanthpur Industry" not in names
    gora = next(row for row in rows if row["station_code"] == "GTPL")
    ypur = next(row for row in rows if row["station_code"] == "YPM")
    assert gora["rti_label"] == "Yeshwanthpur Industry"
    assert ypur["rti_label"] == "Yeshwanthpur"
    assert gora["name"] == "Goraguntepalya"
    assert ypur["name"] == "Yeshwantpur"


def test_city_day_gap_is_not_a_zero_row():
    rows = [
        _city("2024-10-26", "BLR", "Normal", done=10),
        _city("2024-10-28", "BLR", "Normal", done=12),
        _city("2024-10-26", "BLR", "Purple", done=1),
        _city("2024-10-26", "COK", "Normal", done=4),
    ]
    built = build_city_day(rows, **_blr_clock())
    out = built.table.to_pylist()
    assert [row["date"] for row in out] == ["2024-10-26", "2024-10-28"]
    assert "2024-10-27" in built.coverage["missing"]
    assert all(row["done_ride"] != 0 for row in out)
    assert out[0]["drvr_cancel"] == 0
    assert out[0]["cnvr_rate"] is None
    assert built.coverage["rate_columns_present"] == []
    rated = _city("2024-10-26", "BLR", "Normal", done=10)
    rated["drvr_cancel"] = 4
    rated["rider_cancel"] = 2
    rated["cnvr_rate"] = 0.4
    rated["bkng_cancel_rate"] = 0.1
    rated["q_accept_rate"] = 0.8
    kept = build_city_day([rated], **_blr_clock()).table.to_pylist()[0]
    assert kept["drvr_cancel"] == 4
    assert kept["rider_cancel"] == 2
    assert kept["cnvr_rate"] == pytest.approx(0.4)
    assert kept["q_accept_rate"] == pytest.approx(0.8)
    assert built.coverage["counts"]["purple_rows_excluded"] == 1


def test_city_day_duplicate_date_raises():
    rows = [
        _city("2024-10-26", "BLR", "Normal", done=1),
        _city("2024-10-26", "BLR", "Normal", done=2),
    ]
    with pytest.raises(ValueError, match="duplicate"):
        build_city_day(rows, **_blr_clock())


def test_city_day_keeps_the_code_it_is_given():
    rows = [
        _city("2024-10-26", "BLR", "Normal", done=10),
        _city("2024-10-26", "COK", "Normal", done=4),
    ]
    built = build_city_day(
        rows,
        city_code="COK",
        booking_type="Normal",
        window_start=date(2024, 10, 26),
        window_end=date(2024, 10, 26),
    )
    kept = built.table.to_pylist()
    assert [row["city_code"] for row in kept] == ["COK"]


def test_station_pin_comes_from_the_lookup():
    codes = parse_station_codes("code,name\nBYPH,Baiyappanahalli\n")
    built = build_geography(
        codes,
        ["11-Baiyappanahalli"],
        [],
        line_name="yellow",
        line_open=date(2025, 8, 11),
    )
    pinned = apply_station_pins(
        built, {"Baiyappanahalli": (12.99, 77.65)}
    )
    row = pinned.table.to_pylist()[0]
    assert row["latitude"] == pytest.approx(12.99)
    assert row["longitude"] == pytest.approx(77.65)
    assert pinned.coverage["counts"]["coordinate_cells"] == 1


def test_two_airports_each_get_a_pin_row():
    airports = (
        Airport("AAAA", "North Field", True),
        Airport("BBBB", "South Field", False),
    )
    built = build_airport_geography(
        airports,
        {"AAAA": (13.2, 77.7), "BBBB": None},
    )
    rows = {row["airport_code"]: row for row in built.table.to_pylist()}
    assert rows["AAAA"]["opensky_extract"] is True
    assert rows["AAAA"]["latitude"] == pytest.approx(13.2)
    assert rows["BBBB"]["opensky_extract"] is False
    assert rows["BBBB"]["latitude"] is None
    assert built.coverage["extracts"] == 1
    assert built.coverage["pins"] == 1


def test_unknown_city_is_rejected():
    with pytest.raises(ValueError, match="unknown city"):
        load("pune")


def test_metro_cutoff_and_gap():
    frame = _metro(
        [
            ("2026-07-13", 1),
            ("2026-07-15", 2),
            ("2026-07-16", 9),
        ]
    )
    built = build_metro_daily(frame, **_metro_window())
    dates = built.table.column("record_date").to_pylist()
    assert dates == ["2026-07-13", "2026-07-15"]
    assert built.coverage["missing"] == ["2026-07-14"]
    assert built.coverage["excluded_after_cutoff"] == ["2026-07-16"]
    assert "2026-07-14" not in dates


def test_metro_identity_failure_raises():
    frame = _metro([("2026-07-15", 1)])
    frame["Total"] = 0
    with pytest.raises(ValueError, match="Total"):
        build_metro_daily(frame, **_metro_window())


def test_null_station_hour_is_omitted_and_zero_stays():
    august = pd.DataFrame(
        {
            "BUSINESS DATE": ["2025-08-01", "2025-08-11"],
            "STATION": ["11-Baiyappanahalli", "310-Beratena Agrahara"],
            "00:00 Hrs To     01:00 Hrs": [0, 3],
            "01:00 Hrs To     02:00 Hrs": [None, 1],
            "23:00 Hrs To Last train": [5, 2],
            "TOTAL": [5, 6],
        }
    )
    september = pd.DataFrame(
        {
            "BUSINESS DATE": ["2025-09-01"],
            "STATION": ["11-Baiyappanahalli"],
            "H00": [None],
            "H23": [4],
            "TOTAL": [4],
        }
    )
    empty = september.iloc[0:0]
    built = build_station_hour(
        august,
        september,
        empty,
        line_name="yellow",
        line_open=date(2025, 8, 11),
        service_spans=(
            (date(2025, 8, 1), date(2025, 8, 11)),
            (date(2025, 9, 1), date(2025, 9, 1)),
        ),
        august_sheet="aug",
        entry_sheet="entry",
        exit_sheet="exit",
    )
    rows = built.table.to_pylist()
    assert built.coverage["missing_cells"] == 2
    assert all(row["count"] is not None for row in rows)
    assert any(row["count"] == 0 for row in rows)
    last = [
        row
        for row in rows
        if row["ends_at_last_train"] and row["service_date"] == "2025-08-01"
    ]
    assert last[0]["interval_end"] is None
    assert last[0]["measure"] == "august_hourly"
    flags = {
        row["service_date"]: row["line_open"]
        for row in rows
        if row["measure"] == "august_hourly"
    }
    assert flags["2025-08-01"] is False
    assert flags["2025-08-11"] is True
    assert set(built.table.column("measure").to_pylist()) <= {
        "august_hourly",
        "entry",
        "exit",
    }
    assert "310-Beratena Agrahara" in built.extra["line_labels"]


def test_locality_classes():
    def kind(label: str) -> str:
        return classify_label(label, BENGALURU.lexicon)

    assert kind("Diwali") == "citywide_holiday"
    assert kind("Rajayotsava Day") == "citywide_holiday"
    assert kind("Ed Sheeran Concert") == "bengaluru_local"
    assert kind("Yellow Line Begins") == "bengaluru_local"
    assert kind("IPL: RCB v CSK (Bangalore)") == "bengaluru_local"
    assert kind("IPL Qualifier 1: RCB v GT (Dharamsala)") == "nonlocal"
    assert kind("FIFA World Cup FINAL: Spain v. Argentina") == "nonlocal"
    assert kind("Holi (North India observance)") == "nonlocal"
    assert kind("Total lunar eclipse") == "uncertain"
    assert kind("Valentine's Day; Bandland Festival D1") == (
        "bengaluru_local"
    )
    assert kind("Christmas Eve; IND v SL 2nd T20I (Cuttack)") == "uncertain"
    assert (
        kind(
            "IND v PAK T20 World Cup (Colombo); Mahashivratri; "
            "Bandland Festival D2"
        )
        == "uncertain"
    )


def test_locality_class_name_comes_from_the_lexicon():
    lexicon = LocalityLexicon(
        local_class="pune_local",
        local_tokens=("concert",),
        holiday_tokens=("diwali",),
        other_places=("bengaluru",),
        city_names=("pune",),
        nonlocal_phrases=("fifa",),
        away_unless_city=(),
    )
    assert classify_label("Concert", lexicon) == "pune_local"
    assert classify_label("Show (Bengaluru)", lexicon) == "nonlocal"


def test_future_dates_are_after_as_of():
    assert future_dates(
        ["2026-10-14", "2026-10-15"], as_of=date(2026, 10, 14)
    ) == ["2026-10-15"]


def test_require_valid_runs_before_a_read(tmp_path, monkeypatch):
    from flowstate.normalize.sources import reset_source_cache

    def boom(dataset_ids):
        raise ProvenanceError("blocked")

    monkeypatch.setattr(
        "flowstate.normalize.sources.require_valid", boom
    )
    reset_source_cache()
    from flowstate.normalize.tables.city_day import from_disk

    with pytest.raises(ProvenanceError, match="blocked"):
        from_disk(tmp_path, BENGALURU)


def _city(day, code, booking, done):
    return {
        "date": day,
        "city": "Bengaluru",
        "city_code": code,
        "booking_type": booking,
        "srch_rqst": 1,
        "srch_fr_q": 1,
        "srch_which_got_q": 1,
        "srch_which_got_e": 1,
        "booking": done + 1,
        "done_ride": done,
        "cancel_ride": 1,
        "earning": 10,
        "drvr_cancel": 0,
        "rider_cancel": 0,
    }


def test_ward_pin_keeps_latitude_first():
    square = _ward_feature(
        7,
        "Test Ward",
        [[
            [12.0, 77.0],
            [12.0, 77.2],
            [12.2, 77.2],
            [12.2, 77.0],
            [12.0, 77.0],
        ]],
    )
    rows = {
        row["ward_num"]: row
        for row in build_rows(
            ["b_7", "b_9", "m_1", "t_1", "w_0"],
            {"features": [square]},
            BENGALURU.ward,
        )
    }
    pin = rows["b_7"]
    assert pin["latitude"] == pytest.approx(12.1)
    assert pin["longitude"] == pytest.approx(77.1)
    assert pin["pin_method"] == "centroid"
    assert pin["city"] == "bengaluru"
    assert pin["ward_name"] == "Test Ward"
    assert rows["b_9"]["match_status"] == "unmatched"
    assert rows["b_9"]["latitude"] is None
    for ward_num in ("m_1", "t_1", "w_0"):
        assert rows[ward_num]["match_status"] == "out_of_scope"
        assert rows[ward_num]["city"] is None
        assert rows[ward_num]["latitude"] is None


def test_ward_pin_stays_out_of_a_hole():
    rings = [
        [
            [12.0, 77.0],
            [12.0, 81.0],
            [16.0, 81.0],
            [16.0, 77.0],
            [12.0, 77.0],
        ],
        [
            [13.0, 78.0],
            [15.0, 78.0],
            [15.0, 80.0],
            [13.0, 80.0],
            [13.0, 78.0],
        ],
    ]
    feature = _ward_feature(1, "Hole", rings)
    row = build_rows(["b_1"], {"features": [feature]}, BENGALURU.ward)[0]
    parsed = [[(point[0], point[1]) for point in ring] for ring in rings]
    assert row["pin_method"] == "interior"
    assert contains(row["latitude"], row["longitude"], parsed)
    assert not contains(row["latitude"], row["longitude"], [parsed[1]])


def test_ward_axis_comes_from_the_join():
    ring = [
        [77.0, 12.0],
        [77.2, 12.0],
        [77.2, 12.2],
        [77.0, 12.2],
        [77.0, 12.0],
    ]
    join = replace(
        BENGALURU.ward,
        city_id="pune",
        in_scope_prefix="p",
        axis="longitude_latitude",
    )
    feature = _ward_feature(7, "Other", [ring])
    row = build_rows(["p_7"], {"features": [feature]}, join)[0]
    assert row["city"] == "pune"
    assert row["latitude"] == pytest.approx(12.1)
    assert row["longitude"] == pytest.approx(77.1)


def _blr_clock() -> dict:
    return {
        "city_code": "BLR",
        "booking_type": "Normal",
        "window_start": date(2024, 10, 26),
        "window_end": date(2024, 10, 28),
    }


def _metro_window() -> dict:
    return {
        "last_valid": date(2026, 7, 15),
        "window_start": date(2026, 7, 1),
        "window_end": date(2026, 7, 31),
    }


def _ward_feature(number: int, name: str, rings: list) -> dict:
    return {
        "properties": {"KGISWardNo": str(number), "KGISWardName": name},
        "geometry": {"type": "Polygon", "coordinates": rings},
    }


def _metro(rows):
    records = []
    for day, scale in rows:
        smart, ncmc = scale, scale
        tokens, qr, group = scale, scale, scale
        commute = smart + ncmc
        casual = tokens + qr + group
        records.append(
            {
                "Record Date": day,
                "Smart Cards": smart,
                "NCMC": ncmc,
                "Commute": commute,
                "Tokens": tokens,
                "QR": qr,
                "Group Ticket": group,
                "Casual": casual,
                "Total": commute + casual,
            }
        )
    return pd.DataFrame(records)
