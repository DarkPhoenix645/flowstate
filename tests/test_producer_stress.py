"""Random replays and scenario stimulation. Fixture city, no broker."""

from __future__ import annotations

import random
from datetime import date, timedelta

import numpy as np
import pyarrow as pa
import pytest

import resample_fixtures as fx
from flowstate.resample import build_bundle
from flowstate.resample.bundle import ResampleConfig
from flowstate.resample.pins import PinBook
from flowstate.resample.profiles import build_ward_shares
from flowstate.resample.scenario import (
    ScenarioCommand,
    apply_fork,
    load_policy,
    observed_activity_per_day,
)
from flowstate.resample.unfold import ParentTotals, bus_events, rng_for
from flowstate.resample.window import (
    freeze_to_window,
    frozen_source,
    mode_budgets,
    outside_window,
)
from flowstate.streaming.producer import (
    ReplayRequest,
    emit_plan,
    merge_sequence,
    order_failures,
)

RIDE = "flowstate.ridehail.activity.v1"
METRO = "flowstate.metro.station_hour.v1"
BUS = "flowstate.bus.scheduled_stop.v1"
ARRIVAL = "flowstate.aviation.arrival.v1"
DEPARTURE = "flowstate.aviation.departure.v1"
ROWS = 12_000


@pytest.fixture(scope="module")
def bundle():
    return build_bundle(fx.fixture_inputs(), fx.fixture_spec(), seed=42)


@pytest.fixture(scope="module")
def inputs():
    return fx.fixture_inputs()


def _pins(station_ward: dict[str, str] | None = None) -> PinBook:
    return fx.fixture_pins(station_ward)


def _request(bundle, day: str, seed: int, commands=None) -> ReplayRequest:
    return ReplayRequest(
        bundle_version=bundle.version,
        seed=seed,
        window_start=day,
        window_end=day,
        acceleration=60.0,
        max_eps=500.0,
        commands=commands or [],
    )


def _policy():
    return load_policy().model_copy(
        update={"kia_catchment_wards": ["b_1", "b_2", "b_3", "b_4"]}
    )


def _ids(plan, topic: str, scenario_id: str | None) -> list[str]:
    return [
        event["event_id"]
        for event in plan.by_topic.get(topic, [])
        if event["scenario_id"] == scenario_id
    ]


def test_ward_universe_drops_out_of_scope_ids():
    table = pa.Table.from_pylist(
        [
            {"ward_num": "b_1", "hour": 9, "done_ride": 5},
            {"ward_num": "b_2", "hour": 9, "done_ride": 5},
            {"ward_num": "m_1", "hour": 9, "done_ride": 50},
            {"ward_num": "w_0", "hour": 9, "done_ride": 500},
        ]
    )
    kept = build_ward_shares(table, prefix="b")
    for wards, weights in kept.values():
        assert wards == ["b_1", "b_2"]
        assert float(weights.sum()) == pytest.approx(1.0)
    assert build_ward_shares(table)[9][0] == ["b_1", "b_2", "m_1", "w_0"]


def test_frozen_day_keeps_the_weekday():
    known = ["2025-12-22", "2025-12-23", "2025-12-29", "2025-12-30"]
    # 2026-01-05 is a Monday. The latest Monday on file is 2025-12-29.
    assert frozen_source(known, "2026-01-05") == "2025-12-29"
    # Before every known Monday, take the earliest Monday.
    assert frozen_source(known, "2025-12-15") == "2025-12-22"
    # No Sunday on file: nothing to freeze at.
    assert frozen_source(known, "2026-01-04") is None


def test_mode_budgets_are_natural_shares(bundle, inputs):
    budgets = mode_budgets(inputs, bundle, ROWS)
    assert sum(budgets.values()) == ROWS
    assert budgets["ride_hail"] > 0.9 * ROWS
    plan = emit_plan(
        bundle, inputs, _request(bundle, "2025-08-20", 3), mode="replay",
        rows=ROWS,
    )
    rows = plan.plan["rows"]
    assert rows["total"] == ROWS
    assert rows["ride_hail"] >= budgets["ride_hail"] - 60


def test_random_replays_keep_the_contract(bundle, inputs):
    rng = random.Random(20261027)
    start = date(2025, 1, 2)
    days = [
        (start + timedelta(days=rng.randrange(0, 360))).isoformat()
        for _ in range(4)
    ]
    days.extend(["2025-06-15", "2026-03-04"])
    pins = _pins()
    for seed in [rng.randrange(1, 8000) for _ in range(3)]:
        day = rng.choice(days)
        request = _request(bundle, day, seed)
        first = emit_plan(
            bundle, inputs, request, mode="replay", rows=ROWS, pins=pins
        )
        second = emit_plan(
            bundle, inputs, request, mode="replay", rows=ROWS, pins=pins
        )
        assert first.qa_failures == []
        assert first.audit[1]["pins_missing"] == 0
        assert {RIDE, ARRIVAL, DEPARTURE} <= set(first.by_topic)
        merged = merge_sequence(first.by_topic)
        keys = [(e["event_time"], e["event_id"]) for _, e in merged]
        assert keys == sorted(keys)
        for topic, events in first.by_topic.items():
            assert _ids(first, topic, None) == _ids(second, topic, None)
            assert order_failures({topic: events}) == []
            for event in events:
                status = event["provenance"]["status"]
                assert status != "observed"
                outside = outside_window(bundle, event["event_time"][:10])
                assert event["provenance"]["outside_training_window"] is outside
                at = event["location_ref"]["at"]
                assert at["latitude"] is not None
                if event["mode"] == "ride_hail":
                    assert at["ward_id"].startswith("b_")
                    assert event["location_ref"]["drop"]["ward_id"].startswith(
                        "b_"
                    )
        for event in first.by_topic.get(BUS, []):
            # Bus is stamped on the replay day, not on the feed date.
            assert event["event_time"].startswith(day)
            assert event["feed_version"] == "2026-09-07"


def test_shock_scales_one_ward_and_flight_lifts_only_the_fork(bundle, inputs):
    day = "2025-08-20"
    policy = _policy()
    baseline = emit_plan(
        bundle, inputs, _request(bundle, day, 7), mode="replay", rows=ROWS
    )
    shock = ScenarioCommand(
        mode="ride_hail",
        scenario_type="custom_shock",
        location_ref="b_1",
        start=f"{day}T00:00:00+05:30",
        duration="PT24H",
        multiplier=3.0,
        scenario_id="shock-1",
    )
    shocked = emit_plan(
        bundle,
        inputs,
        _request(bundle, day, 7, [shock]),
        mode="replay",
        rows=ROWS,
        policy=policy,
    )

    def ward_count(plan, scenario):
        return sum(
            1
            for event in plan.by_topic[RIDE]
            if event["scenario_id"] == scenario
            and event["location_ref"]["at"].get("ward_id") == "b_1"
        )

    assert ward_count(shocked, None) > 0
    assert ward_count(shocked, "shock-1") >= 2 * ward_count(shocked, None)
    assert len(_ids(shocked, METRO, None)) == len(
        _ids(shocked, METRO, "shock-1")
    )
    assert len(_ids(baseline, RIDE, None)) == len(_ids(shocked, RIDE, None))

    flight = ScenarioCommand(
        mode="aviation",
        scenario_type="flight_arrival",
        location_ref="TEST",
        start=f"{day}T00:00:00+05:30",
        duration="PT24H",
        multiplier=3.0,
        scenario_id="flight-1",
    )
    flown = emit_plan(
        bundle,
        inputs,
        _request(bundle, day, 7, [flight]),
        mode="replay",
        rows=ROWS,
        policy=policy,
    )
    summary = flown.audit[1]["scenario"]
    assert summary["injected_flights"] > 0
    assert summary["lifecycles"] > 0
    assert len(_ids(flown, ARRIVAL, "flight-1")) > len(
        _ids(flown, ARRIVAL, None)
    )
    assert len(_ids(flown, METRO, "flight-1")) == len(
        _ids(flown, METRO, None)
    )
    extra = [
        event
        for event in flown.by_topic[RIDE]
        if event["scenario_id"] == "flight-1"
        and (
            event["event_id"].startswith("flight-1:fs-rh-booking-")
            or event["event_id"].startswith("flight-1:fs-rh-end-")
        )
    ]
    assert len(extra) == summary["lifecycles"] * 2
    assert len(_ids(flown, RIDE, "flight-1")) > len(_ids(flown, RIDE, None))


def _metro_world(bundle, inputs):
    """A baseline with one busy station, bus calls near it, and a plan."""
    day = "2025-08-20"
    base = emit_plan(
        bundle,
        inputs,
        _request(bundle, day, 11),
        mode="replay",
        rows=ROWS,
    )
    return day, base


def test_metro_disruption_moves_riders_to_ride_hail(bundle, inputs):
    day, base = _metro_world(bundle, inputs)
    station = base.by_topic[METRO][0]["location_ref"]["at"]["station_id"]
    pins = _pins({station: "b_2"})
    command = ScenarioCommand(
        mode="metro",
        scenario_type="metro_disruption",
        location_ref=station,
        start=f"{day}T00:00:00+05:30",
        duration="PT24H",
        multiplier=0.2,
        scenario_id="metro-1",
    )
    plan = emit_plan(
        bundle,
        inputs,
        _request(bundle, day, 11, [command]),
        mode="replay",
        rows=ROWS,
        policy=_policy(),
        pins=pins,
    )
    effect = plan.audit[1]["scenario"]
    assert effect["metro_riders_removed"] > 0
    assert effect["shift_ride_hail_lifecycles"] > 0
    assert effect["shift_applied"] is True

    def riders(scenario):
        return sum(
            e["entries_count"]
            for e in plan.by_topic[METRO]
            if e["scenario_id"] == scenario
            and e["location_ref"]["at"]["station_id"] == station
        )

    assert riders("metro-1") < riders(None)
    added = [
        e
        for e in plan.by_topic[RIDE]
        if e["scenario_id"] == "metro-1" and "fs-rh-metro-" in e["event_id"]
    ]
    assert len(added) == effect["shift_ride_hail_lifecycles"] * 2
    assert {e["location_ref"]["at"]["ward_id"] for e in added} == {"b_2"}
    assert len(_ids(plan, ARRIVAL, "metro-1")) == len(
        _ids(plan, ARRIVAL, None)
    )


def test_metro_disruption_without_a_ward_says_why(bundle, inputs):
    day, base = _metro_world(bundle, inputs)
    station = base.by_topic[METRO][0]["location_ref"]["at"]["station_id"]
    command = ScenarioCommand(
        mode="metro",
        scenario_type="metro_disruption",
        location_ref=station,
        start=f"{day}T00:00:00+05:30",
        duration="PT24H",
        multiplier=0.2,
        scenario_id="metro-2",
    )
    plan = emit_plan(
        bundle,
        inputs,
        _request(bundle, day, 11, [command]),
        mode="replay",
        rows=ROWS,
        policy=_policy(),
    )
    effect = plan.audit[1]["scenario"]
    assert effect["metro_riders_removed"] > 0
    assert effect["shift_applied"] is False
    assert "no crosswalk" in effect["shift_note"]


def test_metro_shift_adds_bus_boardings_and_keeps_trip_balance(inputs):
    config = ResampleConfig(assumed_weekday_boardings=2_000)
    local = build_bundle(inputs, fx.fixture_spec(), config=config, seed=42)
    day = "2025-08-20"
    parents = ParentTotals()
    stream, _ = bus_events(
        local,
        rng_for(1, "bus"),
        inputs.stop_times,
        feed_date=day,
        budget_rows=20,
        parents=parents,
        id_prefix=f"{day}-",
    )
    bus_rows = [row for batch in stream for row in batch]
    assert bus_rows
    metro_row = {
        **bus_rows[0],
        "trip_id": "fs-metro-x",
        "mode": "metro",
        "event_type": "station_hour",
        "pickup_zone": "S1",
        "timestamp": bus_rows[0]["timestamp"],
        "hour": bus_rows[0]["hour"],
        "entries_count": 1000,
        "exits_count": 1000,
        "count": 2000,
        "provenance_status": "synthetic",
    }
    policy = _policy()
    command = ScenarioCommand(
        mode="metro",
        scenario_type="metro_disruption",
        location_ref="S1",
        start=f"{day}T00:00:00+05:30",
        duration="PT24H",
        multiplier=0.2,
        scenario_id="metro-3",
    )
    pins = _pins({"S1": "b_2"})
    fork, summary = apply_fork(
        [metro_row, *bus_rows],
        [command],
        local,
        policy,
        seed=5,
        factor=1.0,
        cancel_share=0.2,
        pins=pins,
        scale={
            "metro_scale": {day: {"real_total": 50_000, "people": 1_000}}
        },
    )
    assert summary["shift_bus_boardings"] > 0
    capacity = (
        local.config.assumed_bus_capacity * local.config.assumed_max_load_factor
    )
    by_trip: dict[str, list[dict]] = {}
    for row in fork:
        if row["mode"] == "bus":
            by_trip.setdefault(row["parent_id"], []).append(row)
    touched = 0
    for rows in by_trip.values():
        assert sum(r["boardings"] for r in rows) == sum(
            r["alightings"] for r in rows
        )
        assert max(r["onboard_leaving"] for r in rows) <= capacity
        touched += sum(
            1 for r in rows if r["evidence_grade"] == "assumption_driven"
        )
    assert touched > 0
    original = sum(r["boardings"] for r in bus_rows)
    assert sum(r["boardings"] for rows in by_trip.values() for r in rows) == (
        original + summary["shift_bus_boardings"]
    )


def test_amplification_scale_is_consistent(bundle, inputs):
    observed = observed_activity_per_day(
        inputs.city_day, bundle.spec.window_start, bundle.spec.window_end
    )
    assert observed > 0
    assert np.isfinite(observed)


def test_trend_freezes_at_the_window_edge(bundle, inputs):
    # The fixture window ends 2025-12-31 (a Wednesday).
    assert freeze_to_window(bundle, "2025-06-04") == "2025-06-04"
    assert freeze_to_window(bundle, "2026-01-07") == "2025-12-31"
    assert freeze_to_window(bundle, "2026-10-28") == "2025-12-31"
    assert (
        date.fromisoformat(freeze_to_window(bundle, "2027-03-02")).weekday()
        == date.fromisoformat("2027-03-02").weekday()
    )
    plan = emit_plan(
        bundle,
        inputs,
        _request(bundle, "2027-03-02", 5),
        mode="replay",
        rows=ROWS,
    )
    arrivals = len(plan.by_topic[ARRIVAL])
    departures = len(plan.by_topic[DEPARTURE])
    assert arrivals > 0 and departures > 0
    assert 0.2 < arrivals / departures < 5
    assert all(
        e["provenance"]["status"] == "synthetic"
        for e in plan.by_topic[ARRIVAL] + plan.by_topic[DEPARTURE]
    )


def test_short_waits_add_up_and_the_cap_holds():
    from flowstate.streaming.producer import send_batches

    class Fake:
        def send(self, topic, key, value):
            class _Ack:
                def get(self, timeout=None):
                    return None

            return _Ack()

        def flush(self):
            return None

    events = [
        {"event_id": f"e{i}", "event_time": "2025-08-20T10:00:00+05:30"}
        for i in range(100)
    ]
    slept: list[float] = []
    send_batches(
        Fake(),
        {RIDE: events},
        [],
        [],
        pace=[0.0] + [0.0005] * 99,
        sleeper=slept.append,
    )
    # No wait is shorter than the sleep floor, and none is lost.
    assert slept and min(slept) >= 0.002
    assert sum(slept) == pytest.approx(0.0005 * 99, abs=0.002)


def test_request_file_feeds_the_cli(tmp_path):
    import json

    from flowstate.streaming.producer import _parse, _settings_from

    path = tmp_path / "request.json"
    path.write_text(
        json.dumps(
            {
                "mode": "historical",
                "rows": 5000,
                "max_eps": 77,
                "seed": 9,
                "window_start": "2025-08-01",
                "seek": "2025-08-01T12:00:00+05:30",
                "commands": [{"scenario_id": "x"}],
            }
        )
    )
    values = _settings_from(_parse(["--request", str(path), "--seed", "11"]))
    assert values["mode"] == "historical"
    assert values["rows"] == 5000
    assert values["events_per_sec"] == 77
    assert values["seed"] == 11  # a flag wins over the file
    assert values["seek"] == "2025-08-01T12:00:00+05:30"
    assert values["commands"] == [{"scenario_id": "x"}]


def test_shipped_metro_shares_are_evidence_bounded():
    spec = load_policy().scenarios["metro_disruption"]
    shares = spec.shift_shares
    assert shares == {"ride_hail": 0.15, "bus": 0.30}
    assert sum(shares.values()) == pytest.approx(spec.shift_fraction)
    assert sum(shares.values()) < 1


def test_emitted_at_is_set_at_send_time():
    from datetime import datetime, timedelta

    from flowstate.streaming.producer import send_batches

    class Fake:
        def __init__(self):
            self.values = []

        def send(self, topic, key, value):
            self.values.append(dict(value))

            class _Ack:
                def get(self, timeout=None):
                    return None

            return _Ack()

        def flush(self):
            return None

    start = datetime.fromisoformat("2026-10-27T10:00:00+05:30")
    ticks = iter(start + timedelta(seconds=n) for n in range(100))
    fake = Fake()
    events = [
        {
            "event_id": f"e{i}",
            "event_time": "2025-08-20T10:00:00+05:30",
            "emitted_at": None,
        }
        for i in range(3)
    ]
    send_batches(fake, {RIDE: events}, [], [], clock=lambda: next(ticks))
    stamps = [value["emitted_at"] for value in fake.values]
    assert stamps == sorted(set(stamps)) and len(stamps) == 3
    assert all(item.startswith("2026-10-27T10:00:0") for item in stamps)
    assert all(
        value["event_time"] == "2025-08-20T10:00:00+05:30"
        for value in fake.values
    )


def test_live_drops_events_that_passed_during_planning(bundle, inputs):
    from datetime import datetime

    from flowstate.streaming.producer import refresh_live

    early = datetime.fromisoformat("2026-05-01T09:00:00+05:30")
    plan = emit_plan(
        bundle,
        inputs,
        _request(bundle, "2026-05-01", 3),
        mode="live",
        rows=ROWS,
        clock=lambda: early,
    )
    later = datetime.fromisoformat("2026-05-01T09:00:05+05:30")
    fresh = refresh_live(plan, _request(bundle, "2026-05-01", 3), later)
    events = [e for rows in fresh.by_topic.values() for e in rows]
    assert events
    assert all(
        datetime.fromisoformat(e["event_time"]) >= later for e in events
    )
    first = min(events, key=lambda e: (e["event_time"], e["event_id"]))
    gap = datetime.fromisoformat(first["event_time"]) - later
    expected = gap.total_seconds()
    assert fresh.pace_seconds[0] == pytest.approx(max(expected, 0.0))
    assert sum(len(v) for v in fresh.by_topic.values()) <= sum(
        len(v) for v in plan.by_topic.values()
    )
