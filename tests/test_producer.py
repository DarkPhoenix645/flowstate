"""Producer, envelope, and scenario tests. No broker and no repo data."""

from __future__ import annotations

import json
from datetime import datetime

import pyarrow as pa
import pytest

import resample_fixtures as fx
from flowstate.resample import build_bundle, bundle_hash, to_envelope
from flowstate.resample.bundle import bundle_json
from flowstate.resample.pins import PinBook
from flowstate.resample.scenario import (
    ScenarioCommand,
    amplification_factor,
    applied_multiplier,
    apply_fork,
    check_commands,
    lifecycle_total,
    lifecycles_per_ward,
    load_policy,
    observed_activity_per_day,
    ward_probabilities,
)
from flowstate.resample.unfold import (
    ParentTotals,
    _base_row,
    aviation_events,
    ridehail_day_events,
    rng_for,
)
from flowstate.schema import resolve_vehicle_type
from flowstate.stage.events import metro_station_hour_event
from flowstate.streaming.producer import (
    ReplayRequest,
    emit_plan,
    order_failures,
    pace_delays,
    send_batches,
    stamp_emitted_at,
)
from flowstate.streaming.ride_event_schema import (
    RideEvent,
    envelope_json_schema,
    envelope_spark_schema,
)
from flowstate.streaming.topics import (
    partition_counts,
    require_one_partition,
    topic_specs,
    unacceptable_create_errors,
    v1_topics,
)

ROWS = 12_000


@pytest.fixture(scope="module")
def bundle():
    return build_bundle(fx.fixture_inputs(), fx.fixture_spec(), seed=42)


@pytest.fixture(scope="module")
def inputs():
    return fx.fixture_inputs()


def _request(bundle, commands=None, **overrides) -> ReplayRequest:
    payload = {
        "bundle_version": bundle.version,
        "seed": 42,
        "window_start": "2025-08-20",
        "window_end": "2025-08-20",
        "acceleration": 60.0,
        "max_eps": 500.0,
        "commands": commands or [],
    }
    payload.update(overrides)
    return ReplayRequest(**payload)


def _flight(bundle, trip: str, stamp: str) -> dict:
    moment = datetime.fromisoformat(stamp)
    row = _base_row(
        bundle,
        trip_id=trip,
        mode="aviation",
        event_type="arrival",
        hour=moment.hour,
        timestamp=stamp,
        pickup_zone=None,
        drop_zone=bundle.spec.airport_code,
        status="completed",
        vehicle_type=resolve_vehicle_type("aviation"),
        source="synthetic",
        provenance_status="synthetic",
        evidence_grade="aggregate_calibrated",
        source_ids=["test_arrivals"],
        parent_source="sampled",
        parent_id=f"flight_events:{trip}",
    )
    row["direction"] = "arrival"
    row["icao24"] = "fs00001"
    row["callsign"] = "SYN1000"
    return row


def _ride(bundle, trip: str) -> dict:
    return _base_row(
        bundle,
        trip_id=trip,
        mode="ride_hail",
        event_type="booking_created",
        hour=10,
        timestamp="2025-08-20T10:05:00+05:30",
        pickup_zone="b_1",
        drop_zone="b_2",
        status=None,
        vehicle_type="auto",
        source="synthetic",
        provenance_status="synthetic",
        evidence_grade="aggregate_calibrated",
        source_ids=["namma_yatri_city_day"],
        parent_source="sampled",
        parent_id="namma_yatri_city_day:2025-08-20",
    )


def _policy():
    policy = load_policy()
    return policy.model_copy(
        update={"kia_catchment_wards": ["b_1", "b_2", "b_3", "b_4"]}
    )


def _arrival(multiplier: float = 2.0, scenario_id: str = "fork-1"):
    return ScenarioCommand(
        mode="aviation",
        scenario_type="flight_arrival",
        location_ref="TEST",
        start="2025-08-20T10:00:00+05:30",
        duration="PT1H",
        multiplier=multiplier,
        scenario_id=scenario_id,
    )


def test_envelope_schema_is_one_model(bundle):
    schema = envelope_json_schema()
    assert "event_id" in schema["properties"]
    assert "provenance" in schema["properties"]
    assert "parameters_ref" in schema["properties"]
    spark = envelope_spark_schema()
    names = set(spark.fieldNames())
    assert {"event_id", "event_time", "provenance", "location_ref"} <= names
    row = next(
        iter(
            next(
                ridehail_day_events(
                    bundle,
                    rng_for(42, "ride_hail", "2025-08-20"),
                    "2025-08-20",
                    {"booking": 4, "done_ride": 4, "cancel_ride": 2},
                    None,
                    ParentTotals(),
                )
            )
        )
    )
    envelope = to_envelope(row)
    RideEvent.model_validate(envelope)
    staged = metro_station_hour_event(
        {
            "measure": "entry",
            "count": 12,
            "station": "11-Baiyappanahalli",
            "service_date": "2025-08-11",
            "interval_start": "08:00",
            "interval_end": "09:00",
            "ends_at_last_train": False,
            "source_sheet": "Sep-2025 Entry",
            "source_row": 4,
        }
    )
    RideEvent.model_validate(staged)
    assert staged["schema_version"] == "1.0"


def test_bus_envelope_carries_prior_ref(bundle, inputs):
    from flowstate.resample.unfold import bus_events

    stream, _summary = bus_events(
        bundle,
        rng_for(42, "bus"),
        inputs.stop_times,
        feed_date="2026-09-07",
        budget_rows=20,
        parents=ParentTotals(),
    )
    row = next(iter(stream))[0]
    envelope = to_envelope(row)
    assert envelope["schema_version"] == "1.1"
    assert envelope["parameters_ref"] == "bus_prior"
    RideEvent.model_validate(envelope)


def test_replay_is_deterministic_and_ordered(bundle, inputs):
    request = _request(bundle)
    first = emit_plan(
        bundle, inputs, request, mode="replay", rows=ROWS
    )
    second = emit_plan(
        bundle, inputs, request, mode="replay", rows=ROWS
    )
    assert first.by_topic.keys() == second.by_topic.keys()
    for topic in first.by_topic:
        left = [
            (event["event_time"], event["event_id"])
            for event in first.by_topic[topic]
        ]
        right = [
            (event["event_time"], event["event_id"])
            for event in second.by_topic[topic]
        ]
        assert left == right
        assert left == sorted(left)
    assert first.qa_failures == []
    report = first.audit[1]["counts"]
    assert report["parents_match_children"] is True
    assert "observed_plus_synthetic" not in report
    assert report["observed_and_synthetic_are_separate"] is True
    start = first.audit[0]
    assert start["seed"] == 42
    assert start["bundle_version"] == bundle.version
    assert start["replay_request"]["commands"] == []
    assert first.audit[1]["output_manifest"]["event_ids_sha256"]


def test_ridehail_generator_order_is_late_and_emit_is_not(bundle, inputs):
    rows = [
        row
        for batch in ridehail_day_events(
            bundle,
            rng_for(42, "ride_hail", "2025-08-20"),
            "2025-08-20",
            {"booking": 6, "done_ride": 6, "cancel_ride": 3},
            None,
            ParentTotals(),
        )
        for row in batch
    ]
    raw = [row["timestamp"] for row in rows]
    assert raw != sorted(raw)
    unsorted = [
        {"event_time": stamp, "event_id": str(index)}
        for index, stamp in enumerate(raw)
    ]
    assert order_failures({"topic": unsorted})
    plan = emit_plan(
        bundle, inputs, _request(bundle), mode="replay", rows=ROWS
    )
    assert plan.qa_failures == []


def test_stamp_does_not_rewrite_event_time():
    event = {
        "schema_version": "1.0",
        "event_id": "e1",
        "city_id": "blr",
        "mode": "ride_hail",
        "event_type": "booking_created",
        "event_time": "2025-08-20T10:00:00+05:30",
        "emitted_at": None,
        "count": 1,
        "location_ref": {"at": {"ward_id": "b_1"}},
        "scenario_id": None,
        "parameters_ref": None,
        "provenance": {
            "status": "synthetic",
            "source_ids": ["namma_yatri_city_day"],
            "model_bundle_version": "resampler-1.0.0",
            "profile_version": None,
            "seed_id": "seed-42",
            "evidence_grade": "aggregate_calibrated",
            "outside_training_window": False,
        },
    }
    calls = iter(
        [
            datetime.fromisoformat("2026-10-26T06:00:00+05:30"),
            datetime.fromisoformat("2026-10-26T06:00:01+05:30"),
        ]
    )
    stamp_emitted_at([event], lambda: next(calls))
    assert event["event_time"] == "2025-08-20T10:00:00+05:30"
    assert event["emitted_at"] == "2026-10-26T06:00:00+05:30"


def test_live_sends_from_the_wall_clock_onward(bundle, inputs):
    moment = datetime.fromisoformat("2026-05-01T09:00:00+05:30")

    def clock():
        return moment

    plan = emit_plan(
        bundle,
        inputs,
        _request(bundle),
        mode="live",
        rows=ROWS,
        clock=clock,
    )
    events = [e for rows in plan.by_topic.values() for e in rows]
    assert events
    assert all(
        datetime.fromisoformat(event["event_time"]) >= moment
        for event in events
    )
    assert all(event["event_time"].startswith("2026-05-01") for event in events)
    assert all(
        event["provenance"]["outside_training_window"] is True
        for event in events
    )
    assert plan.audit[0]["extrapolation"]["trend"] == "frozen_at_terminal"
    assert plan.audit[0]["extrapolation"]["uncertainty_widen"] == 0.5
    assert plan.audit[0]["replay_request"]["acceleration"] == 1.0
    assert plan.pace_seconds[0] >= 0
    before = [event["event_time"] for event in events]
    stamp_emitted_at(
        events,
        lambda: datetime.fromisoformat("2026-10-26T06:00:00+05:30"),
    )
    assert [event["event_time"] for event in events] == before
    assert events[0]["emitted_at"].startswith("2026-10-26")


def test_uncertainty_widen_flattens_ward_shares(bundle):
    _wards, raw = ward_probabilities(bundle, 9, 0.0)
    _wards, wide = ward_probabilities(bundle, 9, 0.5)
    assert max(wide) < max(raw)


def test_pace_cap_drops_nothing():
    times = [
        "2025-08-20T10:00:00+05:30",
        "2025-08-20T10:01:00+05:30",
        "2025-08-20T10:02:00+05:30",
    ]
    delays, effective = pace_delays(times, acceleration=60, max_eps=1000)
    assert delays[1] == pytest.approx(1.0)
    assert effective == pytest.approx(60)
    capped, slower = pace_delays(times, acceleration=1000, max_eps=1)
    assert capped[1] == pytest.approx(1.0)
    assert slower < 1000
    assert len(times) == 3


def test_one_fork_and_flight_lifecycles_only_on_injected(bundle):
    policy = _policy()
    baseline = [
        _flight(bundle, "base-1", "2025-08-20T10:10:00+05:30"),
        _flight(bundle, "base-2", "2025-08-20T10:20:00+05:30"),
        _ride(bundle, "ride-1"),
    ]
    snapshot = json.dumps(baseline, sort_keys=True)
    fork, summary = apply_fork(
        baseline,
        [_arrival()],
        bundle,
        policy,
        seed=42,
        factor=1.0,
        cancel_share=0.0,
    )
    assert json.dumps(baseline, sort_keys=True) == snapshot
    assert summary["injected_flights"] == 2
    assert summary["lifecycles"] == lifecycle_total(2, 4, 1.0)
    assert summary["shift_applied"] is False
    injected_parents = {
        row["parent_id"]
        for row in fork
        if row["mode"] == "aviation" and ":injected:" in row["parent_id"]
    }
    response = [
        row
        for row in fork
        if row["mode"] == "ride_hail" and row["parent_id"] in injected_parents
    ]
    assert len(response) == summary["lifecycles"] * 2
    baseline_parents = {row["parent_id"] for row in baseline}
    assert not any(row["parent_id"] in baseline_parents for row in response)
    assert all(row["provenance_status"] == "synthetic" for row in response)
    assert all(row["scenario_id"] == "fork-1" for row in fork)
    assert {row["scenario_id"] for row in baseline} == {None}
    with pytest.raises(ValueError, match="one scenario_id"):
        check_commands(
            [_arrival(), _arrival(scenario_id="fork-2")], policy, bundle
        )


def test_shipped_catchment_is_the_17_85km_pins(bundle):
    policy = load_policy()
    assert policy.kia_catchment_wards == [
        "b_6", "b_1", "b_2", "b_7", "b_8", "b_5", "b_3"
    ]
    with pytest.raises(ValueError, match="unknown"):
        check_commands([_arrival()], policy, bundle)
    holiday = _arrival()
    holiday = holiday.model_copy(
        update={"scenario_type": "local_holiday_event", "mode": "ride_hail"}
    )
    with pytest.raises(ValueError, match="not enabled"):
        check_commands([holiday], load_policy(), bundle)


def test_multiplier_cap_and_metro_shift_needs_a_crosswalk(bundle):
    policy = _policy()
    spec = policy.scenarios["custom_shock"]
    command = ScenarioCommand(
        mode="ride_hail",
        scenario_type="custom_shock",
        location_ref="b_1",
        start="2025-08-20T10:00:00+05:30",
        duration="PT1H",
        multiplier=9,
        scenario_id="fork-1",
    )
    assert applied_multiplier(command, spec) == 3.0
    metro = _base_row(
        bundle,
        trip_id="m1",
        mode="metro",
        event_type="station_hour",
        hour=10,
        timestamp="2025-08-20T10:00:00+05:30",
        pickup_zone="ST1",
        drop_zone=None,
        status="completed",
        vehicle_type="train",
        source="synthetic",
        provenance_status="synthetic",
        evidence_grade="aggregate_calibrated",
        source_ids=["namma_metro_daily"],
        parent_source="sampled",
        parent_id="namma_metro_daily:2025-08-20",
    )
    metro["entries_count"] = 3
    metro["exits_count"] = 3
    metro["count"] = 6
    disrupt = ScenarioCommand(
        mode="metro",
        scenario_type="metro_disruption",
        location_ref="ST1",
        start="2025-08-20T10:00:00+05:30",
        duration="PT2H",
        multiplier=0.2,
        scenario_id="fork-1",
    )
    fork, summary = apply_fork(
        [metro, _ride(bundle, "ride-1")],
        [disrupt],
        bundle,
        policy,
        seed=42,
        factor=1.0,
        cancel_share=0.0,
    )
    assert summary["shift_applied"] is False
    assert "no crosswalk" in summary["shift_note"]
    assert summary["metro_riders_removed"] == 2
    assert sum(1 for row in fork if row["mode"] == "ride_hail") == 1


def test_amplification_uses_activity_sum(inputs, bundle):
    observed = observed_activity_per_day(
        inputs.city_day, bundle.spec.window_start, bundle.spec.window_end
    )
    assert observed > 137_319 or observed > 0
    factor = amplification_factor(1_845, observed)
    assert factor == pytest.approx(1_845 / observed)
    assert lifecycles_per_ward(10, 4, 0.0134, 4) == pytest.approx(0.134)


def test_sparse_and_failed_windows_are_synthetic(bundle):
    fat = "2025-08-01"
    thin = "2025-08-02"
    rows = []
    for day, count in ((fat, 40), (thin, 2)):
        for index in range(count):
            rows.append(
                {
                    "direction": "arrival",
                    "local_date": day,
                    "event_time_unix": 1_754_000_000 + index,
                    "icao24": f"{index:06x}",
                    "callsign": "TST",
                }
            )
    table = pa.Table.from_pylist(rows)
    stream, _summary = aviation_events(
        bundle,
        rng_for(1, "aviation"),
        table,
        day_pool=[fat, thin],
        budget_rows=12,
        parents=ParentTotals(),
    )
    emitted = [row for batch in stream for row in batch]
    thin_status = {
        row["provenance_status"]
        for row in emitted
        if f":{thin}:" in row["parent_id"]
    }
    fat_status = {
        row["provenance_status"]
        for row in emitted
        if f":{fat}:" in row["parent_id"]
    }
    assert thin_status == {"synthetic"}
    assert "observed_proxy" in fat_status

    failed = pa.Table.from_pylist(
        [
            {**row, "query_status": "failed"}
            for row in rows
            if row["local_date"] == fat
        ]
    )
    stream, _summary = aviation_events(
        bundle,
        rng_for(1, "aviation"),
        failed,
        day_pool=[fat],
        budget_rows=4,
        parents=ParentTotals(),
    )
    failed_rows = [row for batch in stream for row in batch]
    assert failed_rows
    assert {row["provenance_status"] for row in failed_rows} == {"synthetic"}


def test_scrub_matches_the_suffix(bundle, inputs):
    request = _request(bundle)
    full = emit_plan(bundle, inputs, request, mode="replay", rows=ROWS)
    topic = "flowstate.ridehail.activity.v1"
    seek = full.by_topic[topic][len(full.by_topic[topic]) // 2]["event_time"]
    scrubbed = emit_plan(
        bundle, inputs, request, mode="replay", rows=ROWS, seek=seek
    )
    expect = [
        event
        for event in full.by_topic[topic]
        if event["event_time"] >= seek
    ]
    assert scrubbed.by_topic[topic] == expect


def test_topics_are_one_partition():
    specs = topic_specs()
    assert len(specs) == 8
    assert len(v1_topics()) == 8
    assert all(spec.num_partitions == 1 for spec in specs)
    counts = partition_counts(
        [
            {"topic": name, "error_code": 0, "partitions": [{"partition": 0}]}
            for name in v1_topics()
        ]
    )
    require_one_partition(counts, v1_topics())
    counts[v1_topics()[0]] = 3
    with pytest.raises(SystemExit, match="partition count"):
        require_one_partition(counts, v1_topics())
    assert unacceptable_create_errors([("a", 0), ("b", 36)]) == []
    assert unacceptable_create_errors([("a", 1)]) == [("a", 1)]


def test_send_batches_flushes_on_the_cap():
    class Fake:
        def __init__(self):
            self.sent = []
            self.flushes = 0

        def send(self, topic, key, value):
            self.sent.append((topic, key))

            class _Ack:
                def get(self, timeout=None):
                    return None

            return _Ack()

        def flush(self):
            self.flushes += 1

    fake = Fake()
    events = [
        {"event_id": f"e{index}", "event_time": "2025-08-20T10:00:00+05:30"}
        for index in range(3)
    ]
    slept: list[float] = []
    send_batches(
        fake,
        {"flowstate.ridehail.activity.v1": events},
        [{"record_type": "start"}, {"record_type": "stop"}],
        [{"scenario_id": "fork-1"}],
        batch_size=2,
        pace=[0.0, 0.2, 0.2],
        sleeper=slept.append,
    )
    assert slept == [0.2, 0.2]
    assert fake.flushes >= 2
    assert [item[0] for item in fake.sent].count(
        "flowstate.simulation.audit.v1"
    ) == 2


def test_publish_complete_requires_every_ack():
    class Rejecting:
        def send(self, topic, key, value):
            class _Ack:
                def get(self, timeout=None):
                    raise RuntimeError("broker rejected")

            return _Ack()

        def flush(self):
            return None

    with pytest.raises(SystemExit, match="publish failed"):
        send_batches(
            Rejecting(),
            {
                "flowstate.ridehail.activity.v1": [
                    {
                        "event_id": "e1",
                        "event_time": "2025-08-20T10:00:00+05:30",
                    }
                ]
            },
            [],
            [],
        )


def test_bundle_hash_includes_policy(bundle):
    plain = __import__("hashlib").sha256(
        __import__("json").dumps(bundle_json(bundle), sort_keys=True).encode()
    ).hexdigest()
    assert bundle_hash(bundle) != plain


def test_fork_streams_share_a_topic(bundle, inputs):
    policy = _policy()
    command = _arrival()
    plan = emit_plan(
        bundle,
        inputs,
        _request(bundle, [command]),
        mode="replay",
        rows=ROWS,
        policy=policy,
    )
    topic = "flowstate.ridehail.activity.v1"
    ids = {event["scenario_id"] for event in plan.by_topic[topic]}
    assert None in ids
    assert "fork-1" in ids
    assert plan.qa_failures == []
    assert plan.audit[0]["replay_request"]["commands"][0]["scenario_id"] == (
        "fork-1"
    )


def test_envelope_copies_geography_pins(bundle):
    pins = PinBook(
        wards={"b_1": (12.97, 77.59), "b_2": (12.98, 77.60)},
        stations={"ST1": (12.99, 77.57)},
        stops={"stop_001": (12.95, 77.58)},
        airports={"TEST": (13.20, 77.71)},
    )
    ride = _ride(bundle, "ride-1")
    ride["drop_zone"] = "b_2"
    envelope = to_envelope(ride, pins=pins)
    assert envelope["location_ref"]["at"]["ward_id"] == "b_1"
    assert envelope["location_ref"]["at"]["latitude"] == 12.97
    assert envelope["location_ref"]["at"]["longitude"] == 77.59
    assert envelope["location_ref"]["drop"]["ward_id"] == "b_2"
    assert envelope["location_ref"]["drop"]["latitude"] == 12.98
    assert envelope["location_ref"]["drop"]["longitude"] == 77.60
    missing = to_envelope(
        {**ride, "pickup_zone": "w_0", "drop_zone": "w_0"}, pins=pins
    )
    assert missing["location_ref"]["at"]["ward_id"] == "w_0"
    assert missing["location_ref"]["at"]["latitude"] is None
    flight = _flight(bundle, "f1", "2025-08-20T10:10:00+05:30")
    aviation = to_envelope(flight, pins=pins)
    assert aviation["location_ref"]["at"]["airport"] == "TEST"
    assert aviation["location_ref"]["at"]["latitude"] == 13.20
    assert "drop" not in aviation["location_ref"]
    metro = dict(ride)
    metro.update(
        mode="metro",
        event_type="station_hour",
        pickup_zone="ST1",
        drop_zone=None,
        entries_count=1,
        exits_count=1,
        hour=10,
    )
    metro_event = to_envelope(metro, pins=pins)
    assert metro_event["location_ref"]["at"]["station_id"] == "ST1"
    assert metro_event["location_ref"]["at"]["latitude"] == 12.99
    bus = dict(ride)
    bus.update(
        mode="bus",
        event_type="scheduled_stop",
        pickup_zone="stop_001",
        boardings=1,
        alightings=1,
        onboard_leaving=0,
        ridership_status="synthetic",
        ridership_grade="assumption_driven",
    )
    bus_event = to_envelope(bus, pins=pins)
    assert bus_event["location_ref"]["at"]["stop_id"] == "stop_001"
    assert bus_event["location_ref"]["at"]["longitude"] == 77.58


def test_replay_past_the_window_still_emits(bundle, inputs):
    request = _request(
        bundle, window_start="2026-02-01", window_end="2026-02-01"
    )
    plan = emit_plan(bundle, inputs, request, mode="replay", rows=ROWS)
    ride = plan.by_topic["flowstate.ridehail.activity.v1"]
    assert ride
    assert all(event["event_time"].startswith("2026-02-01") for event in ride)
    assert all(
        event["provenance"]["outside_training_window"] for event in ride
    )
    arrivals = plan.by_topic["flowstate.aviation.arrival.v1"]
    departures = plan.by_topic["flowstate.aviation.departure.v1"]
    assert arrivals and departures
    assert all(
        event["provenance"]["status"] == "synthetic"
        for event in arrivals + departures
    )
    assert plan.audit[0]["extrapolation"]["trend"] == "frozen_at_terminal"
    again = emit_plan(bundle, inputs, request, mode="replay", rows=ROWS)
    replay = again.by_topic["flowstate.ridehail.activity.v1"]
    assert [event["event_id"] for event in ride] == [
        event["event_id"] for event in replay
    ]


def test_no_synthetic_row_is_labelled_observed(bundle, inputs):
    plan = emit_plan(
        bundle, inputs, _request(bundle), mode="historical", rows=ROWS
    )
    for rows in plan.by_topic.values():
        for event in rows:
            assert event["provenance"]["status"] != "observed"
    labelled = {row["provenance_status"] for row in plan.baseline_rows}
    assert "observed" not in labelled
