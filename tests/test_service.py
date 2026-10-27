"""The long-running producer: session logic and the service loop.

No broker. A fake producer records what would be sent. Control
messages go straight to ``Service.handle`` so each test decides when
they land.
"""

from __future__ import annotations

import json
from datetime import datetime

import pytest

import resample_fixtures as fx
from flowstate.resample import build_bundle
from flowstate.resample.scenario import ScenarioCommand, load_policy
from flowstate.streaming import service as service_module
from flowstate.streaming.control import (
    ControlMessage,
    StartRequest,
)
from flowstate.streaming.producer import (
    ReplayRequest,
    build_base_plan,
)
from flowstate.streaming.service import Runtime, Service, _parse
from flowstate.streaming.session import ReplaySession
from flowstate.streaming.topics import (
    AUDIT_TOPIC,
    COMMAND_TOPIC,
    CONTROL_TOPIC,
    topic_specs,
    v1_topics,
)

ROWS = 12_000
DAY = "2025-08-20"
ZONE = fx.ZONE


@pytest.fixture(scope="module")
def bundle():
    return build_bundle(fx.fixture_inputs(), fx.fixture_spec(), seed=42)


@pytest.fixture(scope="module")
def inputs():
    return fx.fixture_inputs()


@pytest.fixture(scope="module")
def policy():
    return load_policy().model_copy(
        update={"kia_catchment_wards": ["b_1", "b_2", "b_3", "b_4"]}
    )


@pytest.fixture(scope="module")
def pins():
    return fx.fixture_pins()


@pytest.fixture(scope="module")
def base(bundle, inputs, pins):
    request = ReplayRequest(
        bundle.version, 42, DAY, DAY, 1e9, 1e9, []
    )
    return build_base_plan(bundle, inputs, request, ROWS, pins=pins)


class _Future:
    is_done = True
    exception = None

    def succeeded(self) -> bool:
        return True

    def get(self, timeout=None):
        return None


class FakeProducer:
    def __init__(self):
        self.sent: list[tuple[str, str, dict]] = []

    def send(self, topic, key, value):
        self.sent.append((topic, key, json.loads(json.dumps(value))))
        return _Future()

    def flush(self):
        return None

    def events(self):
        return [
            (t, v)
            for t, _, v in self.sent
            if t
            not in (AUDIT_TOPIC, COMMAND_TOPIC, CONTROL_TOPIC)
        ]

    def audit(self, kind=None):
        return [
            v
            for t, _, v in self.sent
            if t == AUDIT_TOPIC
            and (kind is None or v["record_type"] == kind)
        ]


def _clock(stamp="2026-10-27T10:00:00+05:30"):
    moment = datetime.fromisoformat(stamp)
    return lambda: moment


def _start(base, **overrides):
    values = dict(
        mode="replay",
        rows=ROWS,
        seed=42,
        window_start=DAY,
        window_end=DAY,
        acceleration=1e9,
        max_eps=1e9,
        session_id="t1",
    )
    values.update(overrides)
    return StartRequest(**values)


def _session(bundle, base, policy, pins, **overrides):
    start = _start(base, **overrides)
    from_time = datetime.fromisoformat(f"{DAY}T00:00:00+05:30")
    return ReplaySession(
        bundle,
        base,
        policy,
        start,
        session_id="t1",
        pins=pins,
        clock=_clock(),
        from_time=from_time,
    )


def _service(bundle, inputs, policy, pins, **overrides):
    runtime = Runtime(
        settings=None, inputs=inputs, pins=pins, bundle=bundle, policy=policy
    )
    producer = FakeProducer()
    service = Service(
        runtime,
        producer,
        clock=_clock(),
        idle_wait=0,
        status_every=1e9,
        **overrides,
    )
    return service, producer


def _drain(service, limit=400_000):
    for _ in range(limit):
        if service.session is None:
            return
        service.run_once()
    raise AssertionError("session did not finish")


def _control(service, op, **fields):
    service.handle(
        ("control", ControlMessage(op=op, request_id=f"r-{op}", **fields))
    )


def _flight(scenario_id="fork-1", start=f"{DAY}T00:00:00+05:30"):
    return ScenarioCommand(
        mode="aviation",
        scenario_type="flight_arrival",
        location_ref="TEST",
        start=start,
        duration="PT24H",
        multiplier=3.0,
        scenario_id=scenario_id,
    )


# ---- messages ---------------------------------------------------------


def test_control_messages_are_validated():
    ok = ControlMessage.model_validate(
        {"op": "start", "start": {"rows": 5000, "seed": 7}}
    )
    assert ok.start.rows == 5000
    with pytest.raises(ValueError):
        ControlMessage.model_validate({"op": "bogus"})
    with pytest.raises(ValueError):
        ControlMessage.model_validate(
            {"op": "start", "start": {"rows": 10}}
        )
    with pytest.raises(ValueError):
        ControlMessage.model_validate({"op": "speed", "acceleration": 0})
    kind, detail = _parse(CONTROL_TOPIC, {"op": "bogus"})
    assert kind == "invalid" and "control" in detail
    kind, _ = _parse(COMMAND_TOPIC, {"scenario_id": "x"})
    assert kind == "invalid"


def test_the_control_topic_is_a_v1_topic():
    assert CONTROL_TOPIC in v1_topics()
    assert len(v1_topics()) == 8
    assert all(spec.num_partitions == 1 for spec in topic_specs())


# ---- session ----------------------------------------------------------


def test_session_sends_every_event_once_in_global_order(
    bundle, base, policy, pins
):
    session = _session(bundle, base, policy, pins)
    sent = []
    while session.step().kind == "event":
        sent.append(session.commit())
    assert session.step().kind == "done"
    keys = [(e["event_time"], e["event_id"]) for _, e in sent]
    assert keys == sorted(keys)
    assert len({k[1] for k in keys}) == len(keys)
    assert len(sent) == len(base.sequence)
    # Each topic keeps its own non-decreasing order.
    last: dict[str, str] = {}
    for topic, event in sent:
        assert event["event_time"] >= last.get(topic, "")
        last[topic] = event["event_time"]


def test_pause_resume_and_speed_change_delays(bundle, base, policy, pins):
    session = _session(
        bundle, base, policy, pins, acceleration=60.0, max_eps=1e9
    )
    # Move to an event that is later than the play head.
    while session.step().delay < 1e-3:
        session.commit()
    due = session.step()
    session.pause()
    assert session.step().kind == "paused"
    session.resume()
    again = session.step()
    assert again.event["event_id"] == due.event["event_id"]
    assert again.delay == pytest.approx(due.delay)
    session.set_acceleration(120.0)
    faster = session.step()
    assert faster.delay == pytest.approx(again.delay / 2)
    session.commit()
    assert session.effective_acceleration() is not None


def test_the_cap_binds_and_drops_nothing(bundle, base, policy, pins):
    session = _session(
        bundle, base, policy, pins, acceleration=1e9, max_eps=100.0
    )
    step = session.step()
    assert step.delay == pytest.approx(0.01)
    count = 0
    while session.step().kind == "event":
        session.commit()
        count += 1
    assert count == len(base.sequence)
    assert session.effective_acceleration() < 1e9


def test_seek_forward_keeps_the_epoch_and_backward_starts_a_new_one(
    bundle, base, policy, pins
):
    session = _session(bundle, base, policy, pins)
    for _ in range(50):
        session.commit()
    run_before = session.run_id
    noon = datetime.fromisoformat(f"{DAY}T12:00:00+05:30")
    assert session.seek(noon) is False
    assert session.run_id == run_before
    assert session.step().event["event_time"] >= noon.isoformat()
    for _ in range(20):
        session.commit()
    morning = datetime.fromisoformat(f"{DAY}T06:00:00+05:30")
    assert session.seek(morning) is True
    assert session.run_id != run_before
    assert session.run_id.endswith(".1")
    step = session.step()
    assert step.event["event_time"] >= morning.isoformat()
    assert step.event["event_time"] < noon.isoformat()


def test_a_command_forks_from_the_play_head(bundle, base, policy, pins):
    session = _session(bundle, base, policy, pins)
    sent_ids = set()
    for _ in range(len(base.sequence) // 3):
        _, event = session.commit()
        sent_ids.add(event["event_id"])
    head = session.playhead
    effect = session.add_command(_flight())
    assert effect["injected_flights"] > 0
    rest = []
    while session.step().kind == "event":
        rest.append(session.commit())
    forks = [e for _, e in rest if e["scenario_id"] == "fork-1"]
    baseline = [e for _, e in rest if e["scenario_id"] is None]
    assert forks and baseline
    assert all(
        datetime.fromisoformat(e["event_time"]) >= head for e in forks
    )
    # Nothing that went out is sent again.
    assert not ({e["event_id"] for _, e in rest} & sent_ids)
    assert len(baseline) + len(sent_ids) == len(base.sequence)
    keys = [(e["event_time"], e["event_id"]) for _, e in rest]
    assert keys == sorted(keys)


def test_one_fork_id_per_replay(bundle, base, policy, pins):
    session = _session(bundle, base, policy, pins)
    session.add_command(_flight())
    with pytest.raises(ValueError, match="one scenario_id"):
        session.add_command(_flight(scenario_id="fork-2"))
    # A second command may share the id.
    session.add_command(
        ScenarioCommand(
            mode="ride_hail",
            scenario_type="custom_shock",
            location_ref="b_1",
            start=f"{DAY}T08:00:00+05:30",
            duration="PT2H",
            multiplier=2.0,
            scenario_id="fork-1",
        )
    )


def test_a_live_session_follows_the_wall_clock(bundle, base, policy, pins):
    now = datetime.fromisoformat(f"{DAY}T09:00:00+05:30")
    start = _start(base, mode="live")
    session = ReplaySession(
        bundle,
        base,
        policy,
        start,
        session_id="live",
        pins=pins,
        clock=lambda: now,
        from_time=now,
    )
    step = session.step()
    due = datetime.fromisoformat(step.event["event_time"])
    assert due >= now
    assert step.delay == pytest.approx((due - now).total_seconds())
    with pytest.raises(ValueError, match="wall clock"):
        session.set_acceleration(5.0)


# ---- service ----------------------------------------------------------


def test_the_service_replays_a_session_end_to_end(
    bundle, inputs, policy, pins
):
    service, producer = _service(bundle, inputs, policy, pins)
    service.inbox.put(
        (
            "control",
            ControlMessage(
                op="start",
                request_id="s1",
                start=_start(None),
            ),
        )
    )
    service._wait(0)
    assert service.session is not None
    _drain(service)
    events = producer.events()
    assert len(events) > ROWS * 0.9
    keys = [(v["event_time"], v["event_id"]) for _, v in events]
    assert keys == sorted(keys)
    assert all(v["run_id"] == "t1.0" for _, v in events)
    assert all(v["emitted_at"] for _, v in events)
    assert {v["scenario_id"] for _, v in events} == {None}
    audit = producer.audit()
    kinds = [a["record_type"] for a in audit]
    assert kinds[0] == "start" and kinds[-1] == "stop"
    assert "control" in kinds
    stop = audit[-1]
    assert stop["sent"] == len(events)
    assert stop["acked"] == len(events)
    assert stop["failed"] == 0
    assert stop["qa_failures"] == []
    start = audit[0]
    assert "bus_prior" in start["parameters"]
    ack = [a for a in audit if a["record_type"] == "control"][0]
    assert ack["request_id"] == "s1" and ack["accepted"] is True


def test_pause_holds_the_stream_and_resume_continues(
    bundle, inputs, policy, pins
):
    service, producer = _service(bundle, inputs, policy, pins)
    _control(service, "start", start=_start(None))
    for _ in range(300):
        service.run_once()
    _control(service, "pause")
    held = len(producer.events())
    for _ in range(50):
        service.run_once()
    assert len(producer.events()) == held
    paused = [a for a in producer.audit("control") if a["op"] == "pause"][0]
    assert paused["state"] == "paused"
    _control(service, "resume")
    _drain(service)
    assert len(producer.events()) > held
    stop = producer.audit("stop")[0]
    assert stop["sent"] == len(producer.events())


def test_a_backward_seek_changes_the_run_id(bundle, inputs, policy, pins):
    service, producer = _service(bundle, inputs, policy, pins)
    _control(service, "start", start=_start(None))
    for _ in range(2000):
        service.run_once()
    _control(
        service, "seek", to=f"{DAY}T00:30:00+05:30"
    )
    ack = [a for a in producer.audit("control") if a["op"] == "seek"][0]
    assert ack["accepted"] is True and ack["new_epoch"] is True
    _drain(service)
    run_ids = {v["run_id"] for _, v in producer.events()}
    assert run_ids == {"t1.0", "t1.1"}
    second = [v for _, v in producer.events() if v["run_id"] == "t1.1"]
    keys = [(v["event_time"], v["event_id"]) for v in second]
    assert keys == sorted(keys)
    assert second[0]["event_time"] >= f"{DAY}T00:30:00+05:30"
    # Order holds inside each epoch, so the audit counts no regression.
    assert producer.audit("stop")[0]["qa_failures"] == []


def test_a_command_on_the_topic_forks_the_running_replay(
    bundle, inputs, policy, pins
):
    service, producer = _service(bundle, inputs, policy, pins)
    _control(service, "start", start=_start(None))
    for _ in range(3000):
        service.run_once()
    service.handle(("command", _flight()))
    ack = [a for a in producer.audit("control") if a["op"] == "command"][0]
    assert ack["accepted"] is True
    assert ack["effect"]["injected_flights"] > 0
    service.handle(("command", _flight(scenario_id="fork-2")))
    rejected = [
        a
        for a in producer.audit("control")
        if a["op"] == "command" and not a["accepted"]
    ]
    assert rejected and "one scenario_id" in rejected[0]["reason"]
    _drain(service)
    events = producer.events()
    ids = {v["scenario_id"] for _, v in events}
    assert ids == {None, "fork-1"}
    first_fork = min(
        v["event_time"] for _, v in events if v["scenario_id"] == "fork-1"
    )
    assert first_fork >= ack["from"][:19]
    stop = producer.audit("stop")[0]
    assert any("command rejected" in q for q in stop["qa_failures"])


def test_a_bad_message_does_not_stop_the_service(
    bundle, inputs, policy, pins
):
    service, producer = _service(bundle, inputs, policy, pins)
    _control(service, "pause")  # no session
    service.handle(("invalid", "flowstate.simulation.control.v1: bad"))
    service.handle(("command", _flight()))  # no session
    answers = producer.audit("control")
    assert [a["accepted"] for a in answers] == [False, False, False]
    assert service.quit is False
    _control(service, "shutdown")
    assert service.quit is True


def test_a_second_start_reuses_the_plan(
    bundle, inputs, policy, pins, monkeypatch
):
    calls = []
    real = service_module.build_base_plan

    def counting(*args, **kwargs):
        calls.append(1)
        return real(*args, **kwargs)

    monkeypatch.setattr(service_module, "build_base_plan", counting)
    service, producer = _service(bundle, inputs, policy, pins)
    _control(service, "start", start=_start(None))
    _control(service, "start", start=_start(None, session_id="t2"))
    assert len(calls) == 1
    assert [a["reason"] for a in producer.audit("stop")] == ["replaced"]
    _control(service, "stop")
    assert service.session is None


def test_status_records_report_the_play_head(bundle, inputs, policy, pins):
    ticks = iter(range(0, 10_000_000, 2))
    service, producer = _service(
        bundle, inputs, policy, pins, monotonic=lambda: float(next(ticks))
    )
    service.status_every = 1.0
    _control(service, "start", start=_start(None))
    for _ in range(40):
        service.run_once()
    status = producer.audit("status")
    assert status
    assert status[-1]["state"] == "playing"
    assert status[-1]["sent"] > 0
    assert status[-1]["playhead"].startswith(DAY)


def test_ctl_builds_valid_messages():
    from flowstate.streaming import ctl

    topic, key, value = ctl.build(
        ["start", "--rows", "5000", "--seed", "3", "--acceleration", "120"]
    )
    assert topic == CONTROL_TOPIC
    msg = ControlMessage.model_validate(value)
    assert msg.op == "start" and msg.start.rows == 5000
    assert key == msg.request_id
    topic, key, value = ctl.build(["seek", f"{DAY}T10:00:00+05:30"])
    assert ControlMessage.model_validate(value).to == f"{DAY}T10:00:00+05:30"
    topic, key, value = ctl.build(
        ["command", _flight().model_dump_json()]
    )
    assert topic == COMMAND_TOPIC and key == "fork-1"
    assert ScenarioCommand.model_validate(value).scenario_type == (
        "flight_arrival"
    )
    with pytest.raises(SystemExit):
        ctl.build(["bogus"])


def test_clip_commands_start_at_the_play_head_and_drop_finished_ones():
    from flowstate.streaming.producer import clip_commands

    command = _flight()
    noon = datetime.fromisoformat(f"{DAY}T12:00:00+05:30")
    clipped = clip_commands([command], noon)
    assert len(clipped) == 1
    assert clipped[0].start == noon.isoformat()
    assert clipped[0].duration == "PT43200S"
    after = datetime.fromisoformat("2025-08-22T00:00:00+05:30")
    assert clip_commands([command], after) == []
    before = datetime.fromisoformat("2025-08-19T00:00:00+05:30")
    assert clip_commands([command], before)[0].start == command.start


def test_a_live_start_plans_today_and_waits_for_the_clock(
    bundle, inputs, policy, pins
):
    runtime = Runtime(
        settings=None, inputs=inputs, pins=pins, bundle=bundle, policy=policy
    )
    producer = FakeProducer()
    service = Service(
        runtime,
        producer,
        clock=_clock("2026-05-01T09:00:00+05:30"),
        idle_wait=0,
        status_every=1e9,
    )
    _control(
        service,
        "start",
        start=StartRequest(mode="live", rows=ROWS, session_id="live"),
    )
    session = service.session
    assert session is not None and session.live
    assert session.acceleration == 1.0
    step = session.step()
    assert step.event["event_time"] >= "2026-05-01T09:00:00"
    assert step.event["event_time"].startswith("2026-05-01")
    start = producer.audit("start")[0]
    assert start["extrapolation"]["trend"] == "frozen_at_terminal"
    _control(service, "speed", acceleration=10)
    refused = producer.audit("control")[-1]
    assert refused["accepted"] is False and "wall clock" in refused["reason"]
    _control(service, "seek", to="2026-05-01T12:00:00+05:30")
    assert producer.audit("control")[-1]["accepted"] is False
