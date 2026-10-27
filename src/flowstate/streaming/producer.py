"""One-shot producer and the planning code the service shares. #39, ADR-0017.

``python -m flowstate.streaming.producer`` plans one window and sends it
once. Use it for bulk runs and tests. The long-running producer is
``flowstate.streaming.service`` (``make stream-serve``). It builds on
``build_base_plan`` and ``build_fork`` below, so there is one planner.

Modes:

* ``historical``: build the window and send it with no pacing.
* ``replay``: send in ``event_time`` order, paced by acceleration.
* ``live``: plan today and send each event when the wall clock reaches
  its ``event_time``. Events that are already past are not sent.

``--rows`` is a count of events for the whole window. The split by mode
is the measured natural share of the alignment window (ADR-0017). A
replay carries at most one fork. ``--events-per-sec`` is a cap and
drops nothing. Replay does not rewrite ``event_time``. ``emitted_at`` is
send time. All modes go out in one global ``event_time`` order, so a
consumer sees the modes together at the play head.

Host clients use ``127.0.0.1:9094`` (``docs/dev-modes.md``). This
module does not create topics. ``make kafka-topics`` does.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import time
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime
from pathlib import Path
from typing import Any

from flowstate.config import get_settings
from flowstate.log import configure_logging
from flowstate.normalize.cities import DEFAULT_CITY_ID, load
from flowstate.normalize.places.review import require_station_reviews
from flowstate.resample import (
    build_bundle,
    bundle_hash,
    city_spec,
    load_inputs,
    to_envelope,
)
from flowstate.resample.bundle import ModelBundle
from flowstate.resample.envelope import AVIATION_TOPICS, TOPIC_BY_MODE
from flowstate.resample.pins import PinBook, load_pin_book
from flowstate.resample.scenario import (
    ScenarioCommand,
    ScenarioPolicy,
    amplification_factor,
    apply_fork,
    check_commands,
    load_policy,
    observed_activity_per_day,
    parse_duration,
)
from flowstate.resample.unfold import ParentTotals
from flowstate.resample.window import (
    MIN_ROWS,
    frozen_source,
    outside_window,
    plan_window,
    ride_lookup,
    window_days,
)
from flowstate.streaming.ride_event_schema import RideEvent
from flowstate.streaming.topics import (
    AUDIT_TOPIC,
    COMMAND_TOPIC,
    ipv4_bootstrap,
    partition_counts,
    require_one_partition,
    v1_topics,
)

BATCH_SIZE = 500
MIN_SLEEP_SECONDS = 0.002
# The plan, the envelopes, and the merge order live in memory. A bigger
# window must be replayed one day at a time (v2: stream per day).
MAX_ROWS = 2_000_000
log = logging.getLogger(__name__)
MODES = ("historical", "replay", "live")
Clock = Callable[[], datetime]
Sequence = list[tuple[str, dict[str, Any]]]


@dataclass
class ReplayRequest:
    bundle_version: str
    seed: int
    window_start: str
    window_end: str
    acceleration: float
    max_eps: float
    commands: list[ScenarioCommand] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["commands"] = [
            command.model_dump() for command in self.commands
        ]
        return payload


@dataclass
class Emission:
    by_topic: dict[str, list[dict[str, Any]]]
    audit: list[dict[str, Any]]
    qa_failures: list[str]
    pace_seconds: list[float]
    effective_acceleration: float | None
    baseline_rows: list[dict[str, Any]]
    fork_rows: list[dict[str, Any]]
    plan: dict[str, Any] = field(default_factory=dict)


def pace_delays(
    event_times: list[str], acceleration: float, max_eps: float
) -> tuple[list[float], float]:
    """Wall seconds to wait before each event. The cap drops nothing.

    Effective acceleration is the requested acceleration times the
    ratio of unconstrained wall time to capped wall time. A zero
    simulation span reports 0 when the cap still inserts a wait.
    """
    if acceleration <= 0:
        raise ValueError("acceleration must be positive")
    if max_eps <= 0:
        raise ValueError("max_eps must be positive")
    delays = [0.0]
    if len(event_times) < 2:
        return delays, acceleration
    parsed = [datetime.fromisoformat(stamp) for stamp in event_times]
    min_gap = 1.0 / max_eps
    free = 0.0
    for previous, nxt in zip(parsed, parsed[1:]):
        sim = max(0.0, (nxt - previous).total_seconds())
        wall = sim / acceleration
        free += wall
        delays.append(max(wall, min_gap))
    actual = sum(delays[1:])
    if actual == 0:
        effective = acceleration
    elif free == 0:
        effective = 0.0
    else:
        effective = acceleration * (free / actual)
    return delays, effective


def order_failures(by_topic: dict[str, list[dict[str, Any]]]) -> list[str]:
    failures: list[str] = []
    for topic, events in by_topic.items():
        previous: str | None = None
        for event in events:
            stamp = event["event_time"]
            if previous is not None and stamp < previous:
                failures.append(
                    f"{topic} {event['event_id']} at {stamp} "
                    f"is before {previous}"
                )
            previous = stamp
    return failures


def stamp_emitted_at(events: Iterable[dict[str, Any]], clock: Clock) -> None:
    """Set ``emitted_at`` at send time. Leave ``event_time`` alone."""
    for event in events:
        event["emitted_at"] = clock().isoformat()
        RideEvent.model_validate(event)


def conservation_report(
    rows: list[dict[str, Any]], parents: ParentTotals
) -> dict[str, Any]:
    by_provenance: dict[str, int] = {}
    for row in rows:
        status = row["provenance_status"]
        by_provenance[status] = by_provenance.get(status, 0) + 1
    ride_rows = sum(1 for row in rows if row["mode"] == "ride_hail")
    ride_parents = sum(parents.ridehail.values())
    aviation_rows = sum(1 for row in rows if row["mode"] == "aviation")
    aviation_parents = sum(parents.aviation.values())
    return {
        "by_provenance": by_provenance,
        "ridehail_rows": ride_rows,
        "ridehail_parent_total": ride_parents,
        "aviation_rows": aviation_rows,
        "aviation_parent_total": aviation_parents,
        "parents_match_children": (
            ride_rows == ride_parents and aviation_rows == aviation_parents
        ),
        "observed_and_synthetic_are_separate": (
            _aviation_provenances_do_not_share_a_parent(rows)
        ),
    }


def _aviation_provenances_do_not_share_a_parent(
    rows: list[dict[str, Any]],
) -> bool:
    """True when no aviation parent is both observed-proxy and synthetic."""
    seen: dict[str, set[str]] = {}
    for row in rows:
        if row.get("mode") != "aviation":
            continue
        status = row.get("provenance_status")
        if status not in ("observed_proxy", "synthetic"):
            continue
        seen.setdefault(str(row["parent_id"]), set()).add(status)
    return all(len(statuses) == 1 for statuses in seen.values())


def merge_sequence(
    by_topic: dict[str, list[dict[str, Any]]],
) -> list[tuple[str, dict[str, Any]]]:
    """Every topic in one global (event_time, event_id) order.

    Each topic keeps its own non-decreasing order, so P1 holds. A
    consumer that reads several topics sees them at the same play head.
    """
    merged = [
        (topic, event) for topic, events in by_topic.items() for event in events
    ]
    merged.sort(key=lambda pair: (pair[1]["event_time"], pair[1]["event_id"]))
    return merged


@dataclass
class BasePlan:
    """The baseline for one request, built once and reused.

    ``sequence`` is every baseline envelope in global order. A fork is
    derived from ``rows`` and never changes them.
    """

    request: ReplayRequest
    rows: list[dict[str, Any]]
    plan: dict[str, Any]
    parents: ParentTotals
    factor: float
    cancel: float
    sequence: list[tuple[str, dict[str, Any]]]


def build_base_plan(
    bundle: ModelBundle,
    inputs: Any,
    request: ReplayRequest,
    rows: int,
    *,
    pins: PinBook | None = None,
) -> BasePlan:
    if rows < MIN_ROWS:
        raise ValueError(f"rows={rows} is below the minimum {MIN_ROWS}")
    if rows > MAX_ROWS:
        raise ValueError(
            f"rows={rows} is above {MAX_ROWS}. Replay a shorter window."
        )
    parents = ParentTotals()
    baseline, plan = plan_window(
        bundle,
        inputs,
        window_start=request.window_start,
        window_end=request.window_end,
        rows=rows,
        seed=request.seed,
        parents=parents,
        log=log,
    )
    envelopes = [to_envelope(row, pins=pins) for row in baseline]
    return BasePlan(
        request=request,
        rows=baseline,
        plan=plan,
        parents=parents,
        factor=_factor(bundle, inputs, baseline, request),
        cancel=_cancel_share(bundle, inputs, request.window_start),
        sequence=merge_sequence(_group(envelopes)),
    )


def clip_commands(
    commands: list[ScenarioCommand], floor: datetime
) -> list[ScenarioCommand]:
    """Commands that still have time left after ``floor``.

    A command that started earlier begins at ``floor``. Events that
    went out before ``floor`` cannot change.
    """
    kept: list[ScenarioCommand] = []
    for command in commands:
        start = datetime.fromisoformat(command.start)
        end = start + parse_duration(command.duration)
        begin = max(start, floor.astimezone(start.tzinfo))
        left = (end - begin).total_seconds()
        if left <= 0:
            continue
        kept.append(
            command.model_copy(
                update={
                    "start": begin.isoformat(),
                    "duration": f"PT{int(left)}S",
                }
            )
        )
    return kept


def build_fork(
    base: BasePlan,
    bundle: ModelBundle,
    policy: ScenarioPolicy,
    commands: list[ScenarioCommand],
    *,
    pins: PinBook | None = None,
    from_time: datetime | None = None,
) -> tuple[Sequence, list[dict[str, Any]], dict[str, Any]]:
    """Fork envelopes, fork rows, and the effect summary.

    With ``from_time`` the fork covers only the baseline after that
    time, and each command starts no earlier than it.
    """
    rows = base.rows
    applied = commands
    if from_time is not None:
        floor = from_time.astimezone(bundle.spec.timezone).isoformat()
        rows = [row for row in rows if row["timestamp"] >= floor]
        applied = clip_commands(commands, from_time)
    if not applied:
        return [], [], {
            "scenario_id": commands[0].scenario_id if commands else None,
            "injected_flights": 0,
            "lifecycles": 0,
            "shift_applied": False,
        }
    fork_rows, effect = apply_fork(
        rows,
        applied,
        bundle,
        policy,
        seed=base.request.seed,
        factor=base.factor,
        cancel_share=base.cancel,
        pins=pins,
        scale=base.plan,
    )
    envelopes = [to_envelope(row, pins=pins) for row in fork_rows]
    return merge_sequence(_group(envelopes)), fork_rows, effect


def emit_plan(
    bundle: ModelBundle,
    inputs: Any,
    request: ReplayRequest,
    *,
    mode: str,
    rows: int,
    policy: ScenarioPolicy | None = None,
    seek: str | None = None,
    clock: Clock | None = None,
    pins: PinBook | None = None,
    resolved: Any = None,
) -> Emission:
    if mode not in MODES:
        raise ValueError(f"unknown mode {mode}")
    if resolved is None:
        from flowstate.config.resolve import resolve_models

        policy = policy or load_policy()
        resolved = resolve_models(
            policy,
            bundle.config,
            source="memory",
            version_id=None,
            field_sources={
                name: "default" for name in type(bundle.config).model_fields
            },
        )
    else:
        policy = resolved.model.scenario_policy
    check_commands(request.commands, policy, bundle)
    now = None
    if mode == "live":
        now = (clock or _wall_clock(bundle))().astimezone(
            bundle.spec.timezone
        )
        day = now.date().isoformat()
        request = replace(
            request, window_start=day, window_end=day, acceleration=1.0
        )
    base = build_base_plan(bundle, inputs, request, rows, pins=pins)
    fork_seq: list[tuple[str, dict[str, Any]]] = []
    fork: list[dict[str, Any]] = []
    effect: dict[str, Any] = {
        "scenario_id": None,
        "injected_flights": 0,
        "lifecycles": 0,
        "shift_applied": False,
    }
    if request.commands:
        fork_seq, fork, effect = build_fork(
            base, bundle, policy, request.commands, pins=pins
        )
    envelopes = [event for _, event in base.sequence]
    envelopes.extend(event for _, event in fork_seq)
    if now is not None:
        envelopes = [
            event
            for event in envelopes
            if datetime.fromisoformat(event["event_time"]) >= now
        ]
    if seek is not None:
        floor = datetime.fromisoformat(seek)
        envelopes = [
            event
            for event in envelopes
            if datetime.fromisoformat(event["event_time"]) >= floor
        ]
    by_topic = _group(envelopes)
    failures = order_failures(by_topic)
    sequence = merge_sequence(by_topic)
    pace, effective = _pace_for(mode, sequence, request, now)
    plan = base.plan
    audit = [
        {
            "record_type": "start",
            "mode": mode,
            "replay_request": request.as_dict(),
            "rows": rows,
            "seek": seek,
            "bundle_version": bundle.version,
            "bundle_hash": bundle_hash(bundle, resolved.config_hash),
            "seed": request.seed,
            "config_hash": resolved.config_hash,
            "config_version": {
                kind: resolved.version_id(kind) for kind in resolved.kinds
            },
            "coverage": {
                "window": bundle.coverage.get("window"),
                "airport_code": bundle.coverage.get("airport_code"),
            },
            "parameters": _parameters(bundle),
            "plan": {
                "budgets": plan["budgets"],
                "frozen": plan["frozen"],
                "rows": plan["rows"],
            },
            "amplification_factor": base.factor,
            "extrapolation": _extrapolation(bundle, request, plan),
        },
        {
            "record_type": "stop",
            "counts": conservation_report(base.rows, base.parents),
            "qa_failures": failures,
            "scenario": effect,
            "output_manifest": _manifest(envelopes),
            "pace": {
                "requested_acceleration": request.acceleration,
                "effective_acceleration": effective,
                "max_eps": request.max_eps,
            },
            "pins_missing": (
                None if pins is None else _pins_missing(envelopes)
            ),
        },
    ]
    return Emission(
        by_topic=by_topic,
        audit=audit,
        qa_failures=failures,
        pace_seconds=pace,
        effective_acceleration=effective,
        baseline_rows=base.rows,
        fork_rows=fork,
        plan=plan,
    )


def send_batches(
    producer: Any,
    by_topic: dict[str, list[dict[str, Any]]],
    audit: list[dict[str, Any]],
    commands: list[dict[str, Any]],
    *,
    batch_size: int = BATCH_SIZE,
    pace: list[float] | None = None,
    sleeper: Callable[[float], None] | None = None,
    clock: Clock | None = None,
) -> None:
    """Publish in one global event_time order, in bounded batches.

    ``flush`` is the backpressure point. ``pace`` is one wait per merged
    event from ``pace_delays``. The cap drops no event. A missing
    sleeper is a no-op, for tests. ``emitted_at`` is set here, at send
    time, in every mode. Success is logged only after every send is
    acked.
    """
    now = clock or (lambda: datetime.now().astimezone())
    sequence = merge_sequence(by_topic)
    delays = pace or []
    pending = 0
    queued = 0
    futures: list[Any] = []
    total = len(sequence) + len(commands) + len(audit)
    wait = sleeper or (lambda _seconds: None)
    log.info("queueing %s messages across %s topics", total, len(by_topic))

    def _queue(topic: str, key: str, value: dict[str, Any]) -> None:
        nonlocal pending, queued
        futures.append(producer.send(topic, key=key, value=value))
        pending += 1
        queued += 1
        if queued % 20_000 == 0 or queued == total:
            log.info("queued %s/%s topic %s key %s", queued, total, topic, key)
        if pending >= batch_size:
            producer.flush()
            pending = 0

    for topic in by_topic:
        log.info("topic %s events %s", topic, len(by_topic[topic]))
    for command in commands:
        _queue(COMMAND_TOPIC, command["scenario_id"], command)
        log.info(
            "command %s %s",
            command.get("scenario_id"),
            command.get("scenario_type"),
        )
    if audit:
        _queue(AUDIT_TOPIC, "audit-0", audit[0])
        log.info("audit %s", audit[0].get("record_type"))
    owed = 0.0
    for index, (topic, event) in enumerate(sequence):
        if index < len(delays) and delays[index] > 0:
            # sleep() cannot wait less than about a millisecond. Add up
            # the short waits so the cap holds on average.
            owed += delays[index]
            if owed >= MIN_SLEEP_SECONDS:
                wait(owed)
                owed = 0.0
        event["emitted_at"] = now().isoformat()
        _queue(topic, event["event_id"], event)
    for offset, record in enumerate(audit[1:], start=1):
        _queue(AUDIT_TOPIC, f"audit-{offset}", record)
        log.info("audit %s", record.get("record_type"))
    producer.flush()
    acked, failed = _acks(futures)
    if failed:
        log.error(
            "publish failed acked=%s failed=%s first=%s",
            acked,
            len(failed),
            failed[0],
        )
        raise SystemExit(
            f"publish failed acked={acked} failed={len(failed)}"
        )
    log.info("publish complete acked=%s", acked)


def refresh_live(
    emission: Emission, request: ReplayRequest, now: datetime
) -> Emission:
    """Drop events that went past while the plan was built.

    Planning takes seconds. Live waits for the wall clock, so the
    cut and the first wait use the clock at send time.
    """
    kept = {
        topic: [
            event
            for event in events
            if datetime.fromisoformat(event["event_time"]) >= now
        ]
        for topic, events in emission.by_topic.items()
    }
    kept = {topic: events for topic, events in kept.items() if events}
    sequence = merge_sequence(kept)
    pace, effective = _pace_for(
        "live", sequence, replace(request, acceleration=1.0), now
    )
    return replace(
        emission,
        by_topic=kept,
        pace_seconds=pace,
        effective_acceleration=effective,
    )


def _acks(futures: list[Any]) -> tuple[int, list[BaseException]]:
    """Broker acks. ``flush`` only waits. It does not report failures."""
    acked = 0
    failed: list[BaseException] = []
    for future in futures:
        try:
            future.get(timeout=30)
        except Exception as exc:
            failed.append(exc)
        else:
            acked += 1
    return acked, failed


def _parse(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="flowstate.streaming.producer")
    parser.add_argument("--mode", choices=MODES, default=None)
    parser.add_argument("--events-per-sec", type=float, default=None)
    parser.add_argument("--rows", type=int, default=None)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--window-start", default=None)
    parser.add_argument("--window-end", default=None)
    parser.add_argument("--acceleration", type=float, default=None)
    parser.add_argument("--seek", default=None)
    parser.add_argument("--command", action="append", default=[])
    parser.add_argument(
        "--request",
        default=None,
        help="path to a replay request JSON (the demo API writes it)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="build the plan and skip the broker",
    )
    return parser.parse_args(argv)


def _settings_from(args: argparse.Namespace) -> dict[str, Any]:
    """CLI flags over the request file over the defaults."""
    values: dict[str, Any] = {
        "mode": "replay",
        "events_per_sec": 500.0,
        "rows": 10_000,
        "seed": 42,
        "window_start": None,
        "window_end": None,
        "acceleration": 60.0,
        "seek": None,
        "commands": [],
    }
    if args.request:
        raw = json.loads(Path(args.request).read_text())
        for key, dest in (
            ("mode", "mode"),
            ("max_eps", "events_per_sec"),
            ("rows", "rows"),
            ("seed", "seed"),
            ("window_start", "window_start"),
            ("window_end", "window_end"),
            ("acceleration", "acceleration"),
            ("seek", "seek"),
            ("commands", "commands"),
        ):
            if key in raw and raw[key] is not None:
                values[dest] = raw[key]
    for name in (
        "mode",
        "events_per_sec",
        "rows",
        "seed",
        "window_start",
        "window_end",
        "acceleration",
        "seek",
    ):
        flag = getattr(args, name)
        if flag is not None:
            values[name] = flag
    if args.command:
        values["commands"] = [json.loads(item) for item in args.command]
    return values


def main(argv: list[str] | None = None) -> None:
    configure_logging()
    args = _parse(argv)
    wanted = _settings_from(args)
    settings = get_settings()
    log.info(
        "producer mode=%s seed=%s rows=%s max_eps=%s bootstrap=%s",
        wanted["mode"],
        wanted["seed"],
        wanted["rows"],
        wanted["events_per_sec"],
        settings.kafka_bootstrap_servers,
    )
    require_station_reviews(settings.data_dir, DEFAULT_CITY_ID)
    log.info("loading inputs and geography pins")
    inputs = load_inputs(settings)
    pins = load_pin_book(settings)
    log.info(
        "pins wards=%s stations=%s stops=%s airports=%s",
        len(pins.wards),
        len(pins.stations),
        len(pins.stops),
        len(pins.airports),
    )
    spec = city_spec(load(DEFAULT_CITY_ID))
    from flowstate.config.resolve import FileConfigSource

    resolved = FileConfigSource(settings).load()
    log.info("fitting bundle")
    bundle = build_bundle(
        inputs,
        spec,
        resolved.model.resample_config,
        seed=wanted["seed"],
        config_hash=resolved.config_hash,
    )
    window_start = wanted["window_start"] or spec.window_start.isoformat()
    window_end = wanted["window_end"] or window_start
    commands = [
        ScenarioCommand.model_validate(item) for item in wanted["commands"]
    ]
    request = ReplayRequest(
        bundle_version=bundle.version,
        seed=wanted["seed"],
        window_start=window_start,
        window_end=window_end,
        acceleration=wanted["acceleration"],
        max_eps=wanted["events_per_sec"],
        commands=commands,
    )
    if wanted["mode"] == "live":
        log.info("planning today on the wall clock rows=%s", wanted["rows"])
    else:
        log.info(
            "planning window %s..%s rows=%s",
            window_start,
            window_end,
            wanted["rows"],
        )
    emission = emit_plan(
        bundle,
        inputs,
        request,
        mode=wanted["mode"],
        rows=wanted["rows"],
        seek=wanted["seek"],
        pins=pins,
        resolved=resolved,
    )
    log.info("plan budgets %s", emission.plan["budgets"])
    log.info("plan rows %s", emission.plan["rows"])
    if emission.plan["frozen"]["ride_hail"]:
        log.info(
            "ride-hail days frozen at a source day: %s",
            len(emission.plan["frozen"]["ride_hail"]),
        )
    for topic, events in sorted(emission.by_topic.items()):
        log.info("planned %s %s", topic, len(events))
    log.info(
        "plan bundle=%s qa_failures=%s pins_missing=%s",
        bundle.version,
        len(emission.qa_failures),
        emission.audit[1]["pins_missing"],
    )
    if args.dry_run:
        if emission.qa_failures:
            raise SystemExit(1)
        return
    if wanted["mode"] == "live":
        emission = refresh_live(
            emission, request, _wall_clock(bundle)()
        )
        log.info(
            "live: %s events left after planning",
            sum(len(events) for events in emission.by_topic.values()),
        )
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient

    bootstrap = ipv4_bootstrap(settings.kafka_bootstrap_servers)
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    try:
        described = admin.describe_topics(list(v1_topics()))
        require_one_partition(partition_counts(described), v1_topics())
    finally:
        admin.close()
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        key_serializer=lambda key: key.encode(),
        value_serializer=lambda value: json.dumps(value).encode(),
    )
    try:
        send_batches(
            producer,
            emission.by_topic,
            emission.audit,
            [command.model_dump() for command in commands],
            pace=emission.pace_seconds,
            sleeper=time.sleep,
            clock=_wall_clock(bundle),
        )
    finally:
        producer.close()
    if emission.qa_failures:
        raise SystemExit(1)


def _wall_clock(bundle: ModelBundle) -> Clock:
    zone = bundle.spec.timezone

    def clock() -> datetime:
        return datetime.now(zone)

    return clock


def _cancel_share(bundle: ModelBundle, inputs: Any, day: str) -> float:
    """Cancel share of finished rides on ``day``, frozen when absent."""
    known = ride_lookup(inputs, bundle)
    key = day if day in known else frozen_source(list(known), day)
    if key is None:
        return 0.0
    metrics = known[key][0]
    ends = metrics["done_ride"] + metrics["cancel_ride"]
    if ends <= 0:
        return 0.0
    return metrics["cancel_ride"] / ends


def _factor(
    bundle: ModelBundle,
    inputs: Any,
    baseline: list[dict[str, Any]],
    request: ReplayRequest,
) -> float:
    """ADR-0016: ride-hail rows per day over the mean city-day total."""
    observed = observed_activity_per_day(
        inputs.city_day,
        bundle.spec.window_start,
        bundle.spec.window_end,
    )
    ride_rows = sum(1 for row in baseline if row["mode"] == "ride_hail")
    span = window_days(request.window_start, request.window_end)
    return amplification_factor(ride_rows / max(len(span), 1), observed)


def _extrapolation(
    bundle: ModelBundle, request: ReplayRequest, plan: dict[str, Any]
) -> dict[str, Any]:
    outside = any(outside_window(bundle, day) for day in plan["days"])
    return {
        "trend": "frozen_at_terminal" if outside else "in_window",
        "frozen_ride_hail_days": plan["frozen"]["ride_hail"],
        "frozen_metro_days": plan["frozen"]["metro"],
        "uncertainty_widen": (
            bundle.config.outside_widen if outside else 0.0
        ),
    }


def _parameters(bundle: ModelBundle) -> dict[str, Any]:
    config = bundle.config
    return {
        "bus_prior": {
            "FLOWSTATE_ASSUMED_WEEKDAY_BOARDINGS": (
                config.assumed_weekday_boardings
            ),
            "FLOWSTATE_ASSUMED_BUS_CAPACITY": config.assumed_bus_capacity,
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
            "evidence_class": "assumption_driven",
            "note": (
                "A model choice (ADR-0012). The boardings are synthetic "
                "and are not BMTC counts."
            ),
        },
        "outside_widen": config.outside_widen,
    }


def _pins_missing(envelopes: list[dict[str, Any]]) -> int:
    missing = 0
    for event in envelopes:
        place = (event.get("location_ref") or {}).get("at") or {}
        if place.get("latitude") is None or place.get("longitude") is None:
            missing += 1
    return missing


def _group(
    envelopes: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for event in envelopes:
        topic = _topic_of(event)
        grouped.setdefault(topic, []).append(event)
    for events in grouped.values():
        events.sort(
            key=lambda event: (event["event_time"], event["event_id"])
        )
    return grouped


def _topic_of(event: dict[str, Any]) -> str:
    if event["mode"] == "aviation":
        return AVIATION_TOPICS[event["direction"]]
    topic = TOPIC_BY_MODE[event["mode"]]
    if topic is None:
        raise ValueError(f"no topic for {event['mode']}")
    return topic


def _pace_for(
    mode: str,
    sequence: list[tuple[str, dict[str, Any]]],
    request: ReplayRequest,
    now: datetime | None,
) -> tuple[list[float], float | None]:
    """One wait per merged event. Live waits for the wall clock."""
    if mode == "historical":
        return [], None
    times = [event["event_time"] for _, event in sequence]
    delays, effective = pace_delays(
        times, request.acceleration, request.max_eps
    )
    if mode == "live" and times and now is not None:
        first = datetime.fromisoformat(times[0])
        delays[0] = max(0.0, (first - now).total_seconds())
    return delays, effective


def _manifest(envelopes: list[dict[str, Any]]) -> dict[str, Any]:
    ids = sorted(event["event_id"] for event in envelopes)
    digest = hashlib.sha256("\n".join(ids).encode()).hexdigest()
    return {"rows": len(ids), "event_ids_sha256": digest}


if __name__ == "__main__":
    main()
