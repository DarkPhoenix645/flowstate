"""The long-running producer (ADR-0017). ``make stream-serve``.

The process loads the inputs, the pins, and the fitted model once. It
then waits for control messages on ``flowstate.simulation.control.v1``
and scenario commands on ``flowstate.scenario.command.v1``. A replay is
a session: ``start``, ``pause``, ``resume``, ``speed``, ``seek``,
``stop``. A command that arrives during a replay forks the stream from
the play head. Each answer is a ``control`` record on the audit topic.

One thread does all the work. A second thread only reads Kafka and puts
messages in a queue. Waiting for the next event is waiting on that
queue, so a control message cuts the wait short.
"""

from __future__ import annotations

import hashlib
import json
import logging
import queue
import signal
import threading
import time
import uuid
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime
from datetime import time as dtime
from typing import Any

from pydantic import ValidationError

from flowstate.config import Settings, get_settings
from flowstate.log import configure_logging
from flowstate.normalize.cities import DEFAULT_CITY_ID, load
from flowstate.normalize.places.review import require_station_reviews
from flowstate.resample import (
    build_bundle,
    bundle_hash,
    city_spec,
    load_inputs,
)
from flowstate.resample.bundle import ModelBundle
from flowstate.resample.pins import PinBook, load_pin_book
from flowstate.resample.scenario import (
    ScenarioCommand,
    ScenarioPolicy,
    load_policy,
    scenario_policy_sha256,
)
from flowstate.streaming.control import ControlMessage, StartRequest
from flowstate.streaming.producer import (
    BasePlan,
    ReplayRequest,
    _bus_prior,
    _extrapolation,
    _wall_clock,
    build_base_plan,
    conservation_report,
)
from flowstate.streaming.session import ReplaySession
from flowstate.streaming.topics import (
    AUDIT_TOPIC,
    COMMAND_TOPIC,
    CONTROL_TOPIC,
    ipv4_bootstrap,
    partition_counts,
    require_one_partition,
    v1_topics,
)

log = logging.getLogger(__name__)
MIN_SLEEP_SECONDS = 0.002
FLUSH_EVERY = 500
PLAN_CACHE = 2
POLL_EVERY = 256
Clock = Callable[[], datetime]


@dataclass
class Runtime:
    """What the process loads once."""

    settings: Settings
    inputs: Any
    pins: PinBook
    bundle: ModelBundle
    policy: ScenarioPolicy


def load_runtime(settings: Settings) -> Runtime:
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
    log.info("fitting the model bundle once")
    bundle = build_bundle(inputs, city_spec(load(DEFAULT_CITY_ID)), seed=42)
    return Runtime(
        settings=settings,
        inputs=inputs,
        pins=pins,
        bundle=bundle,
        policy=load_policy(),
    )


class Service:
    def __init__(
        self,
        runtime: Runtime,
        producer: Any,
        *,
        inbox: queue.Queue | None = None,
        clock: Clock | None = None,
        idle_wait: float = 0.25,
        status_every: float = 1.0,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.runtime = runtime
        self.producer = producer
        self.inbox: queue.Queue = inbox or queue.Queue()
        self.clock = clock or _wall_clock(runtime.bundle)
        self.idle_wait = idle_wait
        self.status_every = status_every
        self.monotonic = monotonic
        self.session: ReplaySession | None = None
        self.quit = False
        self._owed = 0.0
        self._accounted: str | None = None
        self._since_poll = 0
        self._plans: OrderedDict[tuple, BasePlan] = OrderedDict()
        self._pending: list[Any] = []
        self._acked = 0
        self._failed: list[BaseException] = []
        self._audit_n = 0
        self._last_status = monotonic()
        self._qa: list[str] = []
        self._last_time: dict[tuple[str, str], str] = {}
        self._digest = hashlib.sha256()
        self._since_flush = 0

    # ---- loop ---------------------------------------------------------

    def run(self) -> None:
        log.info("service ready: waiting for control messages")
        while not self.quit:
            self.run_once()
        self._finish("shutdown")
        self.producer.flush()

    def run_once(self) -> None:
        session = self.session
        step = session.step() if session else None
        if step is None or step.kind in ("paused", "done"):
            if step is not None and step.kind == "done":
                self._finish("done")
            self._accounted = None
            self._wait(self.idle_wait)
            self._maybe_status()
            return
        assert session is not None and step.event is not None
        event_id = step.event["event_id"]
        if self._accounted != event_id:
            # The delay counts once. A long wait is cut into pieces so a
            # status record still goes out, and the rest stays owed.
            self._owed += step.delay
            self._accounted = event_id
        if self._owed >= MIN_SLEEP_SECONDS:
            chunk = min(self._owed, self.status_every)
            self._owed -= chunk
            if self._wait(chunk):
                return
            if self._owed >= MIN_SLEEP_SECONDS:
                self._maybe_status()
                return
        topic, event = session.commit()
        self._accounted = None
        self._send(session, topic, event)
        self._maybe_status()
        self._since_poll += 1
        if self._since_poll >= POLL_EVERY:
            # A burst with no waits must still hear a control message.
            self._since_poll = 0
            self._wait(0)

    def _wait(self, seconds: float) -> bool:
        """Wait for a message up to ``seconds``. True if one was handled."""
        try:
            item = (
                self.inbox.get_nowait()
                if seconds <= 0
                else self.inbox.get(timeout=seconds)
            )
        except queue.Empty:
            return False
        self._owed = 0.0
        self._accounted = None
        self.handle(item)
        return True

    # ---- messages -----------------------------------------------------

    def handle(self, item: tuple[str, Any]) -> None:
        kind, payload = item
        try:
            if kind == "control":
                self._control(payload)
            elif kind == "command":
                self._command(payload)
            elif kind == "invalid":
                self._ack(None, "invalid", False, payload)
        except Exception as exc:  # the loop must outlive a bad message
            log.exception("message failed: %s", kind)
            self._ack(
                getattr(payload, "request_id", None),
                kind,
                False,
                f"{type(exc).__name__}: {exc}",
            )

    def _control(self, msg: ControlMessage) -> None:
        op = msg.op
        if op == "shutdown":
            self._ack(msg.request_id, op, True)
            self.quit = True
            return
        if op == "start":
            if msg.start is None:
                raise ValueError("start needs a start request")
            self._start(msg.start, msg.request_id)
            return
        session = self.session
        if session is None:
            self._ack(msg.request_id, op, False, "no session")
            return
        if op == "pause":
            session.pause()
        elif op == "resume":
            session.resume()
        elif op == "speed":
            if msg.acceleration is None:
                raise ValueError("speed needs acceleration")
            session.set_acceleration(msg.acceleration)
        elif op == "seek":
            if session.live:
                raise ValueError("a live session cannot seek")
            if msg.to is None:
                raise ValueError("seek needs a target time")
            fresh = session.seek(datetime.fromisoformat(msg.to))
            self._last_time = {}
            self._ack(
                msg.request_id,
                op,
                True,
                extra={"new_epoch": fresh, "events_left": session.events_left},
            )
            return
        elif op == "stop":
            self._ack(msg.request_id, op, True)
            self._finish("stop")
            return
        self._ack(msg.request_id, op, True)

    def _command(self, command: ScenarioCommand) -> None:
        session = self.session
        if session is None:
            self._ack(None, "command", False, "no session", command)
            return
        try:
            effect = session.add_command(command)
        except ValueError as exc:
            self._qa.append(f"command rejected: {exc}")
            self._ack(None, "command", False, str(exc), command)
            return
        self._ack(
            None,
            "command",
            True,
            extra={
                "effect": effect,
                "events_left": session.events_left,
                "from": session.playhead.isoformat(),
            },
            command=command,
        )

    # ---- sessions -----------------------------------------------------

    def _start(self, start: StartRequest, request_id: str | None) -> None:
        if self.session is not None:
            self._finish("replaced")
        runtime = self.runtime
        bundle = replace(runtime.bundle, seed=start.seed)
        zone = bundle.spec.timezone
        live = start.mode == "live"
        now = self.clock().astimezone(zone)
        if live:
            window_start = window_end = now.date().isoformat()
            from_time = now
        else:
            window_start = (
                start.window_start or bundle.spec.window_start.isoformat()
            )
            window_end = start.window_end or window_start
            from_time = (
                datetime.fromisoformat(start.seek).astimezone(zone)
                if start.seek
                else datetime.combine(
                    datetime.fromisoformat(window_start).date(),
                    dtime(0, 0),
                    tzinfo=zone,
                )
            )
        request = ReplayRequest(
            bundle_version=bundle.version,
            seed=start.seed,
            window_start=window_start,
            window_end=window_end,
            acceleration=1.0 if live else start.acceleration,
            max_eps=start.max_eps,
            commands=list(start.commands),
        )
        key = (window_start, window_end, start.seed, start.rows)
        base = self._plans.get(key)
        if base is None:
            log.info(
                "planning %s..%s rows=%s",
                window_start,
                window_end,
                start.rows,
            )
            base = build_base_plan(
                bundle, runtime.inputs, request, start.rows, pins=runtime.pins
            )
            self._plans[key] = base
            while len(self._plans) > PLAN_CACHE:
                self._plans.popitem(last=False)
        else:
            self._plans.move_to_end(key)
            log.info("plan cache hit %s", key)
        session = ReplaySession(
            bundle,
            base,
            runtime.policy,
            start,
            session_id=start.session_id or uuid.uuid4().hex[:8],
            pins=runtime.pins,
            clock=self.clock,
            from_time=from_time,
        )
        self.session = session
        self._owed = 0.0
        self._qa = []
        self._last_time = {}
        self._digest = hashlib.sha256()
        self._audit(
            {
                "record_type": "start",
                "session_id": session.session_id,
                "run_id": session.run_id,
                "mode": start.mode,
                "replay_request": request.as_dict(),
                "rows": start.rows,
                "seek": start.seek,
                "bundle_version": bundle.version,
                "bundle_hash": bundle_hash(bundle),
                "seed": start.seed,
                "policy_sha256": scenario_policy_sha256(),
                "parameters": {"bus_prior": _bus_prior(bundle)},
                "plan": {
                    "budgets": base.plan["budgets"],
                    "frozen": base.plan["frozen"],
                    "rows": base.plan["rows"],
                },
                "amplification_factor": base.factor,
                "extrapolation": _extrapolation(bundle, request, base.plan),
                "events_to_send": session.events_left,
                "fork": session.effect,
            }
        )
        for command in start.commands:
            self.producer.send(
                COMMAND_TOPIC,
                key=command.scenario_id,
                value=command.model_dump(),
            )
        self._ack(
            request_id,
            "start",
            True,
            extra={
                "events_to_send": session.events_left,
                "from": session.playhead.isoformat(),
            },
        )

    def _finish(self, reason: str) -> None:
        session = self.session
        if session is None:
            return
        session.stop()
        self.producer.flush()
        self._reap(final=True)
        self._audit(
            {
                "record_type": "stop",
                "session_id": session.session_id,
                "run_id": session.run_id,
                "reason": reason,
                "sent": session.sent,
                "acked": self._acked,
                "failed": len(self._failed),
                "qa_failures": list(self._qa),
                "counts": conservation_report(
                    session.base.rows, session.base.parents
                ),
                "scenario": session.effect,
                "sent_ids_sha256": self._digest.hexdigest(),
            }
        )
        self.producer.flush()
        log.info(
            "session %s ended (%s): sent=%s acked=%s failed=%s",
            session.session_id,
            reason,
            session.sent,
            self._acked,
            len(self._failed),
        )
        self.session = None
        self._acked = 0
        self._failed = []

    # ---- sending ------------------------------------------------------

    def _send(
        self, session: ReplaySession, topic: str, event: dict[str, Any]
    ) -> None:
        stamp = f"{session.run_id}|{topic}"
        previous = self._last_time.get((session.run_id, topic))
        if previous is not None and event["event_time"] < previous:
            self._qa.append(
                f"{topic} {event['event_id']} at {event['event_time']} "
                f"is before {previous}"
            )
        self._last_time[(session.run_id, topic)] = event["event_time"]
        event["run_id"] = session.run_id
        event["emitted_at"] = self.clock().isoformat()
        self._digest.update(f"{stamp}|{event['event_id']}\n".encode())
        self._pending.append(
            self.producer.send(topic, key=event["event_id"], value=event)
        )
        self._since_flush += 1
        if self._since_flush >= FLUSH_EVERY:
            self.producer.flush()
            self._since_flush = 0
            self._reap()

    def _reap(self, final: bool = False) -> None:
        """Count settled sends. ``final`` waits for the rest."""
        still: list[Any] = []
        for future in self._pending:
            if not future.is_done and final:
                try:
                    future.get(timeout=30)
                except Exception:
                    pass
            if future.is_done:
                if future.succeeded():
                    self._acked += 1
                else:
                    self._failed.append(future.exception)
            else:
                still.append(future)
        self._pending = still

    def _audit(self, record: dict[str, Any]) -> None:
        self._audit_n += 1
        session = self.session.session_id if self.session else "service"
        self.producer.send(
            AUDIT_TOPIC, key=f"audit-{session}-{self._audit_n}", value=record
        )

    def _ack(
        self,
        request_id: str | None,
        op: str,
        accepted: bool,
        reason: str | None = None,
        command: ScenarioCommand | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        session = self.session
        record: dict[str, Any] = {
            "record_type": "control",
            "request_id": request_id,
            "op": op,
            "accepted": accepted,
            "reason": reason,
            "at": self.clock().isoformat(),
        }
        if session is not None:
            record.update(
                {
                    "session_id": session.session_id,
                    "run_id": session.run_id,
                    "state": session.state,
                    "playhead": session.playhead.isoformat(),
                    "acceleration": session.acceleration,
                    "effective_acceleration": session.effective_acceleration(),
                }
            )
        if command is not None:
            record["command"] = command.model_dump()
        if extra:
            record.update(extra)
        self._audit(record)
        self.producer.flush()
        log.info("control %s accepted=%s %s", op, accepted, reason or "")

    def _maybe_status(self) -> None:
        session = self.session
        if session is None:
            return
        if self.monotonic() - self._last_status < self.status_every:
            return
        self._last_status = self.monotonic()
        self._reap()
        self._audit(
            {
                "record_type": "status",
                "session_id": session.session_id,
                "run_id": session.run_id,
                "state": session.state,
                "playhead": session.playhead.isoformat(),
                "sent": session.sent,
                "acked": self._acked,
                "failed": len(self._failed),
                "events_left": session.events_left,
                "acceleration": session.acceleration,
                "effective_acceleration": session.effective_acceleration(),
            }
        )


# ---- Kafka glue --------------------------------------------------------


def _consume(
    bootstrap: str, inbox: queue.Queue, stop: threading.Event
) -> None:
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap,
        group_id=None,
        enable_auto_commit=False,
        value_deserializer=lambda raw: json.loads(raw.decode()),
    )
    parts = [TopicPartition(CONTROL_TOPIC, 0), TopicPartition(COMMAND_TOPIC, 0)]
    consumer.assign(parts)
    consumer.seek_to_end(*parts)
    log.info("reading %s and %s", CONTROL_TOPIC, COMMAND_TOPIC)
    while not stop.is_set():
        batch = consumer.poll(timeout_ms=500)
        for records in batch.values():
            for record in records:
                inbox.put(_parse(record.topic, record.value))
    consumer.close()


def _parse(topic: str, value: Any) -> tuple[str, Any]:
    try:
        if topic == CONTROL_TOPIC:
            return "control", ControlMessage.model_validate(value)
        return "command", ScenarioCommand.model_validate(value)
    except ValidationError as exc:
        first = exc.errors()[0]["msg"]
        return "invalid", f"{topic}: {exc.error_count()} errors: {first}"


def main() -> None:
    configure_logging()
    settings = get_settings()
    runtime = load_runtime(settings)
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient

    bootstrap = ipv4_bootstrap(settings.kafka_bootstrap_servers)
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    try:
        require_one_partition(
            partition_counts(admin.describe_topics(list(v1_topics()))),
            v1_topics(),
        )
    finally:
        admin.close()
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        key_serializer=lambda key: key.encode(),
        value_serializer=lambda value: json.dumps(value).encode(),
    )
    inbox: queue.Queue = queue.Queue()
    service = Service(runtime, producer, inbox=inbox)
    stop = threading.Event()
    reader = threading.Thread(
        target=_consume, args=(bootstrap, inbox, stop), daemon=True
    )
    reader.start()

    def _quit(_signum: int, _frame: Any) -> None:
        service.quit = True

    signal.signal(signal.SIGTERM, _quit)
    signal.signal(signal.SIGINT, _quit)
    try:
        service.run()
    finally:
        stop.set()
        producer.close()


if __name__ == "__main__":
    main()
