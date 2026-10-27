"""One replay session. The API starts the producer and sends control."""

from __future__ import annotations

import subprocess
import sys
import time
import uuid
from collections.abc import Callable
from typing import Any

from flowstate.resample.scenario import ScenarioCommand
from flowstate.streaming.control import ControlMessage, StartRequest

OFFLINE_AFTER = 10.0
_CONTROL = frozenset({"pause", "resume", "speed", "seek", "stop"})


class ReplayBusy(Exception):
    """A replay session is already running."""


class ReplayGate:
    """The one producer child and the session it serves."""

    def __init__(
        self,
        sender: Callable[[ControlMessage], str] | None = None,
        commander: Callable[[ScenarioCommand], None] | None = None,
        starter: Callable[[], Any] | None = None,
        reader: Any | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        from flowstate.streaming.ctl import send_command, send_control

        self.sender = sender or send_control
        self.commander = commander or send_command
        self.starter = starter or _start_service
        self.reader = reader if reader is not None else _KafkaAudit()
        self.clock = clock or time.monotonic
        self.process: Any | None = None
        self.run_id: str | None = None

    def busy(self) -> bool:
        return self.run_id is not None and self._alive()

    def ensure(self) -> None:
        if self._alive():
            return
        self.process = self.starter()
        self.run_id = None

    def begin(self, run_id: str, start: StartRequest) -> None:
        if self.busy():
            raise ReplayBusy("a replay is already running")
        self.ensure()
        self.run_id = run_id
        message = ControlMessage(
            op="start",
            start=start.model_copy(update={"session_id": run_id}),
        )
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if not self._alive():
                raise RuntimeError("producer exited")
            request_id = self.sender(message)
            ack = self.reader.wait_request(request_id, timeout=5)
            if ack and ack.get("accepted"):
                return
        raise RuntimeError("producer did not accept start")

    def control(self, run_id: str, message: ControlMessage) -> dict[str, Any]:
        self._require(run_id)
        if message.op not in _CONTROL:
            from flowstate.config.resolve import ConfigValidationError

            raise ConfigValidationError(
                "control accepts pause, resume, speed, seek, or stop"
            )
        request_id = self.sender(message)
        ack = self.reader.wait_request(request_id, timeout=15)
        if message.op == "stop" and ack.get("accepted"):
            self.run_id = None
        return _answer(ack)

    def command(self, run_id: str, command: ScenarioCommand) -> dict[str, Any]:
        self._require(run_id)
        forget = getattr(self.reader, "forget_command", None)
        if forget is not None:
            forget(command.scenario_id)
        self.commander(command)
        ack = self.reader.wait_command(command.scenario_id, timeout=15)
        answer = _answer(ack)
        answer["qa_failure"] = answer.get("accepted") is False
        return answer

    def view(self, run_id: str) -> dict[str, Any]:
        record = self.reader.latest_status(run_id)
        if record is None:
            return {"state": "producer_offline"}
        age = self.clock() - float(record.get("seen_at", 0))
        if age > OFFLINE_AFTER:
            stale = dict(record)
            stale["state"] = "producer_offline"
            return stale
        return record

    def shutdown(self) -> None:
        if not self._alive():
            self.process = None
            self.run_id = None
            return
        try:
            self.sender(ControlMessage(op="shutdown"))
        except Exception:
            pass
        try:
            self.process.wait(timeout=20)
        except Exception:
            self.process.kill()
        self.process = None
        self.run_id = None

    def _require(self, run_id: str) -> None:
        if self.run_id != run_id:
            raise ReplayBusy("that run is not the live session")

    def _alive(self) -> bool:
        return self.process is not None and self.process.poll() is None


_GATE: ReplayGate | None = None


def process_gate() -> ReplayGate:
    global _GATE
    if _GATE is None:
        _GATE = ReplayGate()
    return _GATE


def _start_service() -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [sys.executable, "-m", "flowstate.streaming.service"]
    )


def _answer(ack: dict[str, Any] | None) -> dict[str, Any]:
    if not ack:
        return {"accepted": False, "state": "producer_offline"}
    kept = {
        key: ack.get(key)
        for key in (
            "accepted",
            "state",
            "playhead",
            "acceleration",
            "effective_acceleration",
            "reason",
            "sent",
            "acked",
            "failed",
        )
        if key in ack
    }
    kept.setdefault("accepted", False)
    return kept


class _KafkaAudit:
    """Tail the audit topic. Tests pass a fake reader instead."""

    def __init__(self) -> None:
        self._status: dict[str, dict[str, Any]] = {}
        self._requests: dict[str, dict[str, Any]] = {}
        self._commands: dict[str, dict[str, Any]] = {}
        self._started = False

    def forget_command(self, scenario_id: str) -> None:
        self._commands.pop(scenario_id, None)

    def latest_status(self, run_id: str) -> dict[str, Any] | None:
        self._drain()
        return self._status.get(run_id)

    def wait_request(
        self, request_id: str, timeout: float
    ) -> dict[str, Any] | None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self._drain()
            found = self._requests.get(request_id)
            if found is not None:
                return found
            time.sleep(0.05)
        return None

    def wait_command(
        self, scenario_id: str, timeout: float
    ) -> dict[str, Any] | None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self._drain()
            found = self._commands.get(scenario_id)
            if found is not None:
                return found
            time.sleep(0.05)
        return None

    def _drain(self) -> None:
        if not self._started:
            self._start()
        assert self._consumer is not None
        for record in self._consumer.poll(timeout_ms=200).values():
            for item in record:
                self._take(item.value)

    def _start(self) -> None:
        from kafka import KafkaConsumer

        from flowstate.config import get_settings
        from flowstate.streaming.topics import AUDIT_TOPIC, ipv4_bootstrap

        settings = get_settings()
        self._consumer = KafkaConsumer(
            AUDIT_TOPIC,
            bootstrap_servers=ipv4_bootstrap(settings.kafka_bootstrap_servers),
            group_id=f"flowstate-api-{uuid.uuid4().hex[:8]}",
            auto_offset_reset="latest",
            value_deserializer=lambda raw: __import__("json").loads(raw),
            consumer_timeout_ms=200,
        )
        self._started = True

    def _take(self, value: Any) -> None:
        if not isinstance(value, dict):
            return
        value = dict(value)
        value["seen_at"] = time.monotonic()
        kind = value.get("record_type")
        session = str(value.get("session_id") or "")
        if kind == "status" and session:
            self._status[session] = value
        request_id = value.get("request_id")
        if kind == "control" and request_id:
            self._requests[str(request_id)] = value
        command = value.get("command") or {}
        if kind == "control" and value.get("op") == "command":
            scenario_id = command.get("scenario_id")
            if scenario_id:
                self._commands[str(scenario_id)] = value
