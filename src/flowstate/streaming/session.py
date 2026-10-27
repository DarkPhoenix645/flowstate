"""One replay session. No Kafka, no threads, no sleeping (ADR-0017).

A session owns the events still to send and the play head. The service
asks for the next step, waits ``delay`` seconds, and commits it. Control
changes (pause, speed, seek, a new command) change what the next step
is. They never skip an event or send one twice.

``run_id`` names one replay epoch. A seek backwards starts a new epoch,
because the topics already hold later events. A consumer reads the
newest ``run_id`` and treats the others as history.
"""

from __future__ import annotations

import bisect
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from flowstate.resample.bundle import ModelBundle
from flowstate.resample.pins import PinBook
from flowstate.resample.scenario import (
    ScenarioCommand,
    ScenarioPolicy,
    check_commands,
)
from flowstate.streaming.control import StartRequest
from flowstate.streaming.producer import (
    BasePlan,
    build_fork,
    merge_sequence,
)

Clock = Callable[[], datetime]
RECENT = 500


@dataclass
class Step:
    kind: str  # "event", "paused", or "done"
    topic: str | None = None
    event: dict[str, Any] | None = None
    delay: float = 0.0


class ReplaySession:
    def __init__(
        self,
        bundle: ModelBundle,
        base: BasePlan,
        policy: ScenarioPolicy,
        start: StartRequest,
        *,
        session_id: str,
        pins: PinBook | None,
        clock: Clock,
        from_time: datetime,
    ) -> None:
        self.bundle = bundle
        self.base = base
        self.policy = policy
        self.pins = pins
        self.clock = clock
        self.session_id = session_id
        self.live = start.mode == "live"
        self.acceleration = 1.0 if self.live else start.acceleration
        self.max_eps = start.max_eps
        self.commands: list[ScenarioCommand] = []
        self.epoch = 0
        self.state = "playing"
        self.effect: dict[str, Any] = {}
        self.sent = 0
        self.playhead = from_time
        self._base_times = [event["event_time"] for _, event in base.sequence]
        self._seq: list[tuple[str, dict[str, Any]]] = []
        self._cursor = 0
        self._boundary: set[str] = set()
        self._recent: deque[tuple[float, float]] = deque(maxlen=RECENT)
        if start.commands:
            check_commands(start.commands, policy, bundle)
            self.commands = list(start.commands)
        self._rebuild(from_time)

    @property
    def run_id(self) -> str:
        return f"{self.session_id}.{self.epoch}"

    @property
    def events_left(self) -> int:
        return len(self._seq) - self._cursor

    # ---- stepping -----------------------------------------------------

    def step(self) -> Step:
        if self.state == "paused":
            return Step("paused")
        if self.state in ("done", "stopped"):
            return Step("done")
        if self._cursor >= len(self._seq):
            self.state = "done"
            return Step("done")
        topic, event = self._seq[self._cursor]
        due = datetime.fromisoformat(event["event_time"])
        if self.live:
            delay = max(0.0, (due - self.clock()).total_seconds())
        else:
            gap = max(0.0, (due - self.playhead).total_seconds())
            delay = max(gap / self.acceleration, 1.0 / self.max_eps)
        return Step("event", topic, event, delay)

    def commit(self) -> tuple[str, dict[str, Any]]:
        """Take the step that ``step`` returned. Move the play head."""
        topic, event = self._seq[self._cursor]
        self._cursor += 1
        due = datetime.fromisoformat(event["event_time"])
        gap = max(0.0, (due - self.playhead).total_seconds())
        if not self.live:
            self._recent.append(
                (gap, max(gap / self.acceleration, 1.0 / self.max_eps))
            )
        if due != self.playhead:
            self._boundary = set()
        self._boundary.add(event["event_id"])
        self.playhead = max(due, self.playhead)
        self.sent += 1
        return topic, event

    def effective_acceleration(self) -> float | None:
        if self.live:
            return 1.0
        wall = sum(delay for _, delay in self._recent)
        if wall <= 0:
            return None
        return sum(gap for gap, _ in self._recent) / wall

    # ---- control ------------------------------------------------------

    def pause(self) -> None:
        if self.state == "playing":
            self.state = "paused"

    def resume(self) -> None:
        if self.state == "paused":
            self.state = "playing"

    def stop(self) -> None:
        self.state = "stopped"

    def set_acceleration(self, value: float) -> None:
        if self.live:
            raise ValueError("a live session runs on the wall clock")
        self.acceleration = value

    def seek(self, target: datetime) -> bool:
        """Move the play head. Returns True when a new epoch started."""
        target = target.astimezone(self.bundle.spec.timezone)
        backwards = target < self.playhead
        if backwards:
            self.epoch += 1
        self._boundary = set()
        self.playhead = target
        self._recent.clear()
        if self.state in ("done", "stopped"):
            self.state = "playing"
        self._rebuild(target)
        return backwards

    def add_command(self, command: ScenarioCommand) -> dict[str, Any]:
        """Add a command. Its effect starts at the play head, not before.

        The fork stream is a full copy of the baseline after that point.
        Events already sent stay as they are.
        """
        check_commands([*self.commands, command], self.policy, self.bundle)
        self.commands.append(command)
        self._rebuild(self.playhead)
        return dict(self.effect)

    # ---- plan ---------------------------------------------------------

    def _rebuild(self, from_time: datetime) -> None:
        floor = from_time.astimezone(self.bundle.spec.timezone).replace(
            microsecond=0
        ).isoformat()
        start = bisect.bisect_left(self._base_times, floor)
        remaining = self.base.sequence[start:]
        fork_seq: list[tuple[str, dict[str, Any]]] = []
        self.effect = {}
        if self.commands:
            fork_seq, _, self.effect = build_fork(
                self.base,
                self.bundle,
                self.policy,
                self.commands,
                pins=self.pins,
                from_time=from_time,
            )
        merged = merge_sequence(_by_topic([*remaining, *fork_seq]))
        if self._boundary:
            merged = [
                pair
                for pair in merged
                if pair[1]["event_id"] not in self._boundary
            ]
        self._seq = merged
        self._cursor = 0


def _by_topic(
    pairs: list[tuple[str, dict[str, Any]]],
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for topic, event in pairs:
        grouped.setdefault(topic, []).append(event)
    return grouped
