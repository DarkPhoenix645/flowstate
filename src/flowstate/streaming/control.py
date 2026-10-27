"""Control messages for the long-running producer (ADR-0017).

One topic carries them: ``flowstate.simulation.control.v1``. Scenario
commands keep their own topic, ``flowstate.scenario.command.v1``
(ADR-0008). The service answers each message with a ``control`` record
on the audit topic. The record repeats ``request_id``.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from flowstate.resample.scenario import ScenarioCommand


class StartRequest(BaseModel):
    """A replay request plus the size of the plan."""

    model_config = ConfigDict(extra="forbid")

    mode: Literal["replay", "live"] = "replay"
    rows: int = Field(default=10_000, ge=1_000, le=2_000_000)
    seed: int = 42
    window_start: str | None = None
    window_end: str | None = None
    acceleration: float = Field(default=60.0, gt=0)
    max_eps: float = Field(default=500.0, gt=0)
    seek: str | None = None
    commands: list[ScenarioCommand] = Field(default_factory=list)
    session_id: str | None = None


class ControlMessage(BaseModel):
    """One operation on the running replay.

    * ``start`` begins a session. It ends the one that runs.
    * ``pause`` and ``resume`` hold and release the play head.
    * ``speed`` sets ``acceleration``. A live session ignores it.
    * ``seek`` moves the play head to ``to``. A move backwards starts a
      new ``run_id``.
    * ``stop`` ends the session. ``shutdown`` ends the process.
    """

    model_config = ConfigDict(extra="forbid")

    op: Literal[
        "start", "pause", "resume", "speed", "seek", "stop", "shutdown"
    ]
    request_id: str | None = None
    start: StartRequest | None = None
    acceleration: float | None = Field(default=None, gt=0)
    to: str | None = None


def start_default(name: str) -> object:
    """The one copy of a replay default. CLIs read it. They do not repeat it."""
    return StartRequest.model_fields[name].default


def default_window(
    spec_start: str,
    window_start: str | None,
    window_end: str | None,
) -> tuple[str, str]:
    """One day at the spec start when the request leaves the window empty."""
    start = window_start or spec_start
    return start, window_end or start


def control_json_schema() -> dict:
    schema = ControlMessage.model_json_schema()
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    return schema
