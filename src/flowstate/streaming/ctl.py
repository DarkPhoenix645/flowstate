"""Send one control message or command to the running producer.

The demo API sends the same messages. This module is the one publisher.
The service answers on the audit topic.
"""

from __future__ import annotations

import argparse
import json
import logging
import uuid

from flowstate.config import get_settings
from flowstate.log import configure_logging
from flowstate.resample.scenario import ScenarioCommand
from flowstate.streaming.control import (
    ControlMessage,
    StartRequest,
    start_default,
)
from flowstate.streaming.topics import (
    COMMAND_TOPIC,
    CONTROL_TOPIC,
    ipv4_bootstrap,
)

log = logging.getLogger(__name__)


def build(argv: list[str] | None = None) -> tuple[str, str, dict]:
    """Return (topic, key, value) for the arguments."""
    parser = argparse.ArgumentParser(prog="flowstate.streaming.ctl")
    sub = parser.add_subparsers(dest="op", required=True)
    start = sub.add_parser("start")
    start.add_argument(
        "--mode",
        choices=("replay", "live"),
        default=start_default("mode"),
        help="replay or live (default %(default)s)",
    )
    start.add_argument(
        "--rows",
        type=int,
        default=start_default("rows"),
        help="events in the plan (default %(default)s)",
    )
    start.add_argument(
        "--seed",
        type=int,
        default=start_default("seed"),
        help="generator seed (default %(default)s)",
    )
    start.add_argument("--window-start")
    start.add_argument("--window-end")
    start.add_argument(
        "--acceleration",
        type=float,
        default=start_default("acceleration"),
        help="event-time acceleration (default %(default)s)",
    )
    start.add_argument(
        "--events-per-sec",
        "--max-eps",
        dest="max_eps",
        type=float,
        default=start_default("max_eps"),
        help="event cap (default %(default)s)",
    )
    start.add_argument("--seek")
    start.add_argument("--session-id")
    start.add_argument("--command", action="append", default=[])
    for name in ("pause", "resume", "stop", "shutdown"):
        sub.add_parser(name)
    speed = sub.add_parser("speed")
    speed.add_argument("acceleration", type=float)
    seek = sub.add_parser("seek")
    seek.add_argument("to", help="event_time to seek to, ISO with offset")
    command = sub.add_parser("command")
    command.add_argument("json", help="one scenario command as JSON")
    args = parser.parse_args(argv)
    request_id = uuid.uuid4().hex[:8]
    if args.op == "command":
        value = ScenarioCommand.model_validate_json(args.json)
        return COMMAND_TOPIC, value.scenario_id, value.model_dump()
    fields: dict = {"op": args.op, "request_id": request_id}
    if args.op == "start":
        fields["start"] = StartRequest(
            mode=args.mode,
            rows=args.rows,
            seed=args.seed,
            window_start=args.window_start,
            window_end=args.window_end,
            acceleration=args.acceleration,
            max_eps=args.max_eps,
            seek=args.seek,
            session_id=args.session_id,
            commands=[
                ScenarioCommand.model_validate_json(item)
                for item in args.command
            ],
        )
    elif args.op == "speed":
        fields["acceleration"] = args.acceleration
    elif args.op == "seek":
        fields["to"] = args.to
    message = ControlMessage(**fields)
    return (
        CONTROL_TOPIC,
        request_id,
        json.loads(message.model_dump_json(exclude_none=True)),
    )


def publish(topic: str, key: str, value: dict) -> None:
    """Send one record. The CLI and the API both use this."""
    from kafka import KafkaProducer

    settings = get_settings()
    producer = KafkaProducer(
        bootstrap_servers=ipv4_bootstrap(settings.kafka_bootstrap_servers),
        key_serializer=lambda item: item.encode(),
        value_serializer=lambda item: json.dumps(item).encode(),
    )
    try:
        producer.send(topic, key=key, value=value).get(timeout=30)
    finally:
        producer.close()


def send_control(message: ControlMessage) -> str:
    """Publish one control message. Return its request id."""
    request_id = message.request_id or uuid.uuid4().hex[:8]
    payload = json.loads(
        message.model_copy(update={"request_id": request_id}).model_dump_json(
            exclude_none=True
        )
    )
    publish(CONTROL_TOPIC, request_id, payload)
    return request_id


def send_command(command: ScenarioCommand) -> None:
    """Publish one scenario command."""
    publish(
        COMMAND_TOPIC,
        command.scenario_id,
        json.loads(command.model_dump_json()),
    )


def main(argv: list[str] | None = None) -> None:
    configure_logging()
    topic, key, value = build(argv)
    publish(topic, key, value)
    log.info("sent to %s: %s", topic, value.get("op", "command"))


if __name__ == "__main__":
    main()
