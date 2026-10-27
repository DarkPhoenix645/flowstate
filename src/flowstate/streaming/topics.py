"""Create the ADR-0008 topics. One partition each.

``make kafka-topics`` is the only creator. The producer refuses any
other partition count. Do not rely on broker auto-create. #17 uses
this target.
"""

from __future__ import annotations

import logging
from typing import Any

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from flowstate.config import get_settings
from flowstate.log import configure_logging
from flowstate.resample.envelope import AVIATION_TOPICS, TOPIC_BY_MODE

COMMAND_TOPIC = "flowstate.scenario.command.v1"
AUDIT_TOPIC = "flowstate.simulation.audit.v1"
ALREADY_EXISTS = TopicAlreadyExistsError.errno
log = logging.getLogger(__name__)


def ipv4_bootstrap(servers: str) -> str:
    """Use 127.0.0.1 for localhost.

    ``localhost`` resolves to ``::1`` first. Compose publishes Kafka
    on ``127.0.0.1`` only, so the IPv6 dial is connection refused.
    """
    rewritten = []
    for item in servers.split(","):
        host, sep, port = item.strip().rpartition(":")
        if host in ("localhost", "::1"):
            host = "127.0.0.1"
        rewritten.append(f"{host}{sep}{port}" if sep else item.strip())
    return ",".join(rewritten)


def v1_topics() -> tuple[str, ...]:
    names = [topic for topic in TOPIC_BY_MODE.values() if topic]
    names.extend(AVIATION_TOPICS.values())
    names.append(COMMAND_TOPIC)
    names.append(AUDIT_TOPIC)
    return tuple(names)


def topic_specs() -> list[NewTopic]:
    return [
        NewTopic(name=name, num_partitions=1, replication_factor=1)
        for name in v1_topics()
    ]


def partition_counts(described: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in described:
        name = str(item["topic"])
        if item.get("error_code"):
            counts[name] = 0
            continue
        counts[name] = len(item.get("partitions") or [])
    return counts


def require_one_partition(
    counts: dict[str, int], topics: tuple[str, ...]
) -> None:
    bad = [name for name in topics if counts.get(name) != 1]
    if bad:
        raise SystemExit(
            "refusing to produce; partition count is not 1 for "
            + ", ".join(bad)
        )


def create_error_codes(response: Any) -> list[tuple[str, int]]:
    errors = getattr(response, "topic_errors", ()) or ()
    pairs: list[tuple[str, int]] = []
    for item in errors:
        if isinstance(item, dict):
            pairs.append((str(item["topic"]), int(item["error_code"])))
        else:
            pairs.append((str(item[0]), int(item[1])))
    return pairs


def unacceptable_create_errors(
    pairs: list[tuple[str, int]],
) -> list[tuple[str, int]]:
    return [
        (topic, code)
        for topic, code in pairs
        if code not in (0, ALREADY_EXISTS)
    ]


def ensure_topics(admin: KafkaAdminClient) -> None:
    response = admin.create_topics(topic_specs(), timeout_ms=10_000)
    bad = unacceptable_create_errors(create_error_codes(response))
    if bad:
        raise SystemExit(f"topic create failed: {bad}")
    described = admin.describe_topics(list(v1_topics()))
    require_one_partition(partition_counts(described), v1_topics())


def main() -> None:
    configure_logging()
    settings = get_settings()
    admin = KafkaAdminClient(
        bootstrap_servers=ipv4_bootstrap(settings.kafka_bootstrap_servers),
        api_version=(2, 6, 0),
    )
    try:
        ensure_topics(admin)
    finally:
        admin.close()
    log.info("topics ready (%s x 1 partition)", len(v1_topics()))


if __name__ == "__main__":
    main()
