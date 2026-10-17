"""Publish the two staged sample envelopes and read them back.

There is no DGCA topic and no enrichment topic.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from kafka import KafkaConsumer, KafkaProducer, TopicPartition

from flowstate.stage.events import BUS_TOPIC, METRO_TOPIC

SAMPLES = (
    (METRO_TOPIC, Path("data/staged/metro/station_hour_event.json")),
    (BUS_TOPIC, Path("data/staged/bmtc/scheduled_stop_event.json")),
)


def publish_samples(bootstrap: str) -> list[dict[str, Any]]:
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda value: json.dumps(value).encode(),
    )
    sent: list[dict[str, Any]] = []
    try:
        for topic, path in SAMPLES:
            payload = json.loads(path.read_text())
            meta = producer.send(topic, payload).get(timeout=30)
            sent.append(
                {
                    "topic": topic,
                    "partition": meta.partition,
                    "offset": meta.offset,
                    "event_id": payload["event_id"],
                    "provenance": payload["provenance"]["status"],
                }
            )
        producer.flush()
    finally:
        producer.close()
    return sent


def read_back(bootstrap: str, sent: dict[str, Any]) -> dict[str, Any]:
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap,
        value_deserializer=lambda raw: json.loads(raw.decode()),
        consumer_timeout_ms=15000,
        enable_auto_commit=False,
    )
    try:
        part = TopicPartition(sent["topic"], sent["partition"])
        consumer.assign([part])
        consumer.seek(part, sent["offset"])
        message = next(consumer)
    finally:
        consumer.close()
    return message.value
