"""Publish one Kafka topic. The v1 topic set is ADR-0008."""

from __future__ import annotations

import argparse

from flowstate.config import get_settings


def main() -> None:
    parser = argparse.ArgumentParser(prog="flowstate.streaming.producer")
    parser.add_argument("--topic", default="flowstate.ridehail.activity.v1")
    parser.add_argument("--events-per-sec", type=float, default=500)
    args = parser.parse_args()
    settings = get_settings()
    print(
        f"TODO: publish {settings.staged_dir} → {args.topic} "
        f"@ {args.events_per_sec} eps ({settings.kafka_bootstrap_servers})"
    )


if __name__ == "__main__":
    main()
