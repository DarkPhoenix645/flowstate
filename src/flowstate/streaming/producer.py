"""Publish one Kafka topic. The v1 topic set is ADR-0008."""

from __future__ import annotations

import argparse

from flowstate.config import get_settings
from flowstate.normalize.cities import DEFAULT_CITY_ID
from flowstate.normalize.places.review import require_station_reviews


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="flowstate.streaming.producer")
    parser.add_argument("--topic", default="flowstate.ridehail.activity.v1")
    parser.add_argument("--events-per-sec", type=float, default=500)
    args = parser.parse_args(argv)
    settings = get_settings()
    require_station_reviews(settings.data_dir, DEFAULT_CITY_ID)
    print(
        f"TODO: publish {settings.staged_dir} → {args.topic} "
        f"@ {args.events_per_sec} eps ({settings.kafka_bootstrap_servers})"
    )


if __name__ == "__main__":
    main()
