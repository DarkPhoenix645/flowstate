"""Publish one Kafka topic. The v1 topic set is ADR-0008.

The publish loop, scenario engine, and audit topic are ticket #39.
This module already imports the one resampler
(``flowstate.resample``) for its envelope generation so no second
generator can grow here: events come from the model bundle, mapped
by ``flowstate.resample.envelope.to_envelope``.
"""

from __future__ import annotations

import argparse
import json

from flowstate.config import get_settings
from flowstate.normalize.cities import DEFAULT_CITY_ID, load
from flowstate.normalize.places.review import require_station_reviews
from flowstate.resample import build_bundle, city_spec, load_inputs, to_envelope


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="flowstate.streaming.producer")
    parser.add_argument("--topic", default="flowstate.ridehail.activity.v1")
    parser.add_argument("--events-per-sec", type=float, default=500)
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args(argv)
    settings = get_settings()
    require_station_reviews(settings.data_dir, DEFAULT_CITY_ID)
    inputs = load_inputs(settings)
    bundle = build_bundle(
        inputs, city_spec(load(DEFAULT_CITY_ID)), seed=args.seed
    )
    print(
        f"producer plan (emit loop is #39): {args.rows} events at "
        f"{args.events_per_sec} eps from bundle {bundle.version} "
        f"(seed {args.seed}, {settings.kafka_bootstrap_servers}); "
        f"target topic {args.topic}"
    )
    sample = to_envelope(
        {
            "trip_id": "sample",
            "mode": "ride_hail",
            "event_type": "completed_ride",
            "timestamp": "2026-10-10T18:00:00+05:30",
            "count": 1,
            "pickup_zone": "b_112",
            "scenario_id": None,
            "provenance_status": "synthetic",
            "source_ids": "namma_yatri_city_day",
            "model_bundle_version": bundle.version,
            "seed_id": bundle.seed_id,
            "evidence_grade": "aggregate_calibrated",
            "outside_training_window": "false",
            "synthetic_fare_inr": None,
        }
    )
    print(f"sample envelope:\n{json.dumps(sample, indent=2)}")


if __name__ == "__main__":
    main()
