"""Synthetic scale-up to 1M / 10M / 100M."""

from __future__ import annotations

import argparse

from flowstate.config import get_settings
from flowstate.normalize.cities import DEFAULT_CITY_ID
from flowstate.normalize.places.review import require_station_reviews


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="flowstate.ingest.amplify")
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args(argv)
    settings = get_settings()
    require_station_reviews(settings.data_dir, DEFAULT_CITY_ID)
    settings.ensure_dirs()
    print(
        f"TODO: amplify seed rows in {settings.staged_dir} "
        f"to {args.rows} (seed={args.seed}) → rides_amplified parquet"
    )


if __name__ == "__main__":
    main()
