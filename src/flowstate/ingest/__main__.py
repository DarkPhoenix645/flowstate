"""python -m flowstate.ingest"""

from __future__ import annotations

import argparse

from flowstate.config import get_settings
from flowstate.ingest import (
    bmrc_ridership,
    bmtc_gtfs,
    dgca_aviation,
    kaggle_rides,
    mmrda_ridership,
)

LOADERS = {
    "kaggle": kaggle_rides.run,
    "bmrc": bmrc_ridership.run,
    "bmtc": bmtc_gtfs.run,
    "mmrda": mmrda_ridership.run,
    "dgca": dgca_aviation.run,
}


def run_all(dataset: str) -> None:
    settings = get_settings()
    settings.ensure_dirs()
    names = list(LOADERS) if dataset == "all" else [dataset]
    unknown = [n for n in names if n not in LOADERS]
    if unknown:
        raise SystemExit(
            f"unknown dataset(s): {unknown}; choose from {list(LOADERS)}"
        )
    for name in names:
        print(f"ingest:{name}")
        LOADERS[name](settings)


def main() -> None:
    parser = argparse.ArgumentParser(prog="flowstate.ingest")
    parser.add_argument(
        "--dataset",
        default="all",
        help="all|kaggle|bmrc|bmtc|mmrda|dgca",
    )
    args = parser.parse_args()
    run_all(args.dataset)


if __name__ == "__main__":
    main()
