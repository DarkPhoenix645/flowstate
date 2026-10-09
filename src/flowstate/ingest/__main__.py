"""python -m flowstate.ingest"""

from __future__ import annotations

import argparse
import logging
import sys

from flowstate.config import get_settings
from flowstate.ingest import (
    bmrcl_rti,
    bmtc_gtfs,
    dgca_aviation,
    kaggle_rides,
    manifest,
    namma_ridership,
    namma_yatri,
    opensky_arrivals,
    significant_dates,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)

# One modality is one or more loaders. `all` is every modality.
# `kaggle` stays available and is not part of `all`: those seeds are generated.
MODALITIES: dict[str, list] = {
    "ridehail": [namma_yatri.run],
    "metro": [namma_ridership.run, bmrcl_rti.run],
    "bus": [bmtc_gtfs.run],
    "aviation": [dgca_aviation.run, opensky_arrivals.run],
    "events": [significant_dates.run],
    "kaggle": [kaggle_rides.run],
}


def run_all(dataset: str) -> None:
    settings = get_settings()
    settings.ensure_dirs()
    if dataset == "validate":
        sys.exit(manifest.validate())
    if dataset == "manifest":
        manifest.refresh()
        return
    if dataset == "all":
        names = ["ridehail", "metro", "bus", "events", "aviation"]
    else:
        names = [dataset]
    unknown = [name for name in names if name not in MODALITIES]
    if unknown:
        raise SystemExit(
            f"unknown dataset(s): {unknown}; choose from "
            f"{['all', 'validate', 'manifest', *MODALITIES]}"
        )
    code = 0
    for name in names:
        print(f"ingest:{name}", flush=True)
        for loader in MODALITIES[name]:
            try:
                loader(settings)
            except SystemExit as exc:
                code = int(exc.code or 1)
                break
        if code:
            break
    manifest.refresh()
    if code:
        raise SystemExit(code)


def main() -> None:
    parser = argparse.ArgumentParser(prog="flowstate.ingest")
    parser.add_argument(
        "--dataset",
        default="all",
        help=(
            "all | ridehail | metro | bus | aviation | events | "
            "manifest | validate | kaggle"
        ),
    )
    args = parser.parse_args()
    run_all(args.dataset)


if __name__ == "__main__":
    main()
