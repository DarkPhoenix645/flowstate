"""Synthetic scale-up to 1M / 10M / 100M.

ADR-0006: calls the calibrated resampler (ADR-0010). ``N`` is the
count of multimodal events (all modes), written as part files to
``data/staged/rides_amplified/``. ``--seed`` is deterministic and
the run records it. The resampler is one module
(``flowstate.resample``); this CLI is a thin wrapper, never a
second generator.
"""

from __future__ import annotations

import argparse
import logging

from flowstate.config import get_settings
from flowstate.log import configure_logging
from flowstate.normalize.cities import DEFAULT_CITY_ID
from flowstate.normalize.places.review import require_station_reviews
from flowstate.resample import run_amplify

log = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> None:
    configure_logging()
    parser = argparse.ArgumentParser(prog="flowstate.ingest.amplify")
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args(argv)
    settings = get_settings()
    # Station reviews gate event generation: metro events carry
    # station ids, so an unresolved station pin stops the run.
    require_station_reviews(settings.data_dir, DEFAULT_CITY_ID)
    manifest = run_amplify(settings, args.rows, args.seed)
    log.info(
        "amplified %s events to %s/rides_amplified/ seed=%s bundle=%s",
        manifest["rows"],
        settings.staged_dir,
        args.seed,
        manifest["bundle_version"],
    )
    for mode, count in sorted(manifest["mode_counts"].items()):
        log.info("mode %s %s", mode, count)
    for status, count in sorted(manifest["provenance_counts"].items()):
        log.info("provenance %s %s", status, count)


if __name__ == "__main__":
    main()
