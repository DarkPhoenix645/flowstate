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

from pydantic import BaseModel, ConfigDict, Field

from flowstate.config import get_settings
from flowstate.log import configure_logging
from flowstate.normalize.cities import DEFAULT_CITY_ID
from flowstate.normalize.places.review import require_station_reviews
from flowstate.resample import run_amplify

log = logging.getLogger(__name__)


class AmplifyRequest(BaseModel):
    """The amplify command. Not the producer replay request."""

    model_config = ConfigDict(extra="forbid")

    rows: int = Field(default=1_000_000, ge=1)
    seed: int = 42


def parse_amplify(argv: list[str] | None = None) -> AmplifyRequest:
    parser = argparse.ArgumentParser(prog="flowstate.ingest.amplify")
    parser.add_argument(
        "--rows",
        type=int,
        default=AmplifyRequest.model_fields["rows"].default,
        help="multimodal events (default %(default)s)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=AmplifyRequest.model_fields["seed"].default,
        help="generator seed (default %(default)s)",
    )
    args = parser.parse_args(argv)
    return AmplifyRequest(rows=args.rows, seed=args.seed)


def main(argv: list[str] | None = None) -> None:
    configure_logging()
    request = parse_amplify(argv)
    settings = get_settings()
    # Station reviews gate event generation: metro events carry
    # station ids, so an unresolved station pin stops the run.
    require_station_reviews(settings.data_dir, DEFAULT_CITY_ID)
    manifest = run_amplify(settings, request.rows, request.seed)
    log.info(
        "amplified %s events to %s/rides_amplified/ seed=%s bundle=%s",
        manifest["rows"],
        settings.staged_dir,
        request.seed,
        manifest["bundle_version"],
    )
    for mode, count in sorted(manifest["mode_counts"].items()):
        log.info("mode %s %s", mode, count)
    for status, count in sorted(manifest["provenance_counts"].items()):
        log.info("provenance %s %s", status, count)


if __name__ == "__main__":
    main()
