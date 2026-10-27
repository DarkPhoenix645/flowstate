"""Fit and record the frozen resampler evaluation.

``make evaluate`` runs the held-out protocol (ADR-0010) and writes
the record twice: beside the corpus for the run, and to the
committed results directory as review evidence. The fit is
deterministic, so the committed record always rebuilds.
"""

from __future__ import annotations

import argparse
import logging

from flowstate.config import get_settings
from flowstate.log import configure_logging
from flowstate.resample.writer import COMMITTED_EVALUATION, write_evaluation

log = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> None:
    configure_logging()
    parser = argparse.ArgumentParser(prog="flowstate.resample.evaluate")
    parser.parse_args(argv)
    evaluation = write_evaluation(get_settings())
    ride = evaluation["ride_hail"]["aviation_terms"]
    metro = evaluation["metro"]["aviation_terms"]
    log.info("evaluation written to %s", COMMITTED_EVALUATION)
    log.info(
        "ride_hail aviation terms kept: %s (improvement %s)",
        ride.get("keep_aviation_terms"),
        ride.get("improvement"),
    )
    log.info(
        "metro aviation terms kept: %s (improvement %s)",
        metro.get("keep_aviation_terms"),
        metro.get("improvement"),
    )
    log.info(
        "copula rho kept: %s (rho %.4f)",
        evaluation["copula"]["kept"],
        evaluation["copula"]["rho"],
    )


if __name__ == "__main__":
    main()
