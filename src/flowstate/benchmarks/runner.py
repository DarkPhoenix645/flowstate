"""Timed Spark vs MapReduce at 1M/10M/100M."""

from __future__ import annotations

import logging
from pathlib import Path

from flowstate.log import configure_logging

log = logging.getLogger(__name__)
RESULTS = Path("src/flowstate/benchmarks/results/timings.csv")


def main() -> None:
    configure_logging()
    log.info("TODO: time Spark vs MR; append rows to %s", RESULTS)


if __name__ == "__main__":
    main()
