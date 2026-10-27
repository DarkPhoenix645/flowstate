"""Demand heatmaps by zone/hour. Submit on the Compose Spark cluster."""

from __future__ import annotations

import logging

from flowstate.config import get_settings
from flowstate.log import configure_logging

log = logging.getLogger(__name__)


def main() -> None:
    configure_logging()
    settings = get_settings()
    log.info(
        "TODO: demand hotspots from %s (master=%s)",
        settings.staged_dir,
        settings.spark_master,
    )


if __name__ == "__main__":
    main()
