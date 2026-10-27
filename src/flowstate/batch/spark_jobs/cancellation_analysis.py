"""Cancellation root-cause (driver vs customer, by time/zone/vehicle)."""

from __future__ import annotations

import logging

from flowstate.config import get_settings
from flowstate.log import configure_logging

log = logging.getLogger(__name__)


def main() -> None:
    configure_logging()
    settings = get_settings()
    log.info("TODO: cancellation analysis from %s", settings.staged_dir)


if __name__ == "__main__":
    main()
