"""Metro/aviation ridership trends + payment split."""

from __future__ import annotations

import logging

from flowstate.config import get_settings
from flowstate.log import configure_logging

log = logging.getLogger(__name__)


def main() -> None:
    configure_logging()
    settings = get_settings()
    log.info("TODO: ridership trends from %s", settings.staged_dir)


if __name__ == "__main__":
    main()
