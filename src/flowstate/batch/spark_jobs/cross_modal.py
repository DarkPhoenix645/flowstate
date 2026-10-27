"""Metro ridership vs ride-hailing demand correlation."""

from __future__ import annotations

import logging

from flowstate.config import get_settings
from flowstate.log import configure_logging

log = logging.getLogger(__name__)


def main() -> None:
    configure_logging()
    settings = get_settings()
    log.info("TODO: cross-modal correlation from %s", settings.staged_dir)


if __name__ == "__main__":
    main()
