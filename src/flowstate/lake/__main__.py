"""python -m flowstate.lake

Puts staged ride-hail parquet on HDFS. Leaves the host files in place.
Does not run Hive DDL. Does not write rides_amplified.
"""

from __future__ import annotations

import logging
import sys

from flowstate.config import get_settings
from flowstate.lake.ridehail import put_ridehail
from flowstate.log import configure_logging

log = logging.getLogger(__name__)


def main() -> None:
    configure_logging()
    try:
        uris = put_ridehail(get_settings())
    except FileNotFoundError as exc:
        log.error("%s", exc)
        sys.exit(1)
    for uri in uris:
        log.info("%s", uri)


if __name__ == "__main__":
    main()
