"""python -m flowstate.lake

Puts staged ride-hail parquet on HDFS. Leaves the host files in place.
Does not run Hive DDL. Does not write rides_amplified.
"""

from __future__ import annotations

import sys

from flowstate.config import get_settings
from flowstate.lake.ridehail import put_ridehail


def main() -> None:
    try:
        uris = put_ridehail(get_settings())
    except FileNotFoundError as exc:
        print(exc, file=sys.stderr)
        sys.exit(1)
    for uri in uris:
        print(uri)


if __name__ == "__main__":
    main()
