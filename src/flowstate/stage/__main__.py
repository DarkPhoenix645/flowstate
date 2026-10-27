"""python -m flowstate.stage --dataset ridehail|metro|bus|aviation|all

Reads the cold extracts. Does not download. Does not call OpenSky.
``aviation`` here is the DGCA monthly table only.
"""

from __future__ import annotations

import argparse
import logging
import sys

from flowstate.config import get_settings
from flowstate.log import configure_logging
from flowstate.stage.bus import stage_bus
from flowstate.stage.dgca import stage_dgca
from flowstate.stage.metro import stage_metro
from flowstate.stage.ridehail import stage_ridehail

log = logging.getLogger(__name__)

STAGES = {
    "ridehail": stage_ridehail,
    "metro": stage_metro,
    "bus": stage_bus,
    "aviation": stage_dgca,
}


def main() -> None:
    configure_logging()
    parser = argparse.ArgumentParser(prog="flowstate.stage")
    parser.add_argument(
        "--dataset",
        default="all",
        choices=["all", *STAGES],
        help="all | ridehail | metro | bus | aviation (DGCA only, not OpenSky)",
    )
    args = parser.parse_args()
    settings = get_settings()
    settings.ensure_dirs()
    names = list(STAGES) if args.dataset == "all" else [args.dataset]
    for name in names:
        log.info("stage:%s", name)
        summary = STAGES[name](settings)
        rows = summary.get("rows", summary.get("kept_rows"))
        log.info("stage:%s rows=%s", name, rows)
    sys.exit(0)


if __name__ == "__main__":
    main()
