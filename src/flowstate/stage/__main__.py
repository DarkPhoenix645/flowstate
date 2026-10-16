"""python -m flowstate.stage --dataset ridehail|metro|bus|aviation|all

Reads the cold extracts. Does not download. Does not call OpenSky.
``aviation`` here is the DGCA monthly table only.
"""

from __future__ import annotations

import argparse
import sys

from flowstate.config import get_settings
from flowstate.stage.bus import stage_bus
from flowstate.stage.dgca import stage_dgca
from flowstate.stage.metro import stage_metro
from flowstate.stage.ridehail import stage_ridehail

STAGES = {
    "ridehail": stage_ridehail,
    "metro": stage_metro,
    "bus": stage_bus,
    "aviation": stage_dgca,
}


def main() -> None:
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
        print(f"stage:{name}", flush=True)
        summary = STAGES[name](settings)
        rows = summary.get("rows", summary.get("kept_rows"))
        print(f"stage:{name} rows={rows}", flush=True)
    sys.exit(0)


if __name__ == "__main__":
    main()
