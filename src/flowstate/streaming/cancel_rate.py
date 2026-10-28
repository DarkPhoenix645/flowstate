"""Cancel rate. Its own Event-Detector node. Not folded into active rides."""

from __future__ import annotations

import logging

from flowstate.log import configure_logging
from flowstate.streaming.job import events, load_args, session, write_query
from flowstate.streaming.metrics import cancel_rates

log = logging.getLogger(__name__)


def main() -> None:
    configure_logging()
    args = load_args()
    spark = session("flowstate-cancel-rate")
    frame = cancel_rates(events(spark, args))
    write_query(frame, args, "cancel_rate")
    log.info("cancel_rate wrote %s", args["sink"])
    spark.stop()


if __name__ == "__main__":
    main()
