"""Q2 cancel rate. No driver-versus-rider split."""

from __future__ import annotations

import logging

from pyspark.sql import functions as F

from flowstate.batch.spark_jobs.analytics_common import (
    clock,
    corpus,
    parse_args,
    session,
    stamp_run,
    window_of,
    write_partition,
)
from flowstate.log import configure_logging

log = logging.getLogger(__name__)
TABLE = "analytics_cancellation_analysis"


def main() -> None:
    configure_logging()
    args = parse_args()
    spark = session("flowstate-cancellation-analysis")
    base = clock(corpus(spark)).where(F.col("mode") == "ride_hail")
    window = window_of(base)
    grouped = base.groupBy(
        F.col("pickup_zone").alias("ward_id"),
        "hour_of_day",
        "day_type",
        "vehicle_type",
    ).agg(
        F.count("*").alias("bookings"),
        F.sum(
            F.when(F.col("status") == "cancelled", F.lit(1)).otherwise(F.lit(0))
        ).alias("cancels"),
    )
    rates = grouped.withColumn(
        "cancel_rate",
        F.when(F.col("bookings") == 0, F.lit(None)).otherwise(
            F.col("cancels") / F.col("bookings")
        ),
    )
    path = write_partition(
        stamp_run(rates, args, window), TABLE, args["run-id"]
    )
    spark.stop()
    log.info("wrote %s", path)


if __name__ == "__main__":
    main()
