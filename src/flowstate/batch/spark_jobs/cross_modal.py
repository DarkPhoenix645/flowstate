"""Q4 ride-hail versus metro, and Q5 flights versus ride-hail.

Both are associations. Neither is a cause. Q5 is not a prediction:
the covariate test needs 60 shared covered dates, and the extract has
42 arrival days and 38 departure days.
"""

from __future__ import annotations

import logging

from pyspark.sql import functions as F

from flowstate.batch.association import weekday_adjusted_slope
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
NOTE = (
    "GTFS is not joined. The day grain does not use stop or route keys. "
    "No row was dropped for a missing GTFS key. "
    "On the amplified corpus the dependence is the resampler fit, "
    "not new evidence about Bengaluru."
)
Q5 = (
    "Not a prediction. ADR-0017 needs 60 shared covered dates. "
    "The extract has 42 arrival covered days and 38 departure covered days."
)


def main() -> None:
    configure_logging()
    args = parse_args()
    spark = session("flowstate-cross-modal")
    base = clock(corpus(spark))
    window = window_of(base)
    ride = base.where(F.col("mode") == "ride_hail").groupBy("service_date").agg(
        F.count("*").alias("ridehail")
    )
    metro = base.where(F.col("mode") == "metro").groupBy("service_date").agg(
        (
            F.sum(F.coalesce(F.col("entries_count"), F.lit(0)))
            + F.sum(F.coalesce(F.col("exits_count"), F.lit(0)))
        ).alias("metro")
    )
    air = base.where(F.col("mode") == "aviation").groupBy("service_date").agg(
        F.count("*").alias("flights")
    )
    joined = ride.join(metro, "service_date", "inner")
    days = [
        (
            float(row.ridehail),
            float(row.metro),
            1 if row.service_date.weekday() < 5 else 0,
            str(row.service_date),
        )
        for row in joined.collect()
        if row.service_date is not None
    ]
    fit = weekday_adjusted_slope([(y, x, flag) for y, x, flag, _day in days])
    metro_rows = [
        (
            day,
            "day",
            fit["n"],
            fit["estimate"],
            fit["interval_low"],
            fit["interval_high"],
            NOTE,
        )
        for _y, _x, _flag, day in days
    ]
    metro_frame = spark.createDataFrame(
        metro_rows,
        [
            "period_start",
            "grain",
            "n",
            "estimate",
            "interval_low",
            "interval_high",
            "note",
        ],
    )
    flight_days = [
        (
            float(row.ridehail),
            float(row.flights),
            1 if row.service_date.weekday() < 5 else 0,
            str(row.service_date),
        )
        for row in ride.join(air, "service_date", "inner").collect()
        if row.service_date is not None
    ]
    flight_fit = weekday_adjusted_slope(
        [(y, x, flag) for y, x, flag, _day in flight_days]
    )
    flight_rows = [
        (
            day,
            flight_fit["n"],
            flight_fit["estimate"],
            flight_fit["interval_low"],
            flight_fit["interval_high"],
            42,
            38,
            60,
            NOTE + " " + Q5,
        )
        for _y, _x, _flag, day in flight_days
    ]
    flight_frame = spark.createDataFrame(
        flight_rows,
        [
            "period_start",
            "n",
            "estimate",
            "interval_low",
            "interval_high",
            "arrival_covered_days",
            "departure_covered_days",
            "required_covered_days",
            "note",
        ],
    )
    metro_path = write_partition(
        stamp_run(metro_frame, args, window),
        "analytics_ridehail_metro",
        args["run-id"],
    )
    flight_path = write_partition(
        stamp_run(flight_frame, args, window),
        "analytics_flights_ridehail",
        args["run-id"],
    )
    spark.stop()
    log.info("wrote %s and %s", metro_path, flight_path)
    log.info("%s", NOTE)
    log.info("%s", Q5)


if __name__ == "__main__":
    main()
