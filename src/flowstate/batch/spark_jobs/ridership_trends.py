"""Q3 metro and aviation trends. Payment split only when the source has it."""

from __future__ import annotations

import logging

from pyspark.sql import functions as F

from flowstate.batch.spark_jobs.analytics_common import (
    WAREHOUSE,
    clock,
    corpus,
    exists,
    parse_args,
    session,
    stamp_run,
    window_of,
    write_partition,
)
from flowstate.log import configure_logging

log = logging.getLogger(__name__)
TABLE = "analytics_ridership_trends"
OPEN = "2025-08-11"
METRO_DAILY = f"{WAREHOUSE}/metro_daily"


def main() -> None:
    configure_logging()
    args = parse_args()
    spark = session("flowstate-ridership-trends")
    base = clock(corpus(spark))
    window = window_of(base)
    metro = base.where(F.col("mode") == "metro").withColumn(
        "volume",
        F.coalesce(F.col("entries_count"), F.lit(0))
        + F.coalesce(F.col("exits_count"), F.lit(0)),
    ).withColumn(
        "line_phase",
        F.when(
            F.col("service_date") < F.lit(OPEN), F.lit("before_open")
        ).otherwise(F.lit("after_open")),
    )
    day = metro.groupBy(F.col("service_date").alias("period_start")).agg(
        F.sum("volume").alias("volume")
    ).withColumn("grain", F.lit("day")).withColumn("mode", F.lit("metro"))
    month = metro.groupBy(
        F.trunc("service_date", "month").alias("period_start")
    ).agg(F.sum("volume").alias("volume")).withColumn(
        "grain", F.lit("month")
    ).withColumn("mode", F.lit("metro"))
    station = metro.groupBy(
        F.col("pickup_zone").alias("station_id"),
        "hour_of_day",
        "line_phase",
    ).agg(F.sum("volume").alias("volume"))
    totals = station.groupBy("station_id", "line_phase").agg(
        F.sum("volume").alias("station_total"),
        F.max("volume").alias("peak_volume"),
    )
    peak = station.join(totals, ["station_id", "line_phase"]).where(
        F.col("volume") == F.col("peak_volume")
    ).withColumn(
        "peak_share",
        F.when(F.col("station_total") == 0, F.lit(None)).otherwise(
            F.col("volume") / F.col("station_total")
        ),
    ).withColumn("grain", F.lit("station_peak")).withColumn(
        "mode", F.lit("metro")
    ).withColumn("period_start", F.lit(None).cast("date"))
    air = base.where(F.col("mode") == "aviation").groupBy(
        F.col("service_date").alias("period_start")
    ).agg(F.count("*").alias("volume")).withColumn(
        "grain", F.lit("day")
    ).withColumn("mode", F.lit("aviation"))
    # Align columns. Payment rows are added when metro_daily is on HDFS.
    common = ["grain", "period_start", "mode", "volume", "station_id",
              "peak_share", "payment_method", "line_phase"]
    frames = []
    for item in (day, month, air):
        frames.append(
            item.withColumn("station_id", F.lit(None).cast("string"))
            .withColumn("peak_share", F.lit(None).cast("double"))
            .withColumn("payment_method", F.lit(None).cast("string"))
            .withColumn("line_phase", F.lit(None).cast("string"))
            .select(*common)
        )
    frames.append(
        peak.withColumn(
            "payment_method", F.lit(None).cast("string")
        ).select(*common)
    )
    if exists(spark, METRO_DAILY):
        daily = spark.read.parquet(METRO_DAILY)
        for name in ("smart_cards", "tokens", "qr", "ncmc"):
            if name not in daily.columns:
                continue
            pay = daily.select(
                F.to_date("record_date").alias("period_start"),
                F.col(name).cast("double").alias("volume"),
            ).withColumn("grain", F.lit("day")).withColumn(
                "mode", F.lit("metro")
            ).withColumn("station_id", F.lit(None).cast("string")).withColumn(
                "peak_share", F.lit(None).cast("double")
            ).withColumn("payment_method", F.lit(name)).withColumn(
                "line_phase", F.lit(None).cast("string")
            ).select(*common)
            frames.append(pay)
        log.info("payment split from %s", METRO_DAILY)
    else:
        log.info("metro_daily is absent; payment_method stays null")
    out = frames[0]
    for item in frames[1:]:
        out = out.unionByName(item)
    path = write_partition(
        stamp_run(out, args, window), TABLE, args["run-id"]
    )
    spark.stop()
    log.info("wrote %s", path)


if __name__ == "__main__":
    main()
