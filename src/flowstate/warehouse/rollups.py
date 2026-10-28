"""Build the #27 rollups from the amplified corpus. Panels do not scan it."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from flowstate.log import configure_logging

log = logging.getLogger(__name__)
CORPUS = "hdfs://namenode:9000/lake/rides_amplified"
WAREHOUSE = "hdfs://namenode:9000/user/hive/warehouse"


def main() -> None:
    from pyspark.sql import SparkSession

    configure_logging()
    spark = (
        SparkSession.builder.appName("flowstate-rollups")
        .config("spark.sql.session.timeZone", "Asia/Kolkata")
        .getOrCreate()
    )
    frame = spark.read.option("pathGlobFilter", "part-*.parquet").parquet(
        CORPUS
    )
    dated = frame.withColumn("service_date", F.substring("timestamp", 1, 10))
    _write(_ridehail_day(dated), "rollup_ridehail_day")
    _write(_ridehail_hour(dated), "rollup_ridehail_hour")
    _write(_metro_station(dated), "rollup_metro_station_hour")
    _write(_metro_day(dated), "rollup_metro_day")
    _write(_bus_hour(dated), "rollup_bus_hour")
    _write(_aviation_day(dated), "rollup_aviation_day")
    log.info("wrote the six rollups under %s", WAREHOUSE)
    spark.stop()


def _ridehail_day(frame: DataFrame) -> DataFrame:
    ride = frame.where(F.col("mode") == "ride_hail")
    return ride.groupBy("service_date").agg(
        _events("booking_created").alias("bookings"),
        _events("completed_ride").alias("completions"),
        _events("cancelled_ride").alias("cancels"),
    )


def _events(name: str):
    return F.sum(F.when(F.col("event_type") == name, 1).otherwise(0))


def _ridehail_hour(frame: DataFrame) -> DataFrame:
    ride = frame.where(
        (F.col("mode") == "ride_hail")
        & (F.col("event_type") == "booking_created")
    )
    return ride.groupBy("hour").agg(F.count("*").alias("bookings"))


def _metro_station(frame: DataFrame) -> DataFrame:
    metro = frame.where(F.col("mode") == "metro")
    return metro.groupBy(
        F.col("pickup_zone").alias("station_id"),
        "hour",
        "provenance_status",
    ).agg(
        F.sum(F.coalesce(F.col("entries_count"), F.lit(0))).alias(
            "entries_count"
        ),
        F.sum(F.coalesce(F.col("exits_count"), F.lit(0))).alias(
            "exits_count"
        ),
    )


def _metro_day(frame: DataFrame) -> DataFrame:
    metro = frame.where(F.col("mode") == "metro")
    return metro.groupBy("service_date", "provenance_status").agg(
        F.sum(
            F.coalesce(F.col("entries_count"), F.lit(0))
            + F.coalesce(F.col("exits_count"), F.lit(0))
        ).alias("volume")
    )


def _bus_hour(frame: DataFrame) -> DataFrame:
    bus = frame.where(F.col("mode") == "bus")
    return bus.groupBy("hour").agg(
        F.sum(F.coalesce(F.col("boardings"), F.lit(0))).alias("boardings"),
        F.sum(F.coalesce(F.col("alightings"), F.lit(0))).alias("alightings"),
        F.max("onboard_leaving").alias("peak_onboard_leaving"),
        F.avg("onboard_leaving").alias("mean_onboard_leaving"),
    )


def _aviation_day(frame: DataFrame) -> DataFrame:
    air = frame.where(F.col("mode") == "aviation")
    return air.groupBy(
        "service_date",
        F.col("event_type").alias("direction"),
        "provenance_status",
    ).agg(F.count("*").alias("events"))


def _write(frame: DataFrame, name: str) -> None:
    (
        frame.write.mode("overwrite")
        .parquet(f"{WAREHOUSE}/{name}")
    )


if __name__ == "__main__":
    main()
