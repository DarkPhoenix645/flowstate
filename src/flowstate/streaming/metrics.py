"""Window aggregates for the three Event-Detector nodes.

Watermark and the 1-minute tumble apply only to a streaming frame.
A static frame uses the same group so tests can check the grain.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

DELAY = "1 minute"
WIDTH = "1 minute"
ZONE = "Asia/Kolkata"


def _prepared(frame: DataFrame) -> DataFrame:
    if frame.isStreaming:
        return frame.withWatermark("event_time", DELAY)
    return frame


def _replay_key(frame: DataFrame) -> list[str]:
    if "source_run_id" in frame.columns:
        return ["source_run_id"]
    return []


def active_counts(frame: DataFrame) -> DataFrame:
    """Count events. event_type stays a dimension. Not a rate."""
    grouped = _prepared(frame).groupBy(
        F.window("event_time", WIDTH),
        "ward_id",
        "provenance_status",
        "scenario_id",
        "event_type",
        *_replay_key(frame),
    ).agg(F.count("*").alias("activity_count"))
    return grouped.select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "ward_id",
        "provenance_status",
        "scenario_id",
        "event_type",
        *_replay_key(frame),
        "activity_count",
    )


def cancel_rates(frame: DataFrame) -> DataFrame:
    """Cancels over activity, per ward and window. No driver/rider split."""
    grouped = _prepared(frame).groupBy(
        F.window("event_time", WIDTH),
        "ward_id",
        "provenance_status",
        "scenario_id",
        *_replay_key(frame),
    ).agg(
        F.count("*").alias("activity_count"),
        F.sum(
            F.when(F.col("event_type") == "cancelled_ride", 1).otherwise(0)
        ).alias("cancels"),
    )
    rated = grouped.withColumn(
        "cancel_rate",
        F.when(F.col("activity_count") < 5, F.lit(None)).otherwise(
            F.col("cancels") / F.col("activity_count")
        ),
    )
    return rated.select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "ward_id",
        "provenance_status",
        "scenario_id",
        *_replay_key(frame),
        "activity_count",
        "cancels",
        "cancel_rate",
    )


def minute_bookings(frame: DataFrame) -> DataFrame:
    """1-minute booking counts. Surge sums these to the hour."""
    bookings = frame.where(F.col("event_type") == "booking_created")
    grouped = _prepared(bookings).groupBy(
        F.window("event_time", WIDTH),
        "ward_id",
        "scenario_id",
        "evidence_grade",
    ).agg(F.count("*").alias("n"))
    return grouped.select(
        "window",
        "ward_id",
        "scenario_id",
        "evidence_grade",
        "n",
    )


def hour_demand(minute_rows: DataFrame) -> DataFrame:
    """Sum 1-minute booking counts onto the Asia/Kolkata hour."""
    stamped = minute_rows.withColumn(
        "hour_start",
        F.date_trunc(
            "hour",
            F.from_utc_timestamp(F.col("window.start"), ZONE),
        ),
    )
    return stamped.groupBy(
        "hour_start", "ward_id", "scenario_id", "evidence_grade"
    ).agg(F.sum("n").alias("demand"))


def _window_cols():
    return (
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
    )


def metro_counts(frame: DataFrame) -> DataFrame:
    """Entries and exits per station-minute. Not ride-hail."""
    grouped = _prepared(frame).groupBy(
        F.window("event_time", WIDTH),
        "station_id",
        "provenance_status",
        "scenario_id",
        "evidence_grade",
        "source_run_id",
    ).agg(
        F.sum(F.coalesce(F.col("entries_count"), F.lit(0))).alias(
            "entries_count"
        ),
        F.sum(F.coalesce(F.col("exits_count"), F.lit(0))).alias(
            "exits_count"
        ),
    )
    return grouped.select(
        *_window_cols(),
        "station_id",
        "provenance_status",
        "scenario_id",
        "evidence_grade",
        "source_run_id",
        "entries_count",
        "exits_count",
    )


def bus_counts(frame: DataFrame) -> DataFrame:
    """Boardings and alightings per stop-minute."""
    grouped = _prepared(frame).groupBy(
        F.window("event_time", WIDTH),
        "stop_id",
        "provenance_status",
        "scenario_id",
        "evidence_grade",
        "source_run_id",
    ).agg(
        F.sum(F.coalesce(F.col("boardings"), F.lit(0))).alias("boardings"),
        F.sum(F.coalesce(F.col("alightings"), F.lit(0))).alias("alightings"),
    )
    return grouped.select(
        *_window_cols(),
        "stop_id",
        "provenance_status",
        "scenario_id",
        "evidence_grade",
        "source_run_id",
        "boardings",
        "alightings",
    )


def aviation_counts(frame: DataFrame) -> DataFrame:
    """Event count per airport, direction, and minute."""
    grouped = _prepared(frame).groupBy(
        F.window("event_time", WIDTH),
        "airport",
        "direction",
        "provenance_status",
        "scenario_id",
        "evidence_grade",
        "source_run_id",
    ).agg(F.count("*").alias("event_count"))
    return grouped.select(
        *_window_cols(),
        "airport",
        "direction",
        "provenance_status",
        "scenario_id",
        "evidence_grade",
        "source_run_id",
        "event_count",
    )
