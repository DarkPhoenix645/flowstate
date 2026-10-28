"""Start one streaming query on Compose Spark. No second metric."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

from flowstate.streaming.ride_event_schema import envelope_spark_schema
from flowstate.streaming.stream_args import parse_stream_args

log = logging.getLogger(__name__)
RIDEHAIL = "flowstate.ridehail.activity.v1"


class _DropLog(StreamingQueryListener):
    def onQueryStarted(self, event) -> None:  # noqa: ANN001
        return

    def onQueryTerminated(self, event) -> None:  # noqa: ANN001
        return

    def onQueryProgress(self, event) -> None:  # noqa: ANN001
        progress = event.progress
        operators = getattr(progress, "stateOperators", None)
        if operators is None and isinstance(progress, dict):
            operators = progress.get("stateOperators") or []
        for operator in operators or []:
            dropped = getattr(operator, "numRowsDroppedByWatermark", None)
            if dropped is None and isinstance(operator, dict):
                dropped = operator.get("numRowsDroppedByWatermark")
            log.info("numRowsDroppedByWatermark %s", dropped)


def session(name: str, *, one_core: bool = False) -> SparkSession:
    builder = (
        SparkSession.builder.appName(name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if one_core:
        builder = builder.config("spark.cores.max", "1").config(
            "spark.executor.cores", "1"
        )
    spark = builder.getOrCreate()
    spark.streams.addListener(_DropLog())
    return spark


def events(spark: SparkSession, args: dict[str, str]) -> DataFrame:
    """Kafka JSON decoded with the envelope struct. No hand-written schema."""
    schema = envelope_spark_schema()
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args["kafka"])
        .option("subscribe", RIDEHAIL)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", int(args["max-offsets"]))
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), schema).alias("e")
    )
    return parsed.select(
        F.to_timestamp(F.col("e.event_time")).alias("event_time"),
        F.col("e.location_ref.at.ward_id").alias("ward_id"),
        F.col("e.provenance.status").alias("provenance_status"),
        F.col("e.scenario_id").alias("scenario_id"),
        F.col("e.event_type").alias("event_type"),
        F.col("e.provenance.evidence_grade").alias("evidence_grade"),
    )


def write_query(frame: DataFrame, args: dict[str, str], name: str):
    writer = (
        frame.writeStream.format("parquet")
        .option("path", args["sink"])
        .option("checkpointLocation", args["checkpoint"])
        .outputMode("append")
        .queryName(name)
    )
    if args["trigger"] == "live":
        writer = writer.trigger(processingTime="30 seconds")
    else:
        writer = writer.trigger(availableNow=True)
    query = writer.start()
    query.awaitTermination()
    return query


def load_args() -> dict[str, str]:
    return parse_stream_args()
