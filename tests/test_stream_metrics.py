"""Stream node grains. Kafka is not required for the static frame."""

from __future__ import annotations

import sys
from datetime import date

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from flowstate.api.server import dispatch
from flowstate.config.postgres import ConfigStore
from flowstate.jobs.backend import FakeBackend
from flowstate.streaming.metrics import active_counts, cancel_rates, hour_demand
from flowstate.streaming.surge_score import (
    day_type_days,
    expected_hour,
    poisson_z,
)

_SECRET = "runner-secret"


@pytest.fixture(scope="module")
def spark():
    if sys.platform == "win32":
        pytest.skip("local Spark resets its Python worker on Windows")
    try:
        session = (
            SparkSession.builder.master("local[1]")
            .appName("flowstate-stream-test")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except Exception:
        pytest.skip("local Spark cannot start on this JDK")
    yield session
    session.stop()


def test_newest_replay_id_keeps_the_latest_epoch():
    from flowstate.batch.effect import newest_replay_id

    replay = "sess"
    assert newest_replay_id(["other.3", "sess.0", "sess.2"], replay) == (
        "sess.2"
    )
    assert newest_replay_id(["sess", "sess.0"], replay) == "sess.0"
    assert newest_replay_id(["replay-live"], "replay-live") == "replay-live"
    assert newest_replay_id(["other.1"], replay) is None


def test_a_weekday_span_counts_only_weekdays():
    days = day_type_days(date(2024, 10, 28), date(2024, 11, 3), "weekday")
    assert days == 5
    assert poisson_z(10, expected_hour(50, 5)) == 0
    assert poisson_z(1, 0) is None


def test_active_counts_keep_event_type(spark):
    frame = _events(
        spark,
        [
            _row("2024-10-28T10:00:10Z", "booking_created"),
            _row("2024-10-28T10:00:40Z", "booking_created"),
            _row("2024-10-28T10:01:10Z", "cancelled_ride"),
        ],
    )
    rows = {
        (row.event_type, row.activity_count)
        for row in active_counts(frame).collect()
    }
    assert ("booking_created", 2) in rows
    assert ("cancelled_ride", 1) in rows
    assert "cancel_rate" not in active_counts(frame).columns


def test_cancel_rate_is_null_under_five_events(spark):
    frame = _events(
        spark,
        [
            _row("2024-10-28T10:00:10Z", "cancelled_ride"),
            _row("2024-10-28T10:00:20Z", "booking_created"),
        ],
    )
    row = cancel_rates(frame).collect()[0]
    assert row.activity_count == 2
    assert row.cancel_rate is None
    assert "driver" not in cancel_rates(frame).columns


def test_hour_demand_sums_the_minute_counts(spark):
    minute = spark.createDataFrame(
        [
            (
                "2024-10-28 04:30:00",
                "2024-10-28 04:31:00",
                "b_1",
                None,
                "aggregate_calibrated",
                2,
            ),
            (
                "2024-10-28 04:31:00",
                "2024-10-28 04:32:00",
                "b_1",
                None,
                "aggregate_calibrated",
                3,
            ),
        ],
        StructType(
            [
                StructField("start", StringType(), False),
                StructField("end", StringType(), False),
                StructField("ward_id", StringType(), True),
                StructField("scenario_id", StringType(), True),
                StructField("evidence_grade", StringType(), True),
                StructField("n", IntegerType(), False),
            ]
        ),
    )
    minute = minute.select(
        F.struct(
            F.to_timestamp("start").alias("start"),
            F.to_timestamp("end").alias("end"),
        ).alias("window"),
        "ward_id",
        "scenario_id",
        "evidence_grade",
        "n",
    )
    row = hour_demand(minute).collect()[0]
    assert row.demand == 5
    assert row.hour_start.hour == 10


def test_scenario_effect_needs_a_replay_run():
    store = ConfigStore(
        f"postgresql://flowstate:{_SECRET}@127.0.0.1:1/flowstate"
    )
    status, body = dispatch(
        store,
        "POST",
        "/api/runs",
        b'{"kind":"analytics","job":"scenario_effect","params":{}}',
        backends={"compose": FakeBackend()},
    )
    assert status == 422
    assert _SECRET not in str(body)


def test_metro_counts_sum_entries_and_keep_scenario(spark):
    frame = spark.createDataFrame(
        [
            (
                "2024-10-28T10:00:10Z",
                "s1",
                "synthetic",
                None,
                "aggregate_calibrated",
                "replay-1",
                3,
                1,
            ),
            (
                "2024-10-28T10:00:40Z",
                "s1",
                "synthetic",
                "fork-1",
                "assumption_driven",
                "replay-1",
                2,
                0,
            ),
        ],
        StructType(
            [
                StructField("event_time", StringType(), False),
                StructField("station_id", StringType(), True),
                StructField("provenance_status", StringType(), True),
                StructField("scenario_id", StringType(), True),
                StructField("evidence_grade", StringType(), True),
                StructField("source_run_id", StringType(), True),
                StructField("entries_count", IntegerType(), True),
                StructField("exits_count", IntegerType(), True),
            ]
        ),
    ).withColumn(
        "event_time",
        F.to_timestamp("event_time", "yyyy-MM-dd'T'HH:mm:ssX"),
    )
    from flowstate.streaming.metrics import metro_counts

    rows = metro_counts(frame).collect()
    assert {row.scenario_id for row in rows} == {None, "fork-1"}
    baseline = [row for row in rows if row.scenario_id is None][0]
    assert baseline.entries_count == 3
    assert baseline.exits_count == 1


def test_surge_without_a_baseline_is_422():
    store = ConfigStore(
        f"postgresql://flowstate:{_SECRET}@127.0.0.1:1/flowstate"
    )
    status, body = dispatch(
        store,
        "POST",
        "/api/runs",
        b'{"kind":"stream","job":"surge","params":{}}',
        backends={"compose": FakeBackend()},
    )
    assert status == 422
    assert _SECRET not in str(body)


def test_make_has_no_stream_consume_target():
    text = open("Makefile", encoding="utf-8").read()
    assert "stream-consume:" not in text


def _row(when: str, event_type: str) -> tuple:
    return (
        when,
        "b_1",
        "synthetic",
        None,
        event_type,
        "aggregate_calibrated",
    )


def _events(spark, rows):
    frame = spark.createDataFrame(rows, _EVENT_SCHEMA)
    return frame.withColumn(
        "event_time", F.to_timestamp("event_time", "yyyy-MM-dd'T'HH:mm:ssX")
    )


_EVENT_SCHEMA = StructType(
    [
        StructField("event_time", StringType(), False),
        StructField("ward_id", StringType(), True),
        StructField("provenance_status", StringType(), True),
        StructField("scenario_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("evidence_grade", StringType(), True),
    ]
)
