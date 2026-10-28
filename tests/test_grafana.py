"""Dashboard links and the allowlisted Hive reads."""

from __future__ import annotations

import pytest

from flowstate.config import get_settings
from flowstate.jobs.grafana import grafana_link
from flowstate.warehouse.grafana_tables import (
    chart_rows,
    stream_sql,
    warehouse_sql,
)


def test_a_finished_job_links_to_its_question():
    settings = get_settings()
    url = grafana_link(settings, "demand_hotspots", "abc")
    assert url == (
        "http://127.0.0.1:3000/d/analytics-demand_hotspots?var-run_id=abc"
    )
    assert grafana_link(settings, "hdfs_probe", "abc") is None
    flights = grafana_link(settings, "cross_modal", "abc")
    assert flights is not None
    assert "analytics-ridehail_metro" in flights


def test_a_panel_cannot_name_the_corpus():
    with pytest.raises(ValueError):
        warehouse_sql("rides", None)
    with pytest.raises(ValueError):
        warehouse_sql("corpus_amplified", None)
    sql = warehouse_sql("rollup_ridehail_day", None)
    assert "rollup_ridehail_day" in sql
    assert "rides_amplified" not in sql


def test_full_days_stop_at_the_closer_total():
    from datetime import date

    from flowstate.resample.scale_corpus import select_days

    days, total = select_days(
        {
            "2026-10-26": 40,
            "2026-10-27": 70,
            "2026-10-28": 50,
        },
        end=date(2026, 10, 28),
        align_start=date(2026, 10, 1),
        align_end=date(2026, 10, 28),
        target=100,
    )
    assert days == ["2026-10-27", "2026-10-28"]
    assert total == 120


def test_demand_chart_keeps_weekday_and_weekend_apart():
    rows = chart_rows(
        "analytics_demand_hotspots",
        "hour",
        [
            {"hour_of_day": "10", "day_type": "weekday", "demand_count": "2"},
            {"hour_of_day": "10", "day_type": "weekend", "demand_count": "5"},
            {"hour_of_day": "10", "day_type": "weekday", "demand_count": "3"},
        ],
    )
    assert rows == [{"hour": "10", "weekday": 5.0, "weekend": 5.0}]


def test_aviation_chart_does_not_merge_provenance():
    rows = chart_rows(
        "rollup_aviation_day",
        "series",
        [
            {
                "service_date": "2024-10-28",
                "direction": "arrival",
                "provenance_status": "synthetic",
                "events": "1",
            },
            {
                "service_date": "2024-10-28",
                "direction": "arrival",
                "provenance_status": "observed_proxy",
                "events": "4",
            },
        ],
    )
    assert rows == [
        {
            "time": "2024-10-28",
            "arrival synthetic": 1.0,
            "arrival observed_proxy": 4.0,
        }
    ]


def test_live_sql_is_two_series():
    sql = stream_sql("sink_active_rides", "kia")
    assert "sum(activity_count) AS city" in sql
    assert "AS ward" in sql
    assert "b_1" in sql
    cancel = stream_sql("sink_cancel_rate", "kia")
    assert "city_n AS city_events" in cancel
    assert "ward_n AS ward_events" in cancel
    with pytest.raises(ValueError):
        stream_sql("rides", "kia")
    with pytest.raises(ValueError):
        stream_sql("sink_active_rides", "ward_1;drop")
