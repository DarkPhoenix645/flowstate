"""Resampler tests: model math, allocators, provenance, validation.

Everything runs on fixture tables (``resample_fixtures.py``): no
network, no Compose, no repo data directory. The fixture city is
not Bengaluru, so a hardcoded city literal fails here.
"""

from __future__ import annotations

import math
from datetime import datetime
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
import pytest

import resample_fixtures as fx
from flowstate.config import Settings
from flowstate.resample import (
    build_bundle,
    run_amplify,
    to_envelope,
)
from flowstate.resample.bundle import FARE_BASIS
from flowstate.resample.nb2 import (
    crps,
    fit_nb2,
    log_score,
    nb_pmf,
    support_size,
)
from flowstate.resample.unfold import (
    ParentTotals,
    allocate_days,
    bus_events,
    dm_allocate,
    largest_remainder,
    metro_day_events,
    ridehail_day_events,
    rng_for,
    weighted_without_replacement,
)


@pytest.fixture(scope="module")
def bundle():
    return build_bundle(fx.fixture_inputs(), fx.fixture_spec(), seed=42)


def test_nb2_pmf_matches_direct_formula():
    mu, phi = 137.0, 2.3
    pmf = nb_pmf(mu, phi, support_size(mu, phi))

    def direct(k: int) -> float:
        return math.exp(
            math.lgamma(k + phi)
            - math.lgamma(phi)
            - math.lgamma(k + 1.0)
            + phi * math.log(phi / (phi + mu))
            + k * math.log(mu / (phi + mu))
        )

    assert abs(float(pmf.sum()) - 1.0) < 1e-9
    for k in (0, 1, 7, 137, 260):
        assert abs(float(pmf[k]) - direct(k)) < 1e-9


def test_nb2_pmf_survives_large_daily_means():
    mu, phi = 1_146_060.0, 136.5
    pmf = nb_pmf(mu, phi, support_size(mu, phi))
    assert abs(float(pmf.sum()) - 1.0) < 1e-9
    support = np.arange(len(pmf))
    mean = float((pmf * support).sum())
    assert mean == pytest.approx(mu, rel=1e-3)
    assert crps(mu, phi, 1_100_000) < crps(mu, phi, 2_000_000)


def test_nb2_fit_recovers_data_mean():
    rng = np.random.default_rng(3)
    n = 300
    design = np.column_stack(
        [np.ones(n), rng.random(n), np.linspace(0, 1, n) ** 2]
    )
    beta = np.array([3.0, 0.4, -0.5])
    mu = np.exp(design @ beta)
    phi = 12.0
    counts = rng.negative_binomial(phi, phi / (phi + mu))
    fit = fit_nb2(design, counts, ("intercept", "x", "t2"))
    assert float(fit.mean(design).mean()) == pytest.approx(
        float(counts.mean()), rel=0.05
    )
    # Log score of the truth beats the log score of a wild guess.
    mid = len(mu) // 2
    truth = log_score(mu[mid], fit.phi, int(counts[mid]))
    wild = log_score(mu[mid] * 3.0, fit.phi, int(counts[mid]))
    assert truth < wild


def test_largest_remainder_sums_exactly():
    assert sum(largest_remainder(100, [1, 2, 3, 4])) == 100
    assert sum(largest_remainder(7, [0.001, 0.001, 998.0])) == 7
    split = largest_remainder(10, [1.0, 1.0, 1.0])
    assert split == [3, 3, 4] or split == [4, 3, 3] or split == [3, 4, 3]


def test_dm_allocate_conserves_parent_total():
    rng = np.random.default_rng(0)
    weights = [0.1, 0.2, 0.05, 0.65]
    counts = dm_allocate(rng, 1000, weights, kappa=50.0)
    assert int(counts.sum()) == 1000
    assert (counts >= 0).all()
    again = dm_allocate(np.random.default_rng(0), 1000, weights, 50.0)
    assert list(counts) == list(again)
    empty = dm_allocate(rng, 0, weights, 50.0)
    assert int(empty.sum()) == 0


def test_bundle_fits_and_records_evaluation(bundle):
    evaluation = bundle.clocks.evaluation
    assert evaluation["schema"] == "resampler-evaluation/1"
    assert evaluation["protocol"]["bootstrap_seed"] == 20200101
    for mode in ("ride_hail", "metro"):
        record = evaluation[mode]
        assert record["train_dates"] >= 60
        assert record["holdout_dates_scored"] >= 20
        decision = record["aviation_terms"]
        assert isinstance(decision["keep_aviation_terms"], bool)
        if decision["keep_aviation_terms"]:
            assert decision["improvement"] >= 0.05
            assert decision["improvement_ci"][0] > 0
        elif "improvement" in decision:
            assert (
                decision["improvement"] < 0.05
                or decision["improvement_ci"][0] <= 0
            )
    rho = evaluation["copula"]["rho"]
    assert -1.0 <= rho <= 1.0


def test_bundle_is_city_independent(bundle):
    assert bundle.spec.id == "testcity"
    assert bundle.spec.local_class == "local_event"
    assert bundle.spec.airport_code == "TEST"
    assert bundle.coverage["city"] == "testcity"
    # The fixture counts carry no Bengaluru tokens.
    assert "bengaluru_local" not in str(bundle.coverage)


def test_ridehail_day_conserves_parent_totals(bundle):
    parents = ParentTotals()
    rng = rng_for(42, "ride_hail", "2025-05-05")
    rows = [
        row
        for batch in ridehail_day_events(
            bundle,
            rng,
            "2025-05-05",
            {"booking": 400, "done_ride": 250, "cancel_ride": 80},
            40.0,
            parents,
        )
        for row in batch
    ]
    assert len(rows) == 400 + 250 + 80
    by_type: dict[str, int] = {}
    for row in rows:
        by_type[row["event_type"]] = by_type.get(row["event_type"], 0) + 1
    assert by_type == {
        "booking_created": 400,
        "completed_ride": 250,
        "cancelled_ride": 80,
    }
    assert set(parents.ridehail.values()) == {400, 250, 80}
    for row in rows:
        assert row["provenance_status"] == "synthetic"
        assert row["evidence_grade"] == "aggregate_calibrated"
        assert row["parent_source"] == "observed"
        assert row["city"] == "testcity"
        moment = datetime.fromisoformat(row["timestamp"])
        assert moment.date().isoformat() == "2025-05-05"
        assert moment.hour == row["hour"]
        if row["event_type"] == "cancelled_ride":
            assert row["cancel_reason"] == "unknown"
            assert row[FARE_BASIS] is None
        if row["event_type"] == "completed_ride":
            assert row["fare"] is None
            assert row[FARE_BASIS] is not None and row[FARE_BASIS] >= 0
        if row["event_type"] == "booking_created":
            assert row["status"] is None


def test_ridehail_replay_equality(bundle):
    def once() -> list[dict]:
        parents = ParentTotals()
        rng = rng_for(7, "ride_hail", "2025-05-05")
        return [
            row
            for batch in ridehail_day_events(
                bundle,
                rng,
                "2025-05-05",
                {"booking": 50, "done_ride": 30, "cancel_ride": 10},
                None,
                parents,
            )
            for row in batch
        ]

    first = once()
    second = once()
    assert first == second
    assert len({row["trip_id"] for row in first}) == len(first)


def test_allocate_days_drops_zero_budgets():
    assigned = allocate_days(10, ["a", "b", "c"], [100.0, 0.0, 50.0])
    days = [day for day, _ in assigned]
    assert "b" not in days
    assert sum(count for _, count in assigned) == 10


def test_weighted_without_replacement_respects_mass():
    rng = np.random.default_rng(0)
    weights = np.array([0.0, 10.0, 0.0, 10.0, 0.0])
    picked = weighted_without_replacement(rng, weights, 2)
    assert set(picked.tolist()) == {1, 3}


def test_ridehail_partial_day_hits_exact_rows(bundle):
    parents = ParentTotals()
    rng = rng_for(42, "ride_hail", "2025-05-05")
    rows = [
        row
        for batch in ridehail_day_events(
            bundle,
            rng,
            "2025-05-05",
            {"booking": 400, "done_ride": 250, "cancel_ride": 80},
            None,
            parents,
            partial_rows=111,
        )
        for row in batch
    ]
    assert len(rows) == 111
    assert sum(parents.ridehail.values()) == 111


def test_metro_day_conserves_and_respects_regime(bundle):
    pre = ParentTotals()
    rng = rng_for(42, "metro", "2025-08-05")
    rows_pre = [
        row
        for batch in metro_day_events(
            bundle, rng, "2025-08-05", 20_000, pre
        )
        for row in batch
    ]
    post = ParentTotals()
    rng = rng_for(42, "metro", "2025-08-20")
    rows_post = [
        row
        for batch in metro_day_events(
            bundle, rng, "2025-08-20", 20_000, post
        )
        for row in batch
    ]
    for parent, rows in ((pre, rows_pre), (post, rows_post)):
        entries = sum(row["entries_count"] for row in rows)
        exits = sum(row["exits_count"] for row in rows)
        assert entries == 20_000
        assert exits == 20_000
        for row in rows:
            assert row["count"] == row["entries_count"] + row["exits_count"]
            assert row["event_type"] == "station_hour"
    pre_stations = {row["pickup_zone"] for row in rows_pre}
    post_stations = {row["pickup_zone"] for row in rows_post}
    assert "S4" not in pre_stations
    assert "S4" in post_stations


def test_metro_pack_keeps_entries_equal_exits(bundle):
    parents = ParentTotals()
    rng = rng_for(42, "metro", "2025-08-20")
    rows = [
        row
        for batch in metro_day_events(
            bundle,
            rng,
            "2025-08-20",
            4_000,
            parents,
            partial_records=9,
        )
        for row in batch
    ]
    assert 1 <= len(rows) <= 9
    entries = sum(row["entries_count"] for row in rows)
    exits = sum(row["exits_count"] for row in rows)
    assert entries == exits == 4_000


def test_envelope_shape(bundle):
    parents = ParentTotals()
    rng = rng_for(42, "metro", "2025-08-20")
    row = next(
        iter(
            next(
                metro_day_events(
                    bundle, rng, "2025-08-20", 5_000, parents
                ),
                [],
            )
        ),
        None,
    )
    assert row is not None
    envelope = to_envelope(row)
    assert envelope["city_id"] == "blr"
    assert envelope["event_type"] == "station_hour"
    assert envelope["location_ref"]["station_id"] == row["pickup_zone"]
    assert envelope["provenance"]["status"] == "synthetic"
    assert envelope["provenance"]["source_ids"] == [
        "namma_metro_daily",
        "bmrcl_rti",
    ]
    assert envelope["count"] == (
        envelope["entries_count"] + envelope["exits_count"]
    )


def _corrupt(row: dict, **overrides) -> dict:
    clone = dict(row)
    clone.update(overrides)
    return clone


def test_validator_flags_nonsense(bundle):
    from flowstate.resample.checks import _Issues, check_rows

    parents = ParentTotals()
    rng = rng_for(42, "metro", "2025-08-20")
    good = next(
        iter(
            next(
                metro_day_events(bundle, rng, "2025-08-20", 5_000, parents),
                [],
            )
        ),
        None,
    )
    ride = [
        row
        for batch in ridehail_day_events(
            bundle,
            rng_for(42, "ride_hail", "2025-08-20"),
            "2025-08-20",
            {"booking": 10, "done_ride": 10, "cancel_ride": 4},
            None,
            ParentTotals(),
        )
        for row in batch
    ]
    completed = next(r for r in ride if r["event_type"] == "completed_ride")
    cancelled = next(r for r in ride if r["event_type"] == "cancelled_ride")
    # A full-column bus row: clone the metro record, then retarget it.
    stop_call = _corrupt(
        good,
        trip_id="bmtc-1-00001",
        mode="bus",
        event_type="scheduled_stop",
        vehicle_type="bus",
        pickup_zone="stop_001",
        drop_zone=None,
        source="bmtc_gtfs",
        provenance_status="derived",
        evidence_grade="shape_prior",
        source_ids="bmtc_gtfs",
        parent_source="assumed",
        parent_id="bmtc_gtfs:1",
        boardings=10,
        alightings=10,
        onboard_leaving=5,
        ridership_status="synthetic",
        ridership_grade="assumption_driven",
        entries_count=None,
        exits_count=None,
        count=1,
    )

    def collect(rows):
        issues = _Issues()
        check_rows(rows, bundle, issues, set())
        return issues.items

    assert collect([good, completed, cancelled, stop_call]) == []
    assert any("corpus row labelled observed" in i for i in collect(
        [_corrupt(good, provenance_status="observed")]
    ))
    assert any("bad provenance" in i for i in collect(
        [_corrupt(good, provenance_status="probably")]
    ))
    assert any("negative metro count" in i for i in collect(
        [_corrupt(good, entries_count=-1)]
    ))
    assert any("count != entries + exits" in i for i in collect(
        [_corrupt(good, count=99)]
    ))
    assert any("is not active" in i for i in collect(
        [_corrupt(good, pickup_zone="S99")]
    ))
    assert any("cancel_reason" in i for i in collect(
        [_corrupt(cancelled, cancel_reason="driver")]
    ))
    assert any("canonical fare" in i for i in collect(
        [_corrupt(completed, fare=90.0)]
    ))
    assert any("capacity cap" in i for i in collect(
        [_corrupt(stop_call, onboard_leaving=10_000)]
    ))
    assert any("duplicate event id" in i for i in collect(
        [completed, completed]
    ))
    assert any("ridership must stay synthetic" in i for i in collect(
        [_corrupt(stop_call, ridership_status="observed")]
    ))
    assert any("hour column" in i for i in collect(
        [_corrupt(good, hour=23)]
    ))


def test_bus_events_trip_conservation(bundle):
    inputs = fx.fixture_inputs()
    parents = ParentTotals()
    stream, summary = bus_events(
        bundle,
        rng_for(42, "bus"),
        inputs.stop_times,
        feed_date="2026-09-07",
        budget_rows=60,
        parents=parents,
    )
    rows = [row for batch in stream for row in batch]
    assert len(rows) == summary["stop_calls"] == 60
    cap = (
        bundle.config.assumed_bus_capacity
        * bundle.config.assumed_max_load_factor
    )
    trips: dict[str, list[dict]] = {}
    for row in rows:
        assert row["provenance_status"] == "derived"
        assert row["ridership_status"] == "synthetic"
        assert row["ridership_grade"] == "assumption_driven"
        assert row["outside_training_window"] == "true"
        assert 0 <= row["onboard_leaving"] <= cap
        trips.setdefault(row["parent_id"], []).append(row)
    board = sum(
        parents.bus.get((trip, "boardings"), 0) for trip in trips
    )
    alight = sum(
        parents.bus.get((trip, "alightings"), 0) for trip in trips
    )
    assert board == alight and board > 0
    assert summary["boardings_emitted"] == board
    for trip_rows in trips.values():
        assert (
            sum(r["boardings"] for r in trip_rows)
            == sum(r["alightings"] for r in trip_rows)
        )
    # The assumed citywide total per trip far exceeds the capacity
    # cap, so capping must have displaced boardings and said so.
    assert summary["capped_trips"] >= 1
    assert summary["boardings_displaced_by_cap"] > 0


def _amplify(settings_dir: Path, rows: int, seed: int):
    settings = Settings(data_dir=settings_dir)
    return run_amplify(settings, rows, seed)


def test_run_amplify_end_to_end(tmp_path, monkeypatch, bundle):
    import flowstate.resample.writer as writer_module

    inputs = fx.fixture_inputs()
    monkeypatch.setattr(
        writer_module, "load_inputs", lambda settings: inputs
    )
    monkeypatch.setattr(
        writer_module,
        "city_spec",
        lambda profile: fx.fixture_spec(),
    )
    manifest = run_amplify(Settings(data_dir=tmp_path), 2000, seed=42)
    assert manifest["rows"] == 2000
    assert set(manifest["mode_counts"]) == {
        "ride_hail",
        "metro",
        "bus",
        "aviation",
    }
    assert all(count > 0 for count in manifest["mode_counts"].values())
    corpus = tmp_path / "staged" / "rides_amplified"
    parts = sorted(corpus.glob("part-*.parquet"))
    assert parts, "no part files written"
    assert (corpus / "manifest.json").is_file()
    assert (corpus / "bundle.json").is_file()
    assert (corpus / "evaluation.json").is_file()
    written = 0
    provenance: set[str] = set()
    for part in parts:
        table = pq.read_table(part)
        written += table.num_rows
        provenance.update(
            table.column("provenance_status").to_pylist()
        )
    assert written == 2000
    assert provenance == {"synthetic", "observed_proxy", "derived"}
    ride_dates = set()
    metro_dates = set()
    weekdays = {i: 0 for i in range(7)}
    for part in parts:
        table = pq.read_table(part)
        modes = table.column("mode").to_pylist()
        stamps = table.column("timestamp").to_pylist()
        for mode, stamp in zip(modes, stamps, strict=True):
            day = stamp[:10]
            if mode == "ride_hail":
                ride_dates.add(day)
                weekdays[datetime.fromisoformat(stamp).weekday()] += 1
            elif mode == "metro":
                metro_dates.add(day)
    # Thin copy of the window, not three full source days.
    assert len(ride_dates) > 50
    assert len(metro_dates) > 20
    total_wd = sum(weekdays.values()) or 1
    shares = [weekdays[i] / total_wd for i in range(7)]
    assert max(shares) - min(shares) < 0.25


def test_replay_equality_end_to_end(tmp_path, monkeypatch):
    import flowstate.resample.writer as writer_module

    inputs = fx.fixture_inputs()
    monkeypatch.setattr(
        writer_module, "load_inputs", lambda settings: inputs
    )
    monkeypatch.setattr(
        writer_module, "city_spec", lambda profile: fx.fixture_spec()
    )
    first = run_amplify(Settings(data_dir=tmp_path / "a"), 1500, seed=42)
    second = run_amplify(Settings(data_dir=tmp_path / "b"), 1500, seed=42)
    assert first["rows"] == second["rows"] == 1500
    assert first["mode_counts"] == second["mode_counts"]
    assert first["parts"] == second["parts"]
    other = run_amplify(Settings(data_dir=tmp_path / "c"), 1500, seed=43)
    assert other["parts"] != first["parts"]


def test_rows_below_minimum_rejected(tmp_path):
    with pytest.raises(ValueError, match="too small"):
        run_amplify(Settings(data_dir=tmp_path), 10, seed=42)


def test_source_files_not_touched_by_amplify(tmp_path, monkeypatch):
    import flowstate.resample.writer as writer_module

    inputs = fx.fixture_inputs()
    before = {
        name: table.num_rows
        for name, table in (
            ("city_day", inputs.city_day),
            ("flight_events", inputs.flight_events),
        )
    }
    monkeypatch.setattr(
        writer_module, "load_inputs", lambda settings: inputs
    )
    monkeypatch.setattr(
        writer_module, "city_spec", lambda profile: fx.fixture_spec()
    )
    run_amplify(Settings(data_dir=tmp_path), 1000, seed=1)
    assert inputs.city_day.num_rows == before["city_day"]
    assert inputs.flight_events.num_rows == before["flight_events"]
