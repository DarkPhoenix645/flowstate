"""Layer 1 aggregate clocks. NB2 fits, held-out evaluation, copula.

The evaluation protocol per ADR-0010: fit the alignment window up to
the day before the holdout, score the holdout (the station-hour
service spans), then refit the selected spec through the window end
with the holdout results frozen. Aviation covariates stay only under
the 5% rule. Missing dates are dropped, never zero-filled. DGCA
monthly passenger counts are context only and are not a covariate.

No city date or code lives here: ``bundle.py`` reads the city
profile and passes the windows, the line-open date, and the
local-event class in. All builders take pyarrow tables.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import numpy as np
import pyarrow as pa

from flowstate.resample.aviation_coverage import covered_counts
from flowstate.resample.nb2 import (
    CALIBRATION_TOLERANCE,
    EVALUATION_SEED,
    IMPROVEMENT_RULE,
    NB2Fit,
    crps,
    fit_nb2,
    improvement_ci,
    log_score,
    nb_cdf,
    normal_ppf,
    predictive_interval,
)
from flowstate.resample.profiles import to_date

WEEKDAY_NAMES = ("mon", "tue", "wed", "thu", "fri", "sat")
MIN_COVERED_AVIATION_DAYS = 60


@dataclass(frozen=True)
class Marginal:
    """One mode's NB2 marginal plus its frozen evaluation decision."""

    mode: str
    fit_train: NB2Fit
    fit_final: NB2Fit
    keep_flights: bool
    record: dict[str, Any]


@dataclass(frozen=True)
class Copula:
    """Auto-metro residual coupling fitted on shared valid dates."""

    rho: float
    kept: bool
    shared_dates: int
    halves: tuple[float, float]
    ci: tuple[float, float]

    def sample_pair(self, rng: np.random.Generator) -> tuple[float, float]:
        """One correlated standard-normal pair (Cholesky of 2x2)."""
        if not self.kept:
            return float(rng.standard_normal()), float(rng.standard_normal())
        z1 = float(rng.standard_normal())
        z2 = self.rho * z1 + math.sqrt(1.0 - self.rho * self.rho) * float(
            rng.standard_normal()
        )
        return z1, z2


@dataclass(frozen=True)
class Clocks:
    """Layer 1 result: two marginals, the copula, and aviation dailies."""

    ride_hail: Marginal
    metro: Marginal
    copula: Copula
    aviation: dict[str, NB2Fit]
    evaluation: dict[str, Any]


def _day_counts(
    table: pa.Table, date_column: str, value_column: str
) -> dict[str, float]:
    frame = table.select([date_column, value_column]).to_pydict()
    counts: dict[str, float] = {}
    for day, value in zip(
        frame[date_column], frame[value_column], strict=True
    ):
        if value is None:
            continue
        counts[str(day)[:10]] = counts.get(str(day)[:10], 0.0) + float(value)
    return counts


def _in_window(day: str, start: date, end: date) -> bool:
    return start <= to_date(day) <= end


def _design_row(
    day: date,
    span_days: int,
    local_dates: set[str],
    *,
    window_start: date,
    line_open: date | None = None,
    arrivals: float | None = None,
    departures: float | None = None,
) -> list[float]:
    row = [1.0]
    row.extend(
        1.0 if day.weekday() == index else 0.0
        for index in range(6)  # Sunday is the reference weekday.
    )
    t = (day - window_start).days / span_days
    row.extend((t, t * t))
    row.append(1.0 if day.isoformat() in local_dates else 0.0)
    if line_open is not None:
        row.append(1.0 if day >= line_open else 0.0)
    if arrivals is not None:
        row.append(math.log1p(arrivals))
    if departures is not None:
        row.append(math.log1p(departures))
    return row


def _names(*, regime: bool, flights: bool) -> tuple[str, ...]:
    names = ["intercept", *WEEKDAY_NAMES, "trend", "trend_sq", "local_event"]
    if regime:
        names.append("network_regime")
    if flights:
        names.extend(("log1p_arrivals", "log1p_departures"))
    return tuple(names)


def _matrix(
    days: list[str],
    span_days: int,
    local_dates: set[str],
    *,
    window_start: date,
    line_open: date | None = None,
    arrivals: dict[str, float] | None = None,
    departures: dict[str, float] | None = None,
) -> np.ndarray:
    rows = [
        _design_row(
            to_date(day),
            span_days,
            local_dates,
            window_start=window_start,
            line_open=line_open,
            arrivals=arrivals.get(day) if arrivals is not None else None,
            departures=(
                departures.get(day) if departures is not None else None
            ),
        )
        for day in days
    ]
    if not rows:
        raise ValueError("no design rows")
    return np.asarray(rows, dtype=float)


def _split_days(
    counts: dict[str, float], holdout_start: date, holdout_end: date
) -> tuple[list[str], list[str]]:
    train = [
        day for day in sorted(counts) if to_date(day) < holdout_start
    ]
    holdout = [
        day
        for day in sorted(counts)
        if holdout_start <= to_date(day) <= holdout_end
    ]
    return train, holdout


def _per_date_nb2(
    fit: NB2Fit,
    design: np.ndarray,
    counts: dict[str, float],
    days: list[str],
) -> dict[str, dict[str, float | int]]:
    scores: dict[str, dict[str, float | int]] = {}
    for day, mu in zip(days, fit.mean(design), strict=True):
        observed = int(round(counts[day]))
        low, high = predictive_interval(mu, fit.phi)
        scores[day] = {
            "crps": crps(mu, fit.phi, observed),
            "log_score": log_score(mu, fit.phi, observed),
            "mae": abs(observed - mu),
            "covered": int(low <= observed <= high),
        }
    return scores


def _per_date_point(
    predictions: dict[str, float],
    counts: dict[str, float],
    days: list[str],
) -> dict[str, dict[str, float | int]]:
    scores = {}
    for day in days:
        error = abs(counts[day] - predictions.get(day, 0.0))
        scores[day] = {"crps": error, "mae": error}
    return scores


def _aggregate(
    scores: dict[str, dict[str, float | int]],
) -> dict[str, Any]:
    dates = sorted(scores)
    crps_values = [scores[d]["crps"] for d in dates]
    mae_values = [scores[d]["mae"] for d in dates]
    covered = sum(scores[d].get("covered", 0) for d in dates)
    log_values = [
        scores[d]["log_score"] for d in dates if "log_score" in scores[d]
    ]
    return {
        "dates": len(dates),
        "crps": float(np.mean(crps_values)) if dates else None,
        "mae": float(np.mean(mae_values)) if dates else None,
        "log_score": float(np.mean(log_values)) if log_values else None,
        "coverage_90": covered / len(dates) if dates else None,
    }


def _weekday_medians(
    counts: dict[str, float], train: list[str]
) -> dict[int, float]:
    by_weekday: dict[int, list[float]] = {}
    for day in train:
        by_weekday.setdefault(to_date(day).weekday(), []).append(counts[day])
    return {
        weekday: float(np.median(values)) if values else 0.0
        for weekday, values in by_weekday.items()
    }


def _shift(day: str, days: int) -> str:
    return (to_date(day) + timedelta(days=days)).isoformat()


def _fit_marginal(
    mode: str,
    counts: dict[str, float],
    local_dates: set[str],
    span_days: int,
    arrivals: dict[str, float],
    departures: dict[str, float],
    *,
    window_start: date,
    regime: date | None,
    holdout_start: date,
    holdout_end: date,
) -> Marginal:
    """Fit, evaluate on the frozen holdout, apply the 5% rule, refit.

    The holdout scores are frozen: the final refit uses all valid
    dates through the window end, and the selection numbers in the
    record always come from the train-window fit.
    """
    train, holdout = _split_days(counts, holdout_start, holdout_end)
    if len(train) < 60:
        raise ValueError(f"{mode}: too few train dates ({len(train)})")
    all_days = sorted(counts)

    calendar_fit = fit_nb2(
        _matrix(
            train,
            span_days,
            local_dates,
            window_start=window_start,
            line_open=regime,
        ),
        np.asarray([counts[d] for d in train]),
        _names(regime=regime is not None, flights=False),
    )
    calendar_holdout_scores = _aggregate(
        _per_date_nb2(
            calendar_fit,
            _matrix(
                holdout,
                span_days,
                local_dates,
                window_start=window_start,
                line_open=regime,
            ),
            counts,
            holdout,
        )
    )

    # Dates where every covariate is observed. A day without an
    # aviation count is dropped, not zero-filled (G5, G7).
    flight_train = [d for d in train if d in arrivals and d in departures]
    holdout_common = [d for d in holdout if d in arrivals and d in departures]

    flights_record: dict[str, Any] = {
        "rule": (
            f"keep aviation terms only on >= {IMPROVEMENT_RULE:.0%} "
            "held-out CRPS improvement over the best simple baseline "
            "without materially worse 90% interval coverage"
        )
    }
    keep_flights = False
    if len(flight_train) >= 60 and len(holdout_common) >= 20:
        flights_fit = fit_nb2(
            _matrix(
                flight_train,
                span_days,
                local_dates,
                window_start=window_start,
                line_open=regime,
                arrivals=arrivals,
                departures=departures,
            ),
            np.asarray([counts[d] for d in flight_train]),
            _names(regime=regime is not None, flights=True),
        )
        flights_scores = _per_date_nb2(
            flights_fit,
            _matrix(
                holdout_common,
                span_days,
                local_dates,
                window_start=window_start,
                line_open=regime,
                arrivals=arrivals,
                departures=departures,
            ),
            counts,
            holdout_common,
        )
        calendar_scores = _per_date_nb2(
            calendar_fit,
            _matrix(
                holdout_common,
                span_days,
                local_dates,
                window_start=window_start,
                line_open=regime,
            ),
            counts,
            holdout_common,
        )
        medians = _weekday_medians(counts, train)
        median_prediction = {
            d: medians.get(to_date(d).weekday(), 0.0) for d in holdout_common
        }
        naive_prediction = {
            d: counts.get(
                _shift(d, -7), medians.get(to_date(d).weekday(), 0.0)
            )
            for d in holdout_common
        }
        baselines = {
            "calendar_nb2": calendar_scores,
            "weekday_median": _per_date_point(
                median_prediction, counts, holdout_common
            ),
            "seasonal_naive_lag7": _per_date_point(
                naive_prediction, counts, holdout_common
            ),
        }
        best_name = min(
            baselines, key=lambda name: _aggregate(baselines[name])["crps"]
        )
        flights_aggregate = _aggregate(flights_scores)
        best_aggregate = _aggregate(baselines[best_name])
        improvement = (
            best_aggregate["crps"] - flights_aggregate["crps"]
        ) / best_aggregate["crps"]
        per_date = {
            d: (
                float(baselines[best_name][d]["crps"]),
                float(flights_scores[d]["crps"]),
            )
            for d in holdout_common
        }
        low, point, high = improvement_ci(per_date)
        coverage = flights_aggregate["coverage_90"] or 0.0
        calendar_coverage = calendar_holdout_scores["coverage_90"] or 0.0
        calibrated = (
            abs(coverage - 0.90) <= CALIBRATION_TOLERANCE
            and coverage >= calendar_coverage - CALIBRATION_TOLERANCE
        )
        keep_flights = bool(
            improvement >= IMPROVEMENT_RULE and calibrated and low > 0
        )
        flights_record.update(
            {
                "fit_dates": len(flight_train),
                "holdout_dates": len(holdout_common),
                "best_simple_baseline": best_name,
                "scores": {
                    "flights_nb2": flights_aggregate,
                    "calendar_nb2_on_common": _aggregate(calendar_scores),
                    "weekday_median": _aggregate(
                        baselines["weekday_median"]
                    ),
                    "seasonal_naive_lag7": _aggregate(
                        baselines["seasonal_naive_lag7"]
                    ),
                },
                "improvement": improvement,
                "improvement_ci": [low, high],
                "coverage_90": coverage,
                "calendar_coverage_90": calendar_coverage,
                "keep_aviation_terms": keep_flights,
            }
        )
    else:
        flights_record["keep_aviation_terms"] = False
        flights_record["reason"] = (
            "not enough shared flight/mode dates to test the terms "
            f"(fit={len(flight_train)}, holdout={len(holdout_common)})"
        )

    if keep_flights:
        final_fit = fit_nb2(
            _matrix(
                all_days,
                span_days,
                local_dates,
                window_start=window_start,
                line_open=regime,
                arrivals=arrivals,
                departures=departures,
            ),
            np.asarray([counts[d] for d in all_days]),
            _names(regime=regime is not None, flights=True),
        )
    else:
        final_fit = fit_nb2(
            _matrix(
                all_days,
                span_days,
                local_dates,
                window_start=window_start,
                line_open=regime,
            ),
            np.asarray([counts[d] for d in all_days]),
            _names(regime=regime is not None, flights=False),
        )
    record = {
        "mode": mode,
        "train_dates": len(train),
        "holdout_dates_scored": len(holdout),
        "calendar_holdout_scores": calendar_holdout_scores,
        "aviation_terms": flights_record,
        "final_phi": final_fit.phi,
        "note": (
            "A missing date drops out of every fit and score. "
            "It is not a zero row."
        ),
    }
    return Marginal(
        mode=mode,
        fit_train=calendar_fit,
        fit_final=final_fit,
        keep_flights=keep_flights,
        record=record,
    )


def _fit_aviation_daily(
    flight_events: pa.Table, window_start: date, window_end: date
) -> dict[str, NB2Fit]:
    """NB2 on observed daily event counts, per direction.

    The synthetic-day generator for aviation outside fetched
    windows, not a covariate. Calendar-only: intercept, weekday,
    trend. Days with no observed event are absent and stay absent
    (G5). So are failed and thin windows: a gap in the fetch is not a
    low day (``aviation_coverage``, ADR-0017).
    """
    covered = covered_counts(flight_events)
    counts: dict[str, dict[str, float]] = {
        direction: {day: float(value) for day, value in days.items()}
        for direction, days in covered.items()
    }
    span = max(1, (window_end - window_start).days + 1)
    fits: dict[str, NB2Fit] = {}
    for direction in ("arrival", "departure"):
        days = sorted(counts[direction])
        if len(days) < MIN_COVERED_AVIATION_DAYS:
            raise ValueError(
                f"{direction}: {len(days)} covered days, need "
                f"{MIN_COVERED_AVIATION_DAYS}"
            )
        design = _matrix(days, span, set(), window_start=window_start)
        fits[direction] = fit_nb2(
            design,
            np.asarray([counts[direction][d] for d in days]),
            _names(regime=False, flights=False),
        )
    return fits


def _fit_copula(
    ride_hail: Marginal,
    metro: Marginal,
    ride_counts: dict[str, float],
    metro_counts: dict[str, float],
    local_dates: set[str],
    span_days: int,
    *,
    window_start: date,
    holdout_start: date,
    regime: date | None,
) -> Copula:
    """Gaussian copula on auto-metro residuals, shared valid dates.

    The PIT uses the calendar-only train fit on train-window dates,
    so no holdout or extrapolated mean leaks in. Keep the correlation
    only when both temporal halves agree in sign, the bootstrap CI
    excludes zero, and the halves are reproduced within uncertainty.
    Otherwise shrink to zero (ADR-0010 / plan §7.1).
    """
    shared = sorted(
        {
            day
            for day in set(ride_counts) & set(metro_counts)
            if to_date(day) < holdout_start
        }
    )
    z_ride: list[float] = []
    z_metro: list[float] = []
    for day in shared:
        # One design per marginal: the metro fit carries the regime
        # column, the ride-hail fit does not.
        for fit, counts, sink, line_open in (
            (ride_hail.fit_train, ride_counts, z_ride, None),
            (metro.fit_train, metro_counts, z_metro, regime),
        ):
            design = _matrix(
                [day],
                span_days,
                local_dates,
                window_start=window_start,
                line_open=line_open,
            )
            mu = float(fit.mean(design)[0])
            cdf = nb_cdf(mu, fit.phi)
            index = min(int(round(counts[day])), len(cdf) - 1)
            u = min(max(float(cdf[index]), 1e-6), 1.0 - 1e-6)
            sink.append(normal_ppf(u))
    if len(shared) < 30:
        return Copula(
            rho=0.0,
            kept=False,
            shared_dates=len(shared),
            halves=(0.0, 0.0),
            ci=(0.0, 0.0),
        )
    z_ride_array = np.asarray(z_ride)
    z_metro_array = np.asarray(z_metro)
    rho = float(np.corrcoef(z_ride_array, z_metro_array)[0, 1])
    half = len(shared) // 2
    rho_first = float(
        np.corrcoef(z_ride_array[:half], z_metro_array[:half])[0, 1]
    )
    rho_second = float(
        np.corrcoef(z_ride_array[half:], z_metro_array[half:])[0, 1]
    )
    rng = np.random.default_rng(EVALUATION_SEED)
    indices = np.arange(len(shared))
    values = []
    for _ in range(500):
        sample = rng.choice(indices, size=len(indices), replace=True)
        values.append(
            float(
                np.corrcoef(z_ride_array[sample], z_metro_array[sample])[0, 1]
            )
        )
    low, high = float(np.percentile(values, 2.5)), float(
        np.percentile(values, 97.5)
    )
    kept = bool(
        rho_first * rho_second > 0
        and abs(rho) >= 0.1
        and (low > 0 or high < 0)
    )
    return Copula(
        rho=rho if kept else 0.0,
        kept=kept,
        shared_dates=len(shared),
        halves=(rho_first, rho_second),
        ci=(low, high),
    )


def build_clocks(
    city_day: pa.Table,
    metro_daily: pa.Table,
    flight_events: pa.Table,
    significant: pa.Table,
    *,
    window_start: date,
    window_end: date,
    holdout_start: date,
    holdout_end: date,
    line_open: date,
    local_class: str,
) -> Clocks:
    """Fit both marginals, the copula, and the aviation dailies.

    Every date argument comes from the city profile: the alignment
    window, the holdout (the station-hour service spans), the line
    that opened mid-window, and the local-event class name.
    """
    span = max(1, (window_end - window_start).days + 1)
    ride_counts = {
        day: value
        for day, value in _day_counts(city_day, "date", "done_ride").items()
        if _in_window(day, window_start, window_end)
    }
    metro_counts = {
        day: value
        for day, value in _day_counts(
            metro_daily, "record_date", "total"
        ).items()
        if _in_window(day, window_start, window_end)
    }
    local_dates = {
        str(day)[:10]
        for day, locality in zip(
            significant.column("date").to_pylist(),
            significant.column("locality").to_pylist(),
            strict=True,
        )
        if locality == local_class
    }
    covered = covered_counts(flight_events)
    arrivals = {day: float(v) for day, v in covered["arrival"].items()}
    departures = {day: float(v) for day, v in covered["departure"].items()}
    ride_hail = _fit_marginal(
        "ride_hail",
        ride_counts,
        local_dates,
        span,
        arrivals,
        departures,
        window_start=window_start,
        regime=None,
        holdout_start=holdout_start,
        holdout_end=holdout_end,
    )
    metro = _fit_marginal(
        "metro",
        metro_counts,
        local_dates,
        span,
        arrivals,
        departures,
        window_start=window_start,
        regime=line_open,
        holdout_start=holdout_start,
        holdout_end=holdout_end,
    )
    copula = _fit_copula(
        ride_hail,
        metro,
        ride_counts,
        metro_counts,
        local_dates,
        span,
        window_start=window_start,
        holdout_start=holdout_start,
        regime=line_open,
    )
    aviation = _fit_aviation_daily(flight_events, window_start, window_end)
    evaluation = {
        "schema": "resampler-evaluation/1",
        "protocol": {
            "fit": (
                f"{window_start.isoformat()} -> "
                f"{(holdout_start - timedelta(days=1)).isoformat()}"
            ),
            "holdout": (
                f"{holdout_start.isoformat()} -> "
                f"{holdout_end.isoformat()} (the station-hour "
                "service spans; a mid-holdout line opening splits it)"
            ),
            "primary_score": "CRPS; point baselines score as MAE",
            "improvement_rule": IMPROVEMENT_RULE,
            "calibration_tolerance": CALIBRATION_TOLERANCE,
            "block_days": 7,
            "bootstrap_draws": 1000,
            "bootstrap_seed": EVALUATION_SEED,
            "refit": (
                "selected spec through "
                f"{window_end.isoformat()}, holdout frozen"
            ),
        },
        "ride_hail": ride_hail.record,
        "metro": metro.record,
        "copula": {
            "shared_dates": copula.shared_dates,
            "rho": copula.rho,
            "kept": copula.kept,
            "halves": list(copula.halves),
            "ci": list(copula.ci),
            "note": (
                "association between modes after calendar and trend "
                "structure; not a causal effect"
            ),
        },
        "aviation_daily": {
            direction: {"phi": fit.phi}
            for direction, fit in aviation.items()
        },
        "notes": [
            "Missing dates drop out of every fit and score. No date "
            "is zero-filled.",
            "DGCA monthly passenger counts stay context only.",
        ],
    }
    return Clocks(
        ride_hail=ride_hail,
        metro=metro,
        copula=copula,
        aviation=aviation,
        evaluation=evaluation,
    )


def aviation_mean_for(
    fit: NB2Fit, day: str, span_days: int, window_start: date
) -> float:
    """Predicted mean event count for one synthetic aviation day."""
    design = _matrix(
        [day], span_days, set(), window_start=window_start
    )
    return float(fit.mean(design)[0])
