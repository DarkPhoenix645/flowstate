"""Negative-binomial (NB2) count model, scores, and normal helpers.

Layer 1 math for ADR-0010. Pure functions plus numpy. No dataset
reads here. Fitting is IRLS (Fisher scoring) on the log link with a
profile-likelihood step for the dispersion ``phi``.

Scores follow docs/resampling-plan.md §10.2: the primary score is
the exact CRPS of the fitted NB2 (point baselines score as their
MAE), the NB log score is reported beside it, and MAE stays
secondary. Correlation alone never passes.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np

# Fixed seed for evaluation randomness (block bootstrap). Evaluation
# must not depend on the run seed: the 5% decision has to be stable.
EVALUATION_SEED = 20200101
BLOCK_DAYS = 7
BOOTSTRAP_DRAWS = 1000
# A 90% predictive interval whose empirical coverage drifts more than
# this from nominal counts as "materially worse calibration".
CALIBRATION_TOLERANCE = 0.05
IMPROVEMENT_RULE = 0.05


@dataclass(frozen=True)
class NB2Fit:
    """One fitted NB2 marginal.

    ``beta`` are log-link coefficients for the design matrix columns,
    ``phi`` is the NB2 dispersion (Var = mu + mu^2 / phi).
    ``names`` label the columns for the bundle record.
    """

    beta: np.ndarray
    phi: float
    names: tuple[str, ...]

    def mean(self, design: np.ndarray) -> np.ndarray:
        return np.exp(design @ self.beta)


def fit_nb2(
    design: np.ndarray, counts: np.ndarray, names: tuple[str, ...]
) -> NB2Fit:
    """Fit NB2 by alternating IRLS and a log-phi golden-section search.

    Missing observations are not allowed here: the caller passes only
    valid dates. A missing date is never a zero row.
    """
    if design.ndim != 2 or design.shape[0] != len(counts):
        raise ValueError("design and counts do not align")
    if len(counts) <= design.shape[1]:
        raise ValueError("more covariates than observations")

    phi = _phi_moment(counts, _ols_mean(design, counts))
    beta = _irls(design, counts, phi)
    for _ in range(20):
        phi = _fit_phi(design, counts, beta, phi)
        new_beta = _irls(design, counts, phi)
        if np.max(np.abs(new_beta - beta)) < 1e-8:
            beta = new_beta
            break
        beta = new_beta
    return NB2Fit(beta=beta, phi=float(phi), names=names)


def _ols_mean(design: np.ndarray, counts: np.ndarray) -> np.ndarray:
    start = np.linalg.lstsq(design, np.log(np.maximum(counts, 1.0)), rcond=None)
    return np.exp(np.clip(design @ start[0], -10.0, 10.0))


def _irls(
    design: np.ndarray, counts: np.ndarray, phi: float
) -> np.ndarray:
    beta = np.zeros(design.shape[1])
    beta[0] = math.log(max(counts.mean(), 1.0))
    for _ in range(200):
        mu = np.exp(np.clip(design @ beta, -20.0, 20.0))
        work = mu / (1.0 + mu / phi)
        information = design.T @ (work[:, None] * design)
        score = design.T @ ((counts - mu) / (1.0 + mu / phi))
        step = np.linalg.solve(
            information + np.eye(design.shape[1]) * 1e-10, score
        )
        beta = beta + step
        if np.max(np.abs(step)) < 1e-9:
            break
    return beta


def _fit_phi(
    design: np.ndarray, counts: np.ndarray, beta: np.ndarray, phi: float
) -> float:
    lo, hi = math.log10(max(phi / 100.0, 1e-3)), math.log10(phi * 100.0)
    ratio = (math.sqrt(5.0) - 1.0) / 2.0
    a = lo
    b = hi
    c = b - ratio * (b - a)
    d = a + ratio * (b - a)
    fc = -_nb_loglik(design, counts, beta, 10**c)
    fd = -_nb_loglik(design, counts, beta, 10**d)
    for _ in range(60):
        if fc < fd:
            b, d, fd = d, c, fc
            c = b - ratio * (b - a)
            fc = -_nb_loglik(design, counts, beta, 10**c)
        else:
            a, c, fc = c, d, fd
            d = a + ratio * (b - a)
            fd = -_nb_loglik(design, counts, beta, 10**d)
        if abs(b - a) < 1e-6:
            break
    return 10 ** ((a + b) / 2.0)


def _phi_moment(counts: np.ndarray, mu: np.ndarray) -> float:
    var = float(np.var(counts, ddof=1))
    excess = max(var - float(mu.mean()), float(mu.mean()) * 0.01)
    return max(float(mu.mean()) ** 2 / excess, 0.5)


def _nb_loglik(
    design: np.ndarray, counts: np.ndarray, beta: np.ndarray, phi: float
) -> float:
    mu = np.exp(np.clip(design @ beta, -20.0, 20.0))
    total = 0.0
    for y, m in zip(counts, mu, strict=True):
        total += (
            math.lgamma(y + phi)
            - math.lgamma(phi)
            - math.lgamma(y + 1.0)
            + phi * math.log(phi / (phi + m))
            + y * math.log(m / (phi + m))
        )
    return total


def nb_pmf(mu: float, phi: float, support: int) -> np.ndarray:
    """NB2 pmf over 0..support, built in log space.

    The forward recurrence underflows for large daily counts
    (phi * log1p(mu/phi) falls below the double range), so the
    log-pmf is the cumsum of log ratios, shifted by its max before
    exp, then renormalized.
    """
    if mu <= 0 or phi <= 0:
        raise ValueError(f"bad nb2 params mu={mu}, phi={phi}")
    k = np.arange(support + 1, dtype=float)
    log_p = np.empty(support + 1)
    base = phi * math.log(phi / (mu + phi))
    log_p[0] = base
    if support >= 1:
        steps = np.log(k[:-1] + phi) - np.log(k[:-1] + 1.0)
        log_p[1:] = base + k[1:] * math.log(mu / (mu + phi)) + np.cumsum(
            steps
        )
    pmf = np.exp(log_p - log_p.max())
    total = float(pmf.sum())
    if total < 0.999:
        raise ValueError(
            f"pmf support too small for mu={mu}, phi={phi} "
            f"(mass {total:.6f} over {support + 1} points)"
        )
    return pmf / total


def support_size(mu: float, phi: float) -> int:
    """Support reaching the 12-sigma quantile, memory-guarded."""
    spread = math.sqrt(mu + mu * mu / phi)
    return int(min(math.ceil(mu + 12.0 * spread), 8_000_000))


def nb_cdf(mu: float, phi: float) -> np.ndarray:
    return np.cumsum(nb_pmf(mu, phi, support_size(mu, phi)))


def crps(mu: float, phi: float, observed: int) -> float:
    """Exact CRPS of NB2 at one observation.

    CRPS = E|X - y| - 0.5 E|X - X'| with E|X - X'| summed by the
    lattice identity 2 * sum_m F(m) (1 - F(m)).
    """
    pmf = nb_pmf(mu, phi, support_size(mu, phi))
    support = np.arange(len(pmf))
    abs_dev = float(pmf @ np.abs(support - observed))
    cdf = np.cumsum(pmf)
    half_pair = float(np.sum(cdf * (1.0 - cdf)))
    return abs_dev - half_pair


def log_score(mu: float, phi: float, observed: int) -> float:
    pmf = nb_pmf(mu, phi, max(support_size(mu, phi), observed))
    return -math.log(max(float(pmf[observed]), 1e-300))


def predictive_interval(
    mu: float, phi: float, level: float = 0.90
) -> tuple[int, int]:
    cdf = nb_cdf(mu, phi)
    low = int(np.searchsorted(cdf, (1.0 - level) / 2.0))
    high = int(np.searchsorted(cdf, 1.0 - (1.0 - level) / 2.0))
    return low, high


def nb_quantile(u: float, mu: float, phi: float) -> int:
    """Inverse CDF at u in [0, 1). Used by the Gaussian copula."""
    if not 0.0 <= u < 1.0:
        raise ValueError(f"u outside [0, 1): {u}")
    cdf = nb_cdf(mu, phi)
    return int(np.searchsorted(cdf, u))


def normal_cdf(x: float) -> float:
    return 0.5 * math.erfc(-x / math.sqrt(2.0))


def normal_ppf(u: float) -> float:
    """Inverse standard normal CDF by bisection on :func:`normal_cdf`.

    80 halvings from [-40, 40] reach double precision. Copula work
    calls this O(hundreds) of times, so the erfc cost is negligible.
    """
    if not 0.0 < u < 1.0:
        raise ValueError(f"u outside (0, 1): {u}")
    low, high = -40.0, 40.0
    for _ in range(80):
        mid = (low + high) / 2.0
        if normal_cdf(mid) < u:
            low = mid
        else:
            high = mid
    return (low + high) / 2.0


def improvement_ci(
    per_date: dict[str, tuple[float, float]],
) -> tuple[float, float, float]:
    """Block-bootstrap CI for the relative score improvement.

    ``per_date`` maps a held-out date to (baseline score, model
    score). Improvement is (baseline - model) / baseline summed over
    resampled 7-day date blocks. Returns (low, point, high).
    """
    dates = sorted(per_date)

    def improvement(sample: list[str]) -> float:
        base = sum(per_date[d][0] for d in sample)
        model = sum(per_date[d][1] for d in sample)
        return (base - model) / base

    point = improvement(dates)
    rng = np.random.default_rng(EVALUATION_SEED)
    blocks = [
        dates[i : i + BLOCK_DAYS] for i in range(0, len(dates), BLOCK_DAYS)
    ]
    values = []
    for _ in range(BOOTSTRAP_DRAWS):
        picked = rng.integers(0, len(blocks), size=len(blocks))
        sample = [day for index in picked for day in blocks[index]]
        values.append(improvement(sample))
    return float(np.percentile(values, 2.5)), point, float(
        np.percentile(values, 97.5)
    )
