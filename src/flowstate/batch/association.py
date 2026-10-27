"""Weekday-adjusted slope. The estimate is an association, not a cause."""

from __future__ import annotations

import math


def weekday_adjusted_slope(
    rows: list[tuple[float, float, int]],
) -> dict[str, float | int | None]:
    """OLS of y on x and a weekday flag.

    Each row is ``(y, x, weekday)`` with weekday 1 or 0. The estimate
    is the coefficient on x. The interval is that coefficient ± 1.96
    standard errors. Fewer than four rows has no estimate.
    """
    n = len(rows)
    empty: dict[str, float | int | None] = {
        "n": n,
        "estimate": None,
        "interval_low": None,
        "interval_high": None,
    }
    if n < 4:
        return empty
    # Columns: intercept, x, weekday.
    xtx = [[0.0, 0.0, 0.0] for _ in range(3)]
    xty = [0.0, 0.0, 0.0]
    for y, x, weekday in rows:
        design = (1.0, float(x), float(weekday))
        for i in range(3):
            xty[i] += design[i] * float(y)
            for j in range(3):
                xtx[i][j] += design[i] * design[j]
    beta = _solve(xtx, xty)
    if beta is None:
        return empty
    residual = 0.0
    for y, x, weekday in rows:
        fitted = beta[0] + beta[1] * float(x) + beta[2] * float(weekday)
        residual += (float(y) - fitted) ** 2
    sigma2 = residual / (n - 3)
    inverse = _invert(xtx)
    if inverse is None:
        return empty
    se = math.sqrt(max(sigma2 * inverse[1][1], 0.0))
    estimate = beta[1]
    return {
        "n": n,
        "estimate": estimate,
        "interval_low": estimate - 1.96 * se,
        "interval_high": estimate + 1.96 * se,
    }


def _solve(
    matrix: list[list[float]], target: list[float]
) -> list[float] | None:
    size = len(target)
    aug = [row[:] + [target[i]] for i, row in enumerate(matrix)]
    for col in range(size):
        pivot = max(range(col, size), key=lambda r: abs(aug[r][col]))
        if abs(aug[pivot][col]) < 1e-12:
            return None
        aug[col], aug[pivot] = aug[pivot], aug[col]
        scale = aug[col][col]
        for j in range(col, size + 1):
            aug[col][j] /= scale
        for row in range(size):
            if row == col:
                continue
            factor = aug[row][col]
            for j in range(col, size + 1):
                aug[row][j] -= factor * aug[col][j]
    return [aug[i][size] for i in range(size)]


def _invert(matrix: list[list[float]]) -> list[list[float]] | None:
    size = len(matrix)
    out = []
    for col in range(size):
        target = [1.0 if i == col else 0.0 for i in range(size)]
        solved = _solve(matrix, target)
        if solved is None:
            return None
        out.append(solved)
    return [list(row) for row in zip(*out)]
