"""Compare corpus child sums with the amplify manifest parents.

Stdlib only. The Spark job imports this module. The Compose Spark
image does not have pydantic, so this file must not import it.
"""

from __future__ import annotations


def compare(
    parents: dict[str, dict[str, int]],
    children: dict[str, dict[str, int]],
    *,
    rows: int,
    expected_rows: int,
) -> tuple[int, list[str]]:
    """Return ``(checks, mismatches)``. An empty list means conserved."""
    issues: list[str] = []
    checks = 0
    checks += _match_map(
        "ride_hail",
        _ints(parents.get("ridehail")),
        _ints(children.get("ridehail")),
        issues,
    )
    checks += _match_map(
        "metro entries",
        _ints(parents.get("metro_entries")),
        _ints(children.get("metro_entries")),
        issues,
    )
    checks += _match_map(
        "metro exits",
        _ints(parents.get("metro_exits")),
        _ints(children.get("metro_exits")),
        issues,
    )
    recorded_bus = _ints(parents.get("bus"))
    child_bus = _ints(children.get("bus"))
    checks += _match_map("bus", recorded_bus, child_bus, issues)
    checks += _bus_balance(recorded_bus, child_bus, issues)
    recorded_air = _ints(parents.get("aviation"))
    child_air = _ints(children.get("aviation"))
    checks += _match_map("aviation", recorded_air, child_air, issues)
    checks += _aviation_overlap(recorded_air, child_air, issues)
    checks += 1
    if rows != expected_rows:
        issues.append(
            f"corpus rows {rows} != manifest rows {expected_rows}"
        )
    return checks, issues


def _ints(raw: dict[str, int] | None) -> dict[str, int]:
    if not raw:
        return {}
    return {str(key): int(value) for key, value in raw.items()}


def _match_map(
    label: str,
    recorded: dict[str, int],
    actual: dict[str, int],
    issues: list[str],
) -> int:
    keys = set(recorded) | set(actual)
    for key in sorted(keys):
        left = recorded.get(key, 0)
        right = actual.get(key, 0)
        if left != right:
            issues.append(
                f"{label} {key}: child {right} != parent {left}"
            )
    return len(keys)


def _parent(key: str) -> str:
    parent, _metric = key.split("|", 1)
    return parent


def _bus_balance(
    recorded: dict[str, int],
    actual: dict[str, int],
    issues: list[str],
) -> int:
    parents = {_parent(key) for key in set(recorded) | set(actual)}
    for parent in sorted(parents):
        boardings = actual.get(f"{parent}|boardings", 0)
        alightings = actual.get(f"{parent}|alightings", 0)
        if boardings != alightings:
            issues.append(
                f"bus {parent}: boardings {boardings} != "
                f"alightings {alightings}"
            )
    return len(parents)


def _aviation_overlap(
    recorded: dict[str, int],
    actual: dict[str, int],
    issues: list[str],
) -> int:
    observed = {
        _parent(key)
        for key, value in recorded.items()
        if key.endswith("|observed_rows") and value
    }
    for parent in sorted(observed):
        synthetic = actual.get(f"{parent}|synthetic_rows", 0)
        if synthetic:
            issues.append(
                f"aviation {parent}: synthetic rows {synthetic} "
                "inside an observed window"
            )
    return len(observed)
