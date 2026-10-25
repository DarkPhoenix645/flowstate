"""Parent/child conservation. No Spark and no corpus."""

from flowstate.batch.conservation import compare


def _parents() -> dict[str, dict[str, int]]:
    return {
        "ridehail": {"day-1|booking": 4, "day-1|done_ride": 3},
        "metro_entries": {"metro-1": 10},
        "metro_exits": {"metro-1": 10},
        "bus": {"trip-1|boardings": 6, "trip-1|alightings": 6},
        "aviation": {"flight-1|observed_rows": 2},
    }


def _children() -> dict[str, dict[str, int]]:
    return {
        "ridehail": {"day-1|booking": 4, "day-1|done_ride": 3},
        "metro_entries": {"metro-1": 10},
        "metro_exits": {"metro-1": 10},
        "bus": {"trip-1|boardings": 6, "trip-1|alightings": 6},
        "aviation": {"flight-1|observed_rows": 2},
    }


def test_matching_children_have_no_mismatches():
    checks, issues = compare(
        _parents(), _children(), rows=25, expected_rows=25
    )
    assert issues == []
    assert checks > 0


def test_a_ridehail_child_that_misses_the_parent_fails():
    children = _children()
    children["ridehail"]["day-1|done_ride"] = 2
    _checks, issues = compare(
        _parents(), children, rows=24, expected_rows=25
    )
    assert any("done_ride" in issue for issue in issues)
    assert any("corpus rows" in issue for issue in issues)


def test_bus_boardings_must_equal_alightings():
    children = _children()
    children["bus"]["trip-1|alightings"] = 5
    _checks, issues = compare(
        _parents(), children, rows=25, expected_rows=25
    )
    assert any("alightings" in issue for issue in issues)


def test_synthetic_aviation_inside_an_observed_window_fails():
    children = _children()
    children["aviation"]["flight-1|synthetic_rows"] = 1
    _checks, issues = compare(
        _parents(), children, rows=26, expected_rows=26
    )
    assert any("observed window" in issue for issue in issues)
