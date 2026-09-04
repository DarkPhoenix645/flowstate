from flowstate.schema import RIDE_COLUMNS


def test_ride_columns_stable():
    assert "trip_id" in RIDE_COLUMNS
    assert "pickup_zone" in RIDE_COLUMNS
    assert RIDE_COLUMNS[0] == "trip_id"
