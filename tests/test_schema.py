import pytest

from flowstate.schema import (
    ASSIGNED_VEHICLE_TYPE,
    DOCUMENTED_VEHICLE_TYPES,
    MODES,
    RIDE_COLUMNS,
    ZONE_ID_BY_MODE,
    resolve_vehicle_type,
    ride_json_schema,
    ride_spark_schema,
)


def test_ride_columns_stable():
    assert "trip_id" in RIDE_COLUMNS
    assert "pickup_zone" in RIDE_COLUMNS
    assert RIDE_COLUMNS[0] == "trip_id"


def test_json_schema_matches_ride_columns():
    schema = ride_json_schema()
    assert list(schema["properties"]) == RIDE_COLUMNS
    assert schema["required"] == RIDE_COLUMNS
    for name in ("pickup_zone", "drop_zone", "vehicle_type", "timestamp"):
        assert schema["properties"][name]["type"] == ["string", "null"]
    assert schema["properties"]["timestamp"]["format"] == "date-time"
    assert schema["properties"]["fare"]["type"] == ["number", "null"]


def test_spark_struct_matches_ride_columns():
    fields = ride_spark_schema().fields
    assert [f.name for f in fields] == RIDE_COLUMNS
    types = {f.name: f.dataType.simpleString() for f in fields}
    assert types["pickup_zone"] == "string"
    assert types["drop_zone"] == "string"
    assert types["vehicle_type"] == "string"
    assert types["timestamp"] == "string"
    assert types["fare"] == "double"


def test_zone_ids_cover_every_mode():
    assert tuple(ZONE_ID_BY_MODE) == MODES
    assert set(ZONE_ID_BY_MODE.values()) == {
        "ward_id",
        "station_id",
        "stop_id",
        "airport",
    }


def test_vehicle_type_assignment_and_pass_through():
    assert resolve_vehicle_type("ride_hail") == "auto"
    assert resolve_vehicle_type("ride_hail", "Auto") == "auto"
    assert resolve_vehicle_type("ride_hail", "Cab") == "cab"
    assert resolve_vehicle_type("metro") == "train"
    assert resolve_vehicle_type("bus") == "bus"
    assert resolve_vehicle_type("bus", "Bus") == "bus"
    assert resolve_vehicle_type("aviation") == "airplane"
    assert resolve_vehicle_type("ride_hail", "All") == "All"
    assert resolve_vehicle_type("bus", "3") == "3"
    assert resolve_vehicle_type("ride_hail", "") == "auto"
    assert set(ASSIGNED_VEHICLE_TYPE) == {
        "ride_hail",
        "metro",
        "bus",
        "aviation",
    }
    assert DOCUMENTED_VEHICLE_TYPES == (
        "auto",
        "cab",
        "train",
        "bus",
        "airplane",
    )
    with pytest.raises(ValueError, match="tram"):
        resolve_vehicle_type("tram")
