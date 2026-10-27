"""Kafka envelope for the producer and the consumer.

ADR-0008 is the contract. ADR-0016 adds ``parameters_ref`` and
``schema_version`` 1.1 on events that carry it. This module is not
the staged-parquet column set (``flowstate.schema``).

One pydantic model is the constructor. JSON Schema and the Spark
``StructType`` both come from that model. #22 imports the struct.
"""

from __future__ import annotations

from typing import Any, Literal, get_args, get_origin

from pydantic import BaseModel, ConfigDict, model_validator

PROVENANCE_STATUS = (
    "observed",
    "observed_proxy",
    "derived",
    "synthetic",
)
EVIDENCE_GRADE = (
    "aggregate_calibrated",
    "shape_prior",
    "assumption_driven",
)
MODES = ("ride_hail", "metro", "bus", "aviation")
BUS_PRIOR_PARAMETERS = "bus_prior"


class Place(BaseModel):
    """One point: an id and, when the geography table has one, a pin."""

    model_config = ConfigDict(extra="forbid")

    ward_id: str | None = None
    station_id: str | None = None
    stop_id: str | None = None
    airport: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    # How the point was made: ward_synthetic_point, ward_centroid,
    # station_pin, stop_gtfs, or airport_pin. Null when there is no pin.
    placement: str | None = None


class LocationRef(BaseModel):
    """Every place on the event.

    ``at`` is the plot point: pickup ward, station, stop, or airport.
    ``drop`` is ride-hail only. It is a second independent ward draw,
    not an observed origin-destination.
    """

    model_config = ConfigDict(extra="forbid")

    at: Place
    drop: Place | None = None


class Provenance(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal[
        "observed", "observed_proxy", "derived", "synthetic"
    ]
    source_ids: list[str]
    model_bundle_version: str | None = None
    profile_version: str | None = None
    seed_id: str | None = None
    evidence_grade: Literal[
        "aggregate_calibrated", "shape_prior", "assumption_driven"
    ]
    outside_training_window: bool = False


class RidershipProvenance(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["synthetic"]
    evidence_grade: Literal["assumption_driven"]


class Ridership(BaseModel):
    model_config = ConfigDict(extra="forbid")

    boardings: int | None = None
    alightings: int | None = None
    onboard_leaving: int | None = None
    provenance: RidershipProvenance


class RideEvent(BaseModel):
    """One ``flowstate.*.v1`` event. Mode extras stay on this model."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1.0", "1.1"]
    event_id: str
    city_id: str
    mode: Literal["ride_hail", "metro", "bus", "aviation"]
    event_type: str
    event_time: str
    emitted_at: str | None = None
    count: int
    location_ref: LocationRef
    scenario_id: str | None = None
    provenance: Provenance
    parameters_ref: str | None = None
    entries_count: int | None = None
    exits_count: int | None = None
    measure: str | None = None
    interval_start: str | None = None
    interval_end: str | None = None
    ends_at_last_train: bool | None = None
    source_sheet: str | None = None
    source_row: int | None = None
    trip_id: str | None = None
    stop_sequence: int | None = None
    feed_version: str | None = None
    ridership: Ridership | None = None
    direction: Literal["arrival", "departure"] | None = None
    icao24: str | None = None
    callsign: str | None = None
    synthetic_fare_inr: int | None = None

    @model_validator(mode="after")
    def _contract(self) -> RideEvent:
        expected = "1.1" if self.parameters_ref else "1.0"
        if self.schema_version != expected:
            raise ValueError(
                f"schema_version {self.schema_version} does not match "
                f"parameters_ref {self.parameters_ref!r}; expected {expected}"
            )
        if self.mode == "aviation" and self.direction is None:
            raise ValueError("aviation envelope requires direction")
        if self.mode == "bus" and self.ridership is None:
            raise ValueError("bus envelope requires ridership")
        if self.mode == "metro":
            if self.measure == "entry" and self.entries_count is None:
                raise ValueError("metro entry requires entries_count")
            if self.measure == "exit" and self.exits_count is None:
                raise ValueError("metro exit requires exits_count")
            if (
                self.measure is None
                and self.entries_count is None
                and self.exits_count is None
            ):
                raise ValueError(
                    "metro envelope requires an entry or exit count"
                )
        if self.provenance.status not in PROVENANCE_STATUS:
            raise ValueError("bad provenance status")
        return self


def envelope_json(payload: dict[str, Any]) -> dict[str, Any]:
    """Validate one envelope and return its JSON object.

    Keys that the caller set are kept, including explicit nulls.
    Unset mode extras are omitted.
    """
    model = RideEvent.model_validate(payload)
    return model.model_dump(mode="json", exclude_unset=True)


def envelope_json_schema() -> dict[str, Any]:
    """JSON Schema for the envelope. Generated from ``RideEvent``."""
    schema = RideEvent.model_json_schema()
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    return schema


def envelope_spark_schema() -> Any:
    """Spark ``StructType`` generated from ``RideEvent``."""
    return _spark_model(RideEvent)


def _spark_model(model: type[BaseModel]) -> Any:
    from pyspark.sql.types import StructField, StructType

    return StructType(
        [
            StructField(
                name,
                _spark_annotation(info.annotation),
                nullable=_nullable(info.annotation),
            )
            for name, info in model.model_fields.items()
        ]
    )


def _nullable(annotation: Any) -> bool:
    origin = get_origin(annotation)
    if origin is Literal:
        return False
    args = get_args(annotation)
    return type(None) in args if args else False


def _spark_annotation(annotation: Any) -> Any:
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        DoubleType,
        LongType,
        StringType,
    )

    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin is Literal:
        return StringType()
    if origin is list:
        return ArrayType(_spark_annotation(args[0]), True)
    if args:
        inner = [arg for arg in args if arg is not type(None)]
        if len(inner) == 1:
            return _spark_annotation(inner[0])
        if all(arg in (int, float) for arg in inner):
            return DoubleType()
    if annotation in (str,):
        return StringType()
    if annotation is int:
        return LongType()
    if annotation is float:
        return DoubleType()
    if annotation is bool:
        return BooleanType()
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return _spark_model(annotation)
    raise TypeError(f"no Spark type for {annotation!r}")
