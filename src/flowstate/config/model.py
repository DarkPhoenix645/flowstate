"""Model and policy config. Files and env. No deploy settings.

``ResampleConfig`` is the ``resample_config`` kind. The scenario
policy kind lives in ``flowstate.resample.scenario`` and gains
``multiplier_floor``. Hashes are over canonical JSON of the
validated model.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

# The nine #40 bus-prior fields. Env overrides these when the
# kind comes from files. A database version replaces the document.
BUS_PRIOR_FIELDS = (
    "assumed_weekday_boardings",
    "assumed_bus_capacity",
    "assumed_max_load_factor",
    "metro_substitution_multiplier",
    "bus_hour_profile",
    "bus_peak_share_low",
    "bus_peak_share_high",
    "bus_trip_length_km_low",
    "bus_trip_length_km_high",
)


class CoverageRule(BaseModel):
    """The OpenSky day-direction gate. Fit and generator share it."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    sparse_fraction: float = Field(default=0.5, gt=0, le=1)
    reference_quantile: float = Field(default=75, ge=0, le=100)
    failed_statuses: list[str] = Field(
        default_factory=lambda: ["failed", "quota"]
    )
    minimum_covered_days: int = Field(default=60, ge=1)


class ResampleConfig(BaseModel):
    """Layer-3 assumptions plus the coverage rule.

    The nine bus-prior fields match ``Settings``. The rest are
    generator choices. ``outside_widen`` is how far an
    out-of-window day moves toward a uniform share.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    cab_share: float = Field(default=0.22, ge=0, le=1)
    bus_hour_weight_metro: float = Field(default=0.5, ge=0, le=1)
    bus_hour_weight_ridehail: float = Field(default=0.5, ge=0, le=1)
    assumed_weekday_boardings: int = Field(default=4_600_000, gt=0)
    assumed_bus_capacity: float = Field(default=41.3, gt=0)
    assumed_max_load_factor: float = Field(default=1.4, gt=0)
    metro_substitution_multiplier: float = Field(default=1.0, ge=0)
    bus_hour_profile: str = "metro_ridehail_blend"
    bus_peak_share_low: float = Field(default=0.47, ge=0, le=1)
    bus_peak_share_high: float = Field(default=0.51, ge=0, le=1)
    bus_trip_length_km_low: float = Field(default=10.7, gt=0)
    bus_trip_length_km_high: float = Field(default=10.9, gt=0)
    fare_sigma: float = Field(default=0.30, gt=0)
    clock_vehicle_type: str = "Auto"
    provenance_note: str = (
        "cab_share is the thin post-2025-06 city-hour slice share, "
        "an uncertain prior. Bus boardings, capacity, load factor, "
        "and the rest of the boarding prior are the #40 assumption "
        "set (ADR-0012), not BMTC measurements. "
        "clock_vehicle_type is the archive token for the clock "
        "vehicle. The assumption set is FLOWSTATE_ settings."
    )
    coverage: CoverageRule = Field(default_factory=CoverageRule)
    outside_widen: float = Field(default=0.5, ge=0, le=1)

    @model_validator(mode="after")
    def _ranges(self) -> ResampleConfig:
        if self.bus_peak_share_low > self.bus_peak_share_high:
            raise ValueError("bus peak share low is above high")
        if self.bus_trip_length_km_low > self.bus_trip_length_km_high:
            raise ValueError("bus trip length low is above high")
        return self

    @classmethod
    def from_settings(cls, settings: Any) -> ResampleConfig:
        """Build a document from a Settings object.

        Unit tests use this. The running process reads Postgres
        and does not call this method.
        """
        return cls(
            assumed_weekday_boardings=settings.assumed_weekday_boardings,
            assumed_bus_capacity=settings.assumed_bus_capacity,
            assumed_max_load_factor=settings.assumed_max_load_factor,
            metro_substitution_multiplier=(
                settings.metro_substitution_multiplier
            ),
            bus_hour_profile=settings.bus_hour_profile,
            bus_peak_share_low=settings.bus_peak_share_low,
            bus_peak_share_high=settings.bus_peak_share_high,
            bus_trip_length_km_low=settings.bus_trip_length_km_low,
            bus_trip_length_km_high=settings.bus_trip_length_km_high,
        )


def canonical_json(model: BaseModel) -> str:
    """Key-sorted compact JSON. Whitespace and key order do not matter."""
    payload = model.model_dump(mode="json")
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def content_sha256(model: BaseModel) -> str:
    return hashlib.sha256(canonical_json(model).encode()).hexdigest()


def config_hash_from(parts: dict[str, str]) -> str:
    """Hash of the sorted (kind, sha256) pairs."""
    pairs = [[kind, parts[kind]] for kind in sorted(parts)]
    raw = json.dumps(pairs, separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()
