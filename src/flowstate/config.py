"""Env-driven paths and Docker-stack endpoints."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# BMTC boarding prior (#40, ADR-0012). Empty env means the default;
# a present value overrides. The defaults are model choices, not
# BMTC measurements. Evidence: docs/bmtc-boarding-prior.md.
_BUS_PRIOR_FIELDS = (
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


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="FLOWSTATE_",
        env_file=".env",
        extra="ignore",
    )

    data_dir: Path = Path("data")
    # Source extracts. Same directory as raw_dir.
    datasets_dir: Path = Path("data/raw")
    kafka_bootstrap_servers: str = "localhost:9094"
    spark_master: str = "spark://spark:7077"
    hdfs_namenode: str = "hdfs://namenode:9000"
    google_maps_api_key: str = ""
    # BMTC synthetic-boarding prior (#40). assumed_bus_capacity is
    # seats, never seats plus standing: 41.3 is a dated fleet
    # average seat count (Karnataka Economic Survey 2018-19), not
    # total capacity. assumed_max_load_factor is a synthetic
    # guardrail, not a statutory or observed peak load.
    assumed_weekday_boardings: int = 4_600_000
    assumed_bus_capacity: float = 41.3
    assumed_max_load_factor: float = 1.4
    metro_substitution_multiplier: float = 1.0
    # metro_ridehail_blend until a versioned 24-hour BMTC table
    # exists. Do not invent hourly weights from the 47-51% summary.
    bus_hour_profile: str = "metro_ridehail_blend"
    bus_peak_share_low: float = 0.47
    bus_peak_share_high: float = 0.51
    bus_trip_length_km_low: float = 10.7
    bus_trip_length_km_high: float = 10.9

    @field_validator(*_BUS_PRIOR_FIELDS, mode="before")
    @classmethod
    def _empty_env_means_default(
        cls, value: object, info: Any
    ) -> object:
        """An empty or blank env value falls back to the default."""
        if isinstance(value, str) and not value.strip():
            return cls.model_fields[info.field_name].default
        return value


    def opensky_clients(self) -> list[tuple[int, str, str]]:
        """Up to 10 OpenSky client pairs.

        Pair 1: FLOWSTATE_OPENSKY_CLIENT_ID and
        FLOWSTATE_OPENSKY_CLIENT_SECRET.
        Pairs 2-10: the same names with a _2 .. _10 suffix.
        """
        pairs: list[tuple[int, str, str]] = []
        for index in range(1, 11):
            suffix = "" if index == 1 else f"_{index}"
            client_id = os.environ.get(
                f"FLOWSTATE_OPENSKY_CLIENT_ID{suffix}", ""
            ).strip()
            secret = os.environ.get(
                f"FLOWSTATE_OPENSKY_CLIENT_SECRET{suffix}", ""
            ).strip()
            if client_id and secret:
                pairs.append((index, client_id, secret))
            elif client_id or secret:
                raise RuntimeError(
                    f"OpenSky pair {index} is incomplete. Set both "
                    f"FLOWSTATE_OPENSKY_CLIENT_ID{suffix} and "
                    f"FLOWSTATE_OPENSKY_CLIENT_SECRET{suffix}, or neither."
                )
        return pairs

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def staged_dir(self) -> Path:
        return self.data_dir / "staged"

    @property
    def lake_dir(self) -> Path:
        return self.data_dir / "lake"

    @property
    def stream_sink_dir(self) -> Path:
        return self.data_dir / "stream-sink"

    def ensure_dirs(self) -> None:
        for p in (
            self.raw_dir,
            self.staged_dir,
            self.lake_dir,
            self.stream_sink_dir,
        ):
            p.mkdir(parents=True, exist_ok=True)


def get_settings() -> Settings:
    # Load .env into os.environ so non-FLOWSTATE keys (e.g. KAGGLE_API_TOKEN)
    # are visible to kagglehub and other clients.
    load_dotenv()
    return Settings()


def check() -> int:
    from flowstate.log import configure_logging

    configure_logging()
    log = logging.getLogger(__name__)
    settings = get_settings()
    settings.ensure_dirs()
    log.info("data_dir=%s", settings.data_dir.resolve())
    log.info("kafka=%s", settings.kafka_bootstrap_servers)
    log.info("spark_master=%s", settings.spark_master)
    log.info("config check passed")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(prog="flowstate.config")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if args.check:
        sys.exit(check())
    parser.print_help()


if __name__ == "__main__":
    main()
