"""Env-driven paths and Docker-stack endpoints."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="FLOWSTATE_",
        env_file=".env",
        extra="ignore",
    )

    data_dir: Path = Path("data")
    datasets_dir: Path = Path("flowstate-datasets")
    kafka_bootstrap_servers: str = "localhost:9094"
    spark_master: str = "spark://spark:7077"
    hdfs_namenode: str = "hdfs://namenode:9000"

    def opensky_clients(self) -> list[tuple[int, str, str]]:
        """Up to 4 OpenSky client pairs.

        Pair 1: FLOWSTATE_OPENSKY_CLIENT_ID and
        FLOWSTATE_OPENSKY_CLIENT_SECRET.
        Pairs 2-4: the same names with a _2, _3, or _4 suffix.
        Unprefixed OPENSKY_CLIENT_ID is ignored.
        """
        pairs: list[tuple[int, str, str]] = []
        for index in range(1, 5):
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
    settings = get_settings()
    settings.ensure_dirs()
    print(f"data_dir={settings.data_dir.resolve()}")
    print(f"kafka={settings.kafka_bootstrap_servers}")
    print(f"spark_master={settings.spark_master}")
    print("config check passed")
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
