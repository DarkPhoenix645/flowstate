"""Env-driven paths and Docker-stack endpoints."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="FLOWSTATE_",
        env_file=".env",
        extra="ignore",
    )

    data_dir: Path = Path("data")
    kafka_bootstrap_servers: str = "localhost:9094"
    spark_master: str = "spark://spark:7077"
    hdfs_namenode: str = "hdfs://namenode:9000"

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
