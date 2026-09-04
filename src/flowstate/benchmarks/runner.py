"""Timed Spark vs MapReduce at 1M/10M/100M."""

from __future__ import annotations

from pathlib import Path

RESULTS = Path("src/flowstate/benchmarks/results/timings.csv")


def main() -> None:
    print(f"TODO: time Spark vs MR; append rows to {RESULTS}")


if __name__ == "__main__":
    main()
