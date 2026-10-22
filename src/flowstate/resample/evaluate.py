"""Fit and record the frozen resampler evaluation.

``make evaluate`` runs the held-out protocol (ADR-0010) and writes
the record twice: beside the corpus for the run, and to the
committed results directory as review evidence. The fit is
deterministic, so the committed record always rebuilds.
"""

from __future__ import annotations

import argparse

from flowstate.config import get_settings
from flowstate.resample.writer import COMMITTED_EVALUATION, write_evaluation


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="flowstate.resample.evaluate")
    parser.parse_args(argv)
    evaluation = write_evaluation(get_settings())
    ride = evaluation["ride_hail"]["aviation_terms"]
    metro = evaluation["metro"]["aviation_terms"]
    print(f"evaluation written to {COMMITTED_EVALUATION}")
    print(
        "ride_hail aviation terms kept: "
        f"{ride.get('keep_aviation_terms')} "
        f"(improvement {ride.get('improvement')})"
    )
    print(
        "metro aviation terms kept: "
        f"{metro.get('keep_aviation_terms')} "
        f"(improvement {metro.get('improvement')})"
    )
    print(
        "copula rho kept: "
        f"{evaluation['copula']['kept']} "
        f"(rho {evaluation['copula']['rho']:.4f})"
    )


if __name__ == "__main__":
    main()
