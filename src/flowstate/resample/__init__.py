"""The calibrated multimodal resampler. One module, two consumers.

ADR-0010. Layer 1 (aggregate clocks) is :mod:`clocks`, layer 2
(shape priors) is :mod:`profiles`, layer 3 (event unfolding) is
:mod:`unfold`. :mod:`bundle` fits and versions the model bundle,
:mod:`writer` orchestrates ``make amplify``, :mod:`checks` holds the
pre-emptive event checks, and :mod:`envelope` maps corpus rows onto
the ADR-0008 Kafka contract for the producer (#39).

There is no second generator. The producer imports this package.
"""

from flowstate.resample.bundle import (
    BUNDLE_VERSION,
    CitySpec,
    ModelBundle,
    ResampleConfig,
    build_bundle,
    bundle_hash,
    bundle_json,
    city_spec,
    load_inputs,
)
from flowstate.resample.checks import CorpusError, validate_written
from flowstate.resample.envelope import to_envelope
from flowstate.resample.writer import corpus_dir, run_amplify, write_evaluation

__all__ = [
    "BUNDLE_VERSION",
    "CitySpec",
    "CorpusError",
    "ModelBundle",
    "ResampleConfig",
    "build_bundle",
    "bundle_hash",
    "bundle_json",
    "city_spec",
    "corpus_dir",
    "load_inputs",
    "run_amplify",
    "to_envelope",
    "validate_written",
    "write_evaluation",
]
