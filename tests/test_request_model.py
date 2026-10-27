"""One request model per entrypoint. Defaults are not copied."""

from __future__ import annotations

import inspect
from pathlib import Path

import pytest

from flowstate.ingest.amplify import AmplifyRequest, parse_amplify
from flowstate.normalize.__main__ import NormalizeRequest, parse_normalize
from flowstate.streaming.control import StartRequest, default_window
from flowstate.streaming.ctl import build
from flowstate.streaming.producer import parse_producer, produce
from flowstate.streaming.service import main as service_main
from flowstate.streaming.service import serve
from flowstate.streaming.topics import CONTROL_TOPIC


def _ctl_start(argv: list[str]) -> StartRequest:
    _topic, _key, value = build(["start", *argv])
    assert _topic == CONTROL_TOPIC
    return StartRequest.model_validate(value["start"])


def test_empty_flags_match_start_request():
    bare = StartRequest()
    mode, producer, dry = parse_producer([])
    ctl = _ctl_start([])
    assert dry is False
    assert mode == bare.mode == ctl.mode
    assert producer.rows == ctl.rows == bare.rows
    assert producer.seed == ctl.seed == bare.seed
    assert producer.acceleration == ctl.acceleration == bare.acceleration
    assert producer.max_eps == ctl.max_eps == bare.max_eps
    assert producer.window_start is None
    assert ctl.window_start is None


def test_equal_flags_build_equal_requests():
    shared = [
        "--rows",
        "5000",
        "--seed",
        "3",
        "--acceleration",
        "12",
        "--window-start",
        "2025-08-01",
        "--window-end",
        "2025-08-02",
    ]
    _mode, by_eps, _dry = parse_producer(
        [*shared, "--events-per-sec", "77"]
    )
    _mode, by_cap, _dry = parse_producer([*shared, "--max-eps", "77"])
    ctl = _ctl_start([*shared, "--max-eps", "77"])
    ctl_alias = _ctl_start([*shared, "--events-per-sec", "77"])
    assert by_eps == by_cap == ctl == ctl_alias
    assert by_eps.max_eps == 77
    assert by_eps.rows == 5000


def test_the_window_default_is_one_spec_day():
    assert default_window("2024-10-26", None, None) == (
        "2024-10-26",
        "2024-10-26",
    )
    assert default_window("2024-10-26", "2025-08-01", None) == (
        "2025-08-01",
        "2025-08-01",
    )


def test_entrypoints_take_a_request_without_argv():
    assert inspect.signature(produce).parameters["start"].annotation == (
        "StartRequest"
    )
    assert "start" in inspect.signature(serve).parameters
    assert "argv" in inspect.signature(service_main).parameters


def test_amplify_help_names_its_own_default(capsys):
    assert parse_amplify([]) == AmplifyRequest()
    with pytest.raises(SystemExit) as caught:
        parse_amplify(["--help"])
    assert caught.value.code == 0
    text = capsys.readouterr().out
    assert "1000000" in text
    assert "42" in text


def test_normalize_geocode_defaults_on():
    assert parse_normalize([]).geocode is True
    assert parse_normalize([]).city == NormalizeRequest().city
    assert parse_normalize(["--no-geocode"]).geocode is False


def test_make_does_not_repeat_the_replay_defaults():
    text = Path("Makefile").read_text()
    assert "PRODUCE_ROWS ?=" not in text
    assert "RATE ?=" not in text
    assert "SEED ?=" not in text
    assert "ROWS ?=" not in text
    for name in (
        "src/flowstate/streaming/producer.py",
        "src/flowstate/streaming/ctl.py",
    ):
        source = Path(name).read_text()
        assert "10_000" not in source
        assert "10000" not in source
        assert "500.0" not in source
        assert "60.0" not in source
