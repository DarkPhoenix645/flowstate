"""GitHub rate-limit waits for the ridehail snapshot fetch."""

import requests

from flowstate.ingest.github import rate_limit_delay, too_large


def _response(
    status: int, body: bytes = b"", **headers: str
) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response._content = body
    response.headers.update(headers)
    return response


def test_retry_after_is_the_wait():
    response = _response(429, **{"Retry-After": "12"})
    assert rate_limit_delay(response) == 12


def test_low_remaining_waits_until_reset(monkeypatch):
    monkeypatch.setattr(
        "flowstate.ingest.github.time.time", lambda: 1000
    )
    response = _response(
        200,
        **{
            "X-RateLimit-Remaining": "10",
            "X-RateLimit-Reset": "1060",
        },
    )
    assert rate_limit_delay(response) == 61


def test_healthy_remaining_does_not_wait():
    response = _response(200, **{"X-RateLimit-Remaining": "4000"})
    assert rate_limit_delay(response) == 0


def test_oversized_blob_is_not_a_rate_limit():
    response = _response(
        403, b"This API returns blobs up to 1 MB in size."
    )
    assert too_large(response)
    assert rate_limit_delay(response) == 0
