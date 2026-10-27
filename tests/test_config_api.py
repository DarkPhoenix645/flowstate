"""Model-config HTTP API."""

from __future__ import annotations

import json
import os
import threading
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import psycopg
import pytest

from flowstate.config.api import dispatch
from flowstate.config.postgres import ConfigStore
from flowstate.config.seed import seed

_SECRET = "s3cret-token"
_LOCAL_URL = (
    "postgresql://flowstate:flowstate@127.0.0.1:5432/flowstate"
)


@pytest.fixture
def database_url():
    url = os.environ.get("FLOWSTATE_CONFIG_DATABASE_URL", _LOCAL_URL)
    try:
        with psycopg.connect(url, connect_timeout=2) as conn:
            conn.execute("SELECT version FROM schema_version")
    except psycopg.OperationalError:
        pytest.skip("Postgres is not available")
    with psycopg.connect(url) as conn:
        conn.execute(
            "TRUNCATE config_active, config_version RESTART IDENTITY"
        )
    return url


def _request(base: str, method: str, path: str, body: dict | None = None):
    data = None if body is None else json.dumps(body).encode()
    request = Request(base + path, data=data, method=method)
    if data is not None:
        request.add_header("Content-Type", "application/json")
    try:
        with urlopen(request, timeout=5) as response:
            return response.status, json.load(response)
    except HTTPError as exc:
        return exc.code, json.load(exc)


def test_an_invalid_put_is_422_and_hides_the_url():
    store = ConfigStore(
        f"postgresql://flowstate:{_SECRET}@127.0.0.1:1/flowstate"
    )
    status, body = dispatch(
        store,
        "PUT",
        "/api/model-config/resample_config",
        json.dumps({"cab_share": 2}).encode(),
    )
    assert status == 422
    assert _SECRET not in json.dumps(body)
    status, body = dispatch(store, "GET", "/api/missing", b"")
    assert status == 404


@pytest.mark.integration
def test_http_round_trip(database_url):
    from flowstate.config.api import serve

    seed(database_url)
    server = serve(ConfigStore(database_url), "127.0.0.1", 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    port = server.server_address[1]
    base = f"http://127.0.0.1:{port}"
    try:
        status, active = _request(
            base, "GET", "/api/model-config/scenario_policy"
        )
        assert status == 200
        assert active["source"] == "db"
        assert active["config_hash"]
        status, listed = _request(
            base, "GET", "/api/model-config/resample_config/versions"
        )
        assert status == 200
        assert listed["versions"]
        status, denied = _request(
            base,
            "PUT",
            "/api/model-config/resample_config",
            {"bus_peak_share_low": 0.9, "bus_peak_share_high": 0.1},
        )
        assert status == 422
        status, saved = _request(
            base,
            "PUT",
            "/api/model-config/resample_config",
            {"note": "api", "content": {"outside_widen": 0.25}},
        )
        assert status == 200
        assert saved["source"] == "db"
        assert saved["content"]["outside_widen"] == 0.25
        first = listed["versions"][0]["version_id"]
        status, diff = _request(
            base,
            "GET",
            "/api/model-config/resample_config/diff"
            f"?from={first}&to={saved['version_id']}",
        )
        assert status == 200
        assert "outside_widen" in diff["changes"]
        status, restored = _request(
            base,
            "POST",
            "/api/model-config/resample_config/activate",
            {"version_id": first},
        )
        assert status == 200
        assert restored["version_id"] == first
        status, cleared = _request(
            base, "POST", "/api/model-config/resample_config/deactivate"
        )
        assert status == 200 and cleared["active"] is False
        status, missing = _request(
            base, "GET", "/api/model-config/resample_config"
        )
        assert status == 404
        assert database_url not in json.dumps(missing)
    finally:
        server.shutdown()
        server.server_close()
        seed(database_url)
