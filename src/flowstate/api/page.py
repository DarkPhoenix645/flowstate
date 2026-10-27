"""Cursor pages for list endpoints. Offset is not used."""

from __future__ import annotations

import base64
import json
from typing import Any

from flowstate.config.resolve import ConfigValidationError

DEFAULT_LIMIT = 50
MAX_LIMIT = 100


def limit_of(raw: str | None) -> int:
    if raw is None or raw == "":
        return DEFAULT_LIMIT
    try:
        value = int(raw)
    except ValueError:
        raise ConfigValidationError("limit must be an integer") from None
    if value < 1 or value > MAX_LIMIT:
        raise ConfigValidationError("limit must be from 1 to 100")
    return value


def encode_cursor(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, separators=(",", ":")).encode()
    token = base64.urlsafe_b64encode(raw).decode()
    return token.rstrip("=")


def decode_cursor(token: str) -> dict[str, Any]:
    pad = "=" * (-len(token) % 4)
    try:
        payload = json.loads(base64.urlsafe_b64decode(token + pad))
    except (ValueError, json.JSONDecodeError):
        raise ConfigValidationError("cursor is not valid") from None
    if not isinstance(payload, dict):
        raise ConfigValidationError("cursor is not valid")
    return payload


def slice_page(
    items: list[dict[str, Any]],
    limit: int,
    cursor: str | None,
    key: str,
    *,
    descending: bool = False,
) -> tuple[list[dict[str, Any]], str | None]:
    """Items are already sorted by ``key``."""
    if cursor:
        last = decode_cursor(cursor).get("k")
        if not isinstance(last, str):
            raise ConfigValidationError("cursor is not valid")
        if descending:
            items = [item for item in items if str(item[key]) < last]
        else:
            items = [item for item in items if str(item[key]) > last]
    page = items[:limit]
    if len(items) <= limit:
        return page, None
    return page, encode_cursor({"k": str(page[-1][key])})
