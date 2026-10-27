"""Grafana link for a run. Null until that job has a dashboard."""

from __future__ import annotations

from typing import Any

# #91 fills this. The uid contract is ADR-0019: analytics-<question>.
DASHBOARD_UID: dict[str, str] = {}


def grafana_link(settings: Any, job: str, run_id: str) -> str | None:
    uid = DASHBOARD_UID.get(job)
    if uid is None:
        return None
    base = str(settings.grafana_url).rstrip("/")
    return f"{base}/d/{uid}?var-run_id={run_id}"
