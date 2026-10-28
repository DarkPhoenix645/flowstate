"""Grafana link for a run. Null until that job has a dashboard."""

from __future__ import annotations

from typing import Any

# ADR-0019 uid is analytics-<question>. cross_modal opens the first
# of its two questions. flights_ridehail is the other dashboard.
DASHBOARD_UID: dict[str, str] = {
    "demand_hotspots": "analytics-demand_hotspots",
    "cancellation_analysis": "analytics-cancellation_analysis",
    "ridership_trends": "analytics-ridership_trends",
    "cross_modal": "analytics-ridehail_metro",
    "scenario_effect": "analytics-scenario_effect",
}


def grafana_link(settings: Any, job: str, run_id: str) -> str | None:
    uid = DASHBOARD_UID.get(job)
    if uid is None:
        return None
    base = str(settings.grafana_url).rstrip("/")
    return f"{base}/d/{uid}?var-run_id={run_id}"
