"""The run table. Same database as model config. No migration tool."""

from __future__ import annotations

from typing import Any

from psycopg.types.json import Json

from flowstate.config.postgres import _connect


class RunStore:
    def __init__(self, database_url: str | None = None) -> None:
        self._url = database_url

    def insert(self, row: dict[str, Any]) -> dict[str, Any]:
        with _connect(self._url) as conn:
            with conn.transaction():
                saved = conn.execute(
                    """
                    INSERT INTO run (
                        run_id, kind, job, params, config_snapshot,
                        config_hash, scenario_policy_version_id,
                        resample_config_version_id, backend, status,
                        grafana_url
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, 'queued', %s
                    )
                    RETURNING run_id
                    """,
                    (
                        row["run_id"],
                        row["kind"],
                        row["job"],
                        Json(row["params"]),
                        Json(row["config_snapshot"]),
                        row["config_hash"],
                        row["scenario_policy_version_id"],
                        row["resample_config_version_id"],
                        row["backend"],
                        row["grafana_url"],
                    ),
                ).fetchone()
        return self.get(str(saved["run_id"]))

    def get(self, run_id: str) -> dict[str, Any] | None:
        with _connect(self._url) as conn:
            row = conn.execute(
                "SELECT * FROM run WHERE run_id = %s",
                (run_id,),
            ).fetchone()
        if row is None:
            return None
        return _public(row)

    def list_runs(
        self, kind: str | None = None, status: str | None = None
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        values: list[Any] = []
        if kind:
            clauses.append("kind = %s")
            values.append(kind)
        if status:
            clauses.append("status = %s")
            values.append(status)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with _connect(self._url) as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM run
                {where}
                ORDER BY created_at DESC
                """,
                values,
            ).fetchall()
        return [_public(row) for row in rows]

    def mark_running(self, run_id: str, external_id: str) -> None:
        with _connect(self._url) as conn:
            with conn.transaction():
                conn.execute(
                    """
                    UPDATE run
                    SET status = 'running',
                        external_id = %s,
                        started_at = COALESCE(started_at, now())
                    WHERE run_id = %s
                    """,
                    (external_id, run_id),
                )

    def finish(
        self,
        run_id: str,
        status: str,
        error: str | None = None,
        result_location: str | None = None,
    ) -> None:
        with _connect(self._url) as conn:
            with conn.transaction():
                conn.execute(
                    """
                    UPDATE run
                    SET status = %s,
                        error = %s,
                        result_location = COALESCE(%s, result_location),
                        finished_at = now()
                    WHERE run_id = %s
                    """,
                    (status, error, result_location, run_id),
                )

    def running(self) -> list[dict[str, Any]]:
        return self.list_runs(status="running")


def _public(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    for key in ("created_at", "started_at", "finished_at"):
        value = out.get(key)
        if hasattr(value, "isoformat"):
            out[key] = value.isoformat()
    return out
