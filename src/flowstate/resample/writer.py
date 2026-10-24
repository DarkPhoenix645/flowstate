"""Amplify orchestration. Budgets, part files, manifest, validation.

``make amplify`` lands here through
``flowstate.ingest.amplify``. The corpus is a thin copy of every
source day in the alignment window (ADR-0013): each mode gets a
budget by its measured natural share, that budget is split across
the mode's source days with ``largest_remainder``, and each day is
unfolded at its slice. Whole bus trips can undershoot; ride-hail
absorbs the exact remainder so the row count is exactly ``--rows``.
The allocator conserves the emitted parent.

Output: ``data/staged/rides_amplified/`` part files of about one
HDFS block (128 MB), plus ``manifest.json``, ``bundle.json``, and
``evaluation.json``. Source files are never touched.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from flowstate.config import Settings
from flowstate.normalize.cities import DEFAULT_CITY_ID, load
from flowstate.resample.bundle import (
    FARE_BASIS,
    ModelBundle,
    ResampleConfig,
    build_bundle,
    bundle_hash,
    bundle_json,
    city_spec,
    load_inputs,
)
from flowstate.resample.checks import (
    CorpusError,
    _Issues,
    check_rows,
    validate_written,
)
from flowstate.resample.profiles import to_date
from flowstate.resample.unfold import (
    EXTRA_COLUMNS,
    ParentTotals,
    allocate_days,
    aviation_events,
    bus_events,
    largest_remainder,
    metro_day_events,
    ridehail_day_events,
    rng_for,
)
from flowstate.schema import RIDE_FIELDS

TARGET_PART_BYTES = 128 * 1024 * 1024
CORPUS_DATASET = "rides_amplified"
MIN_ROWS = 1000
COMMITTED_EVALUATION = Path(
    "src/flowstate/benchmarks/results/resampler-evaluation.json"
)

# One fixed schema for every part. Inferred-per-batch types would
# turn an all-null integer column in one batch into a string column
# and collide with the next batch.
_INT_EXTRAS = frozenset(
    {
        "hour",
        "count",
        "entries_count",
        "exits_count",
        "boardings",
        "alightings",
        "onboard_leaving",
        FARE_BASIS,
    }
)


def corpus_schema() -> pa.Schema:
    fields = [
        pa.field(name, pa.string() if kind == "string" else pa.float64())
        for name, kind in RIDE_FIELDS
    ]
    fields.extend(
        pa.field(name, pa.int64() if name in _INT_EXTRAS else pa.string())
        for name in EXTRA_COLUMNS
    )
    return pa.schema(fields)


def corpus_table(rows: list[dict[str, Any]]) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=corpus_schema())


def corpus_dir(settings: Settings) -> Path:
    return settings.staged_dir / CORPUS_DATASET


class PartWriter:
    """Streaming parquet writer: many ~block-sized part files.

    The first batch calibrates rows-per-part from its written size;
    later parts fill to that count. Batches become row groups, so
    memory stays at one batch regardless of corpus size.
    """

    def __init__(self, directory: Path, target_bytes: int = TARGET_PART_BYTES):
        self.directory = directory
        self.target_bytes = target_bytes
        self.rows_per_part: int | None = None
        self.parts: list[Path] = []
        self.part_rows: list[int] = []
        self.total_rows = 0
        self._writer: pq.ParquetWriter | None = None
        self._part_index = 0
        self._rows_in_part = 0

    def append(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        table = corpus_table(rows)
        if self.rows_per_part is None:
            # First batch calibrates rows-per-part from its written
            # size (row group bytes are on disk before the footer).
            self._write_table(table)
            size = self.parts[-1].stat().st_size
            per_row = max(size / max(len(rows), 1), 1.0)
            self.rows_per_part = max(1, int(self.target_bytes / per_row))
            if self._rows_in_part >= self.rows_per_part:
                self._close_part()
            return
        self._write_table(table)
        if self._rows_in_part >= (self.rows_per_part or 0):
            self._close_part()

    def _write_table(self, table: pa.Table) -> None:
        if self._writer is None:
            self.directory.mkdir(parents=True, exist_ok=True)
            path = self.directory / f"part-{self._part_index:05d}.parquet"
            self._writer = pq.ParquetWriter(
                path, table.schema, compression="snappy"
            )
            self.parts.append(path)
            self.part_rows.append(0)
            self._rows_in_part = 0
            self._part_index += 1
        self._writer.write_table(table)
        self._rows_in_part += table.num_rows
        self.part_rows[-1] = self._rows_in_part
        self.total_rows += table.num_rows

    def _close_part(self) -> None:
        if self._writer is not None:
            self._writer.close()
            self._writer = None

    def close(self) -> None:
        self._close_part()


def _natural_shares(
    inputs: Any, bundle: ModelBundle
) -> dict[str, float]:
    """Measured natural volumes in the window. Budget priors only."""
    start, end = bundle.spec.window_start, bundle.spec.window_end
    ride_total = 0
    frame = inputs.city_day.select(
        ["date", "booking", "done_ride", "cancel_ride"]
    ).to_pydict()
    ride_days: list[str] = []
    for day, booking, done, cancel in zip(
        frame["date"],
        frame["booking"],
        frame["done_ride"],
        frame["cancel_ride"],
        strict=True,
    ):
        if not start <= to_date(str(day)[:10]) <= end:
            continue
        total = sum(
            int(value or 0) for value in (booking, done, cancel)
        )
        if total <= 0:
            continue
        ride_total += total
        ride_days.append(str(day)[:10])
    metro_days = _metro_days(inputs, start, end)
    metro_records = 0
    for day in metro_days:
        active = bundle.metro.active(to_date(day))
        metro_records += 24 * len(active)
    flights = inputs.flight_events.column("local_date").to_pylist()
    aviation_rows = sum(
        1
        for day in flights
        if day and start <= to_date(str(day)[:10]) <= end
    )
    return {
        "ride_hail": float(ride_total),
        "metro": float(metro_records),
        "bus": float(inputs.stop_times.num_rows),
        "aviation": float(aviation_rows),
    }


def _metro_days(inputs: Any, start: date, end: date) -> list[str]:
    days = inputs.metro_daily.column("record_date").to_pylist()
    return [
        str(day)[:10]
        for day in days
        if start <= to_date(str(day)[:10]) <= end
    ]


def run_amplify(
    settings: Settings, rows: int, seed: int = 42
) -> dict[str, Any]:
    """Build the bundle, unfold the corpus, write, validate."""
    if rows < MIN_ROWS:
        raise ValueError(
            f"rows={rows} is too small for a multimodal corpus "
            f"(minimum {MIN_ROWS})"
        )
    inputs = load_inputs(settings)
    spec = city_spec(load(DEFAULT_CITY_ID))
    # The #40 boarding prior is FLOWSTATE_ settings (ADR-0012).
    # The bus summary and bundle.json quote the values in force.
    bundle = build_bundle(
        inputs,
        spec,
        config=ResampleConfig.from_settings(settings),
        seed=seed,
    )
    shares = _natural_shares(inputs, bundle)
    budgets = dict(
        zip(
            ("ride_hail", "metro", "bus", "aviation"),
            largest_remainder(
                rows,
                [
                    shares[mode]
                    for mode in (
                        "ride_hail",
                        "metro",
                        "bus",
                        "aviation",
                    )
                ],
            ),
            strict=True,
        )
    )
    parents = ParentTotals()
    issues = _Issues()
    seen_ids: set[str] = set()
    directory = corpus_dir(settings)
    # A fresh corpus per run. Stale parts from an earlier run would
    # ride along to the lake put (#25). Only this directory is
    # cleared; source files are never touched.
    if directory.exists():
        for stale in directory.glob("part-*.parquet"):
            stale.unlink()
    writer = PartWriter(directory)

    # ---- bus: whole trips from the feed day -----------------------
    from flowstate.stage.bus import FEED_DATE

    bus_stream, bus_summary = bus_events(
        bundle,
        rng_for(seed, "bus"),
        inputs.stop_times,
        feed_date=FEED_DATE,
        budget_rows=budgets["bus"],
        parents=parents,
    )

    # ---- metro: whole days, subset last ---------------------------
    metro_pool = _metro_days(
        inputs, bundle.spec.window_start, bundle.spec.window_end
    )
    metro_summary: dict[str, Any] = {"days": [], "subset_days": []}

    # ---- aviation: observed replay + synthetic fills --------------
    flight_days = sorted(
        {
            str(day)[:10]
            for day in inputs.flight_events.column("local_date").to_pylist()
            if day
            and bundle.spec.window_start
            <= to_date(str(day)[:10])
            <= bundle.spec.window_end
        }
    )
    aviation_stream, aviation_summary = aviation_events(
        bundle,
        rng_for(seed, "aviation"),
        inputs.flight_events,
        day_pool=flight_days,
        budget_rows=budgets["aviation"],
        parents=parents,
    )

    generated = {
        "bus": list(bus_stream),
        "aviation": list(aviation_stream),
        "metro": [],
        "ride_hail": [],
    }
    budget_used_bus = sum(len(b) for b in generated["bus"])
    budget_used_aviation = sum(len(b) for b in generated["aviation"])

    metro_budget = budgets["metro"]
    metro_totals = [_metro_total(inputs, day) for day in metro_pool]
    record_w = [
        24 * len(bundle.metro.active(to_date(day))) for day in metro_pool
    ]
    row_w = [float(value) for value in metro_totals]
    if sum(row_w) <= 0:
        row_w = [float(value) for value in record_w]
    assigned_metro = allocate_days(metro_budget, metro_pool, row_w)
    weight_by_day = dict(zip(metro_pool, row_w, strict=True))
    total_by_day = dict(zip(metro_pool, metro_totals, strict=True))
    den = sum(record_w) or 1
    people_pool = int(
        round(sum(metro_totals) * metro_budget / den)
    )
    people_pool = max(people_pool, sum(k for _, k in assigned_metro))
    people_assigned = dict(
        allocate_days(
            people_pool,
            [day for day, _ in assigned_metro],
            [weight_by_day[day] for day, _ in assigned_metro],
        )
    )
    used = 0
    for day, n_records in assigned_metro:
        people = max(people_assigned.get(day, 0), n_records)
        day_rng = rng_for(seed, "metro", day)
        batches = list(
            metro_day_events(
                bundle,
                day_rng,
                day,
                people,
                parents,
                partial_records=n_records,
            )
        )
        emitted = sum(len(batch) for batch in batches)
        used += emitted
        for batch in batches:
            generated["metro"].append(batch)
        if emitted < n_records:
            metro_summary["subset_days"].append(day)
        else:
            metro_summary["days"].append(day)
        metro_summary.setdefault("day_budgets", {})[day] = {
            "rows": n_records,
            "people": people,
            "tracker": total_by_day[day],
            "emitted": emitted,
        }
    metro_summary["rows"] = used
    budget_used_metro = used

    # ---- ride_hail: remainder, thinned across every source day ----
    ride_budget = (
        rows - budget_used_bus - budget_used_aviation - budget_used_metro
    )
    ride_days = _ride_days(inputs, bundle)
    ride_summary: dict[str, Any] = {"days": [], "day_budgets": {}}
    used = 0
    ride_rows = inputs.city_day.select(
        [
            "date",
            "booking",
            "done_ride",
            "cancel_ride",
            "earning_per_completed_ride_proxy",
        ]
    ).to_pydict()
    day_metrics = {
        str(day)[:10]: (booking, done, cancel, proxy)
        for day, booking, done, cancel, proxy in zip(
            ride_rows["date"],
            ride_rows["booking"],
            ride_rows["done_ride"],
            ride_rows["cancel_ride"],
            ride_rows["earning_per_completed_ride_proxy"],
            strict=True,
        )
    }
    ride_weights = []
    for day in ride_days:
        booking, done, cancel, _proxy = day_metrics.get(
            day, (0, 0, 0, None)
        )
        ride_weights.append(
            float(
                int(booking or 0) + int(done or 0) + int(cancel or 0)
            )
        )
    assigned_ride = allocate_days(ride_budget, ride_days, ride_weights)
    leftover = ride_budget - sum(k for _, k in assigned_ride)
    for index, (day, k) in enumerate(assigned_ride):
        if leftover and index == len(assigned_ride) - 1:
            k += leftover
            leftover = 0
        booking, done, cancel, proxy = day_metrics.get(
            day, (None, None, None, None)
        )
        metrics = {
            "booking": int(booking or 0),
            "done_ride": int(done or 0),
            "cancel_ride": int(cancel or 0),
        }
        day_total = sum(metrics.values())
        if day_total <= 0:
            continue
        partial = k if k < day_total else None
        day_rng = rng_for(seed, "ride_hail", day)
        batches = ridehail_day_events(
            bundle,
            day_rng,
            day,
            metrics,
            proxy,
            parents,
            partial_rows=partial,
        )
        emitted = 0
        for batch in batches:
            emitted += len(batch)
            generated["ride_hail"].append(batch)
        used += emitted
        ride_summary["days"].append(day)
        ride_summary["day_budgets"][day] = {
            "rows": k,
            "day_total": day_total,
            "emitted": emitted,
        }
    ride_summary["rows"] = used

    # ---- validate in memory, then write --------------------------
    all_batches: list[list[dict[str, Any]]] = []
    mode_counts: dict[str, int] = {}
    provenance_counts: dict[str, int] = {}
    total = 0
    for mode in ("ride_hail", "metro", "bus", "aviation"):
        for batch in generated[mode]:
            check_rows(batch, bundle, issues, seen_ids)
            all_batches.append(batch)
            mode_counts[mode] = mode_counts.get(mode, 0) + len(batch)
            for row in batch:
                provenance_counts[row["provenance_status"]] = (
                    provenance_counts.get(row["provenance_status"], 0) + 1
                )
            total += len(batch)
    issues.raise_if_any("pre-write validation failed")
    if total != rows:
        raise CorpusError(
            f"budget math produced {total} rows, expected {rows}"
        )
    for batch in all_batches:
        writer.append(batch)
    writer.close()

    parents_check = {
        "ridehail": dict(parents.ridehail),
        "entries": dict(parents.entries),
        "exits": dict(parents.exits),
        "bus": dict(parents.bus),
        "aviation": dict(parents.aviation),
        "total_rows": total,
    }
    validate_written(writer.parts, bundle, parents_check)

    parts_record = [
        {"part": part.name, "rows": count, "sha256": _sha256(part)}
        for part, count in zip(writer.parts, writer.part_rows, strict=True)
    ]
    parents_manifest = {
        "ridehail": {
            f"{parent}|{metric}": value
            for (parent, metric), value in parents.ridehail.items()
        },
        "metro_entries": parents.entries,
        "metro_exits": parents.exits,
        "bus": {
            f"{parent}|{metric}": value
            for (parent, metric), value in parents.bus.items()
        },
        "aviation": {
            f"{parent}|{kind}": value
            for (parent, kind), value in parents.aviation.items()
        },
    }
    manifest = {
        "schema": "rides-amplified-manifest/1",
        "rows": total,
        "seed": seed,
        "bundle_version": bundle.version,
        "bundle_hash": bundle_hash(bundle),
        "mode_counts": mode_counts,
        "provenance_counts": provenance_counts,
        "natural_shares": shares,
        "budgets": budgets,
        "parts": parts_record,
        "target_part_bytes": TARGET_PART_BYTES,
        "parents": parents_manifest,
        "replay": {
            "bus": bus_summary,
            "metro": metro_summary,
            "aviation": {
                "days": aviation_summary["days"],
                "observed_rows": aviation_summary["observed_rows"],
                "synthetic_rows": aviation_summary["synthetic_rows"],
                "synthetic_day_directions": aviation_summary[
                    "synthetic_day_directions"
                ],
            },
            "ride_hail": ride_summary,
        },
        "veracity": [
            "Every event is resampler output. Observed-proxy aviation "
            "rows keep their source timestamps; nothing here is an "
            "observed trip.",
            "Parent totals are conserved exactly by every allocator. "
            "Each source day in the alignment window is thinned to a "
            "share of N (ADR-0013). The emitted parent is the slice.",
            "synthetic_fare_inr is a lognormal draw centred on the "
            "day's earning_per_completed_ride_proxy. It is never an "
            "observed fare.",
            "Bus ridership is the #40 assumption set (ADR-0012), not "
            "BMTC ridership. Do not add it to observed totals.",
            "Mode budgets follow measured natural shares: "
            + ", ".join(
                f"{mode}={shares[mode]:.0f}" for mode in sorted(shares)
            ),
            "The whole corpus is deterministic for a given seed.",
        ],
    }
    _write_json(directory / "manifest.json", manifest)
    _write_json(directory / "bundle.json", bundle_json(bundle))
    _write_json(directory / "evaluation.json", bundle.clocks.evaluation)
    return manifest


def _metro_total(inputs: Any, day: str) -> int:
    frame = inputs.metro_daily.select(
        ["record_date", "total"]
    ).to_pydict()
    for record_date, total in zip(
        frame["record_date"], frame["total"], strict=True
    ):
        if str(record_date)[:10] == day:
            return int(total or 0)
    return 0


def _ride_days(inputs: Any, bundle: ModelBundle) -> list[str]:
    start, end = bundle.spec.window_start, bundle.spec.window_end
    frame = inputs.city_day.select(["date", "done_ride"]).to_pydict()
    days = []
    for day, done in zip(frame["date"], frame["done_ride"], strict=True):
        key = str(day)[:10]
        if start <= to_date(key) <= end and (done or 0) > 0:
            days.append(key)
    return days


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str) + "\n")


def write_evaluation(settings: Settings) -> dict[str, Any]:
    """Fit and record the frozen evaluation. Commits as evidence."""
    inputs = load_inputs(settings)
    spec = city_spec(load(DEFAULT_CITY_ID))
    bundle = build_bundle(inputs, spec)
    evaluation = bundle.clocks.evaluation
    _write_json(corpus_dir(settings) / "evaluation.json", evaluation)
    _write_json(Path(COMMITTED_EVALUATION), evaluation)
    return evaluation
