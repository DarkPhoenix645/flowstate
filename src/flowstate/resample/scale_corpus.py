"""One scale corpus of full Bengaluru days. Days are not thinned.

The window ends on a requested date. Each ride-hail day emits that
day's city-day total, or the same weekday inside the alignment
window when the date is past the source. Whole days only, so the
row count is the closest sum to the target.
"""

from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from typing import Any

from flowstate.resample.bundle import bundle_hash
from flowstate.resample.profiles import freeze_day, to_date
from flowstate.resample.unfold import (
    ParentTotals,
    aviation_events,
    metro_day_events,
    ridehail_day_events,
    rng_for,
)
from flowstate.resample.window import metro_total
from flowstate.resample.writer import PartWriter, _sha256, corpus_dir

log = logging.getLogger(__name__)
TARGET = 100_000_000
END = date(2026, 10, 28)


def select_days(
    totals: dict[str, int],
    *,
    end: date,
    align_start: date,
    align_end: date,
    target: int,
) -> tuple[list[str], int]:
    """Calendar days ending at ``end``. Whole days, closest to ``target``."""
    chosen: list[str] = []
    total = 0
    cursor = end
    while cursor >= align_start:
        source = freeze_day(cursor.isoformat(), align_start, align_end)
        add = int(totals.get(source, 0))
        if add <= 0:
            cursor -= timedelta(days=1)
            continue
        if total > 0 and abs(total + add - target) > abs(total - target):
            break
        total += add
        chosen.append(cursor.isoformat())
        cursor -= timedelta(days=1)
    chosen.reverse()
    return chosen, total


def _ride_totals(inputs: Any, start: date, end: date) -> dict[str, tuple]:
    frame = inputs.city_day.select(
        [
            "date",
            "booking",
            "done_ride",
            "cancel_ride",
            "earning_per_completed_ride_proxy",
        ]
    ).to_pydict()
    found: dict[str, tuple] = {}
    for day, booking, done, cancel, proxy in zip(
        frame["date"],
        frame["booking"],
        frame["done_ride"],
        frame["cancel_ride"],
        frame["earning_per_completed_ride_proxy"],
        strict=True,
    ):
        key = str(day)[:10]
        if not start <= to_date(key) <= end:
            continue
        metrics = (
            int(booking or 0),
            int(done or 0),
            int(cancel or 0),
            proxy,
        )
        if sum(metrics[:3]) > 0:
            found[key] = metrics
    return found


def _write_aviation(
    bundle: Any,
    inputs: Any,
    seed: int,
    parents: ParentTotals,
    days: list[str],
    spec: Any,
    consume: Any,
) -> None:
    pool = sorted(
        {
            freeze_day(day, spec.window_start, spec.window_end)
            for day in days
            if to_date(day) <= spec.window_end
        }
    )
    if not pool:
        return
    flights = inputs.flight_events.column("local_date").to_pylist()
    budget = sum(1 for day in flights if str(day)[:10] in set(pool))
    if budget <= 0:
        return
    batches, _summary = aviation_events(
        bundle,
        rng_for(seed, "aviation"),
        inputs.flight_events,
        day_pool=pool,
        budget_rows=budget,
        parents=parents,
    )
    consume("aviation", batches, False)


def _stamp_outside(batch: list[dict[str, Any]], outside: bool) -> None:
    if not outside:
        return
    for row in batch:
        row["outside_training_window"] = "true"


def run_scale(
    settings: Any,
    *,
    end: date = END,
    target: int = TARGET,
    seed: int = 42,
) -> dict[str, Any]:
    """Write the corpus under the staged amplify directory."""
    from flowstate.config.resolve import open_config_source
    from flowstate.normalize.cities import DEFAULT_CITY_ID, load
    from flowstate.resample.bundle import (
        build_bundle,
        bundle_json,
        city_spec,
        load_inputs,
    )

    inputs = load_inputs(settings)
    spec = city_spec(load(DEFAULT_CITY_ID))
    resolved = open_config_source(settings).load()
    bundle = build_bundle(
        inputs,
        spec,
        config=resolved.model.resample_config,
        seed=seed,
        config_hash=resolved.config_hash,
    )
    lookup = _ride_totals(inputs, spec.window_start, spec.window_end)
    totals = {day: sum(item[:3]) for day, item in lookup.items()}
    days, ride_rows = select_days(
        totals,
        end=end,
        align_start=spec.window_start,
        align_end=spec.window_end,
        target=target,
    )
    directory = corpus_dir(settings)
    directory.mkdir(parents=True, exist_ok=True)
    for stale in directory.glob("part-*.parquet"):
        stale.unlink()
    writer = PartWriter(directory)
    parents = ParentTotals()
    mode_counts = {"ride_hail": 0, "metro": 0, "aviation": 0}
    provenance: dict[str, int] = {}
    total = 0

    def consume(mode: str, batches: Any, outside: bool) -> None:
        nonlocal total
        for batch in batches:
            _stamp_outside(batch, outside)
            writer.append(batch)
            count = len(batch)
            mode_counts[mode] = mode_counts.get(mode, 0) + count
            for row in batch:
                status = str(row["provenance_status"])
                provenance[status] = provenance.get(status, 0) + 1
            total += count

    log.info(
        "full days %s through %s ride-hail %s",
        days[0],
        days[-1],
        ride_rows,
    )
    for index, day in enumerate(days, start=1):
        if index == 1 or index % 20 == 0:
            log.info("writing day %s of %s (%s)", index, len(days), day)
        source = freeze_day(day, spec.window_start, spec.window_end)
        booking, done, cancel, proxy = lookup[source]
        outside = to_date(day) > spec.window_end
        consume(
            "ride_hail",
            ridehail_day_events(
                bundle,
                rng_for(seed, "ride_hail", day),
                day,
                {
                    "booking": booking,
                    "done_ride": done,
                    "cancel_ride": cancel,
                },
                proxy,
                parents,
            ),
            outside,
        )
        footfall = metro_total(inputs, source)
        if footfall > 0:
            consume(
                "metro",
                metro_day_events(
                    bundle,
                    rng_for(seed, "metro", day),
                    day,
                    footfall,
                    parents,
                ),
                outside,
            )
    _write_aviation(bundle, inputs, seed, parents, days, spec, consume)
    writer.close()
    manifest = _manifest(
        bundle, seed, total, mode_counts, provenance, parents, writer, days
    )
    (directory / "manifest.json").write_text(json.dumps(manifest))
    (directory / "bundle.json").write_text(json.dumps(bundle_json(bundle)))
    evaluation = directory / "evaluation.json"
    if not evaluation.is_file():
        evaluation.write_text(json.dumps(bundle.clocks.evaluation))
    log.info(
        "scale corpus %s rows %s through %s",
        total,
        days[0],
        days[-1],
    )
    return manifest


def _manifest(
    bundle: Any,
    seed: int,
    total: int,
    mode_counts: dict[str, int],
    provenance: dict[str, int],
    parents: ParentTotals,
    writer: PartWriter,
    days: list[str],
) -> dict[str, Any]:
    parts = [
        {"part": part.name, "rows": count, "sha256": _sha256(part)}
        for part, count in zip(writer.parts, writer.part_rows, strict=True)
    ]
    return {
        "schema": "rides-amplified-manifest/1",
        "rows": total,
        "seed": seed,
        "bundle_version": bundle.version,
        "bundle_hash": bundle_hash(bundle),
        "mode_counts": mode_counts,
        "provenance_counts": provenance,
        "natural_shares": {},
        "budgets": {"ride_hail": mode_counts.get("ride_hail", 0)},
        "parts": parts,
        "target_part_bytes": writer.target_bytes,
        "parents": {
            "ridehail": {
                f"{parent}|{metric}": value
                for (parent, metric), value in parents.ridehail.items()
            },
            "metro_entries": parents.entries,
            "metro_exits": parents.exits,
            "bus": {},
            "aviation": {},
        },
        "replay": {
            "window_start": days[0],
            "window_end": days[-1],
            "full_days": True,
            "bus": "omitted; the GTFS feed is one service day",
            "aviation": "source days on or before 2026-04-15",
        },
        "veracity": [
            "Each ride-hail day is the city-day total, not a thinned share.",
            "A day after 2026-04-15 uses the same weekday inside the "
            "alignment window and is marked outside the training window.",
            "Metro days use that source day's footfall.",
            "Bus is omitted. The feed is one service day, not a calendar.",
            "Aviation uses the source days on or before 2026-04-15.",
        ],
    }


def main() -> None:
    from flowstate.config import get_settings
    from flowstate.log import configure_logging

    configure_logging()
    run_scale(get_settings())


if __name__ == "__main__":
    main()
