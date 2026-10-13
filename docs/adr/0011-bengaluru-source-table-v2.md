# ADR-0011: Bengaluru source table v2

## Status

Accepted. This is the source set for v1. It replaces the earlier table that treated three Kaggle taxi files as observations.

## Context

Three Kaggle taxi files were checked from their contents and failed the same integrity test: flat daily rates, Poisson-like count noise, and round status totals. They are generated files, not observations. The names and the counts are in [rejected-sources.md](../rejected-sources.md). Bengaluru has real series that overlap in time. This ADR assigns each one a role.

## Decision

The accepted source set is the ten rows in [data-sources.md](../data-sources.md) "Exactly what we use": Namma Yatri city-day, the two stitched Namma Yatri hourly archives, BMRCL RTI workbooks, OpenSky VOBL arrivals and departures, the Namma Metro daily tracker, DGCA monthly city pairs, BMTC GTFS, significant dates, and the RTI station-code map.

Roles and integrity rules:

- Ride-hail clock: Namma Yatri city-day, `city_code=BLR`, `booking_type=Normal`, `done_ride`. Purple is a separate accessibility product and stays out.
- Metro hour-grain truth: the RTI workbooks, for their covered dates only (Aug 1–18, Sep 1–30, 2025; 48 days). The daily tracker is a separate daily signal. The two are never summed.
- Metro tracker: local file is hash-identical to the current public file. Any row dated after 2026-07-15 (the last upstream commit) is invalid and excluded. None fall inside the alignment window.
- Aviation: OpenSky arrivals and departures over the alignment window. `firstSeen`/`lastSeen` are estimated-time proxies. Every query window carries a status record; a sparse or failed window is never a zero-flight day. Departure coverage is degraded at the oldest end of the window (checked 2026-10-11: 79 departures in the 2024-10-26→27 window against ~330–375/day at mid and recent dates).
- DGCA stays monthly. Never expanded into daily rows, never equated with flight counts.
- BMTC GTFS is a schedule. The stop call on a bus event is `derived`. Boardings and alightings on that same event are synthetic. They are never observed ridership.

Exclusions:

- The three generated Kaggle taxi files in [rejected-sources.md](../rejected-sources.md) are not sources. They are out of ingest, fitting, and generation. `city=ncr` is not a city in this project.
- Mumbai, Delhi, Chennai, Kolkata are out of scope. Bengaluru only.
- XRides 2013 and the Rapido 2018 coordinates have no v1 role.

## Consequences

- Ingest loaders and the provenance manifest cover this set only. `DATASET=all` cannot download or validate a rejected source.
- Coverage windows and known limitations for each source live in `data-sources.md`, not in this ADR.
- Adding or re-adding a source requires a new ADR or an amendment of this one.

## Sources

`docs/data-sources.md`, `docs/rejected-sources.md`, `docs/resampling-plan.md`, [ADR-0010](0010-bengaluru-calibrated-resampler.md)
