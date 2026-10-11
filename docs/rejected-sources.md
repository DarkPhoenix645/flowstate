# Rejected sources (audit trail)

Every dataset below was checked from its downloaded contents and confirmed generated, mislabeled, or not Indian mobility data. This is the exclusion list for [ADR-0011](adr/0011-bengaluru-source-table-v2.md). None of these may appear in ingest, the provenance manifest, or the model.

Test used for taxi-like files: real city traffic has a weekly rhythm. Generated files usually assign a flat rate per day. Count rows per day. If the spread of daily counts matches coin-flip noise (Poisson, std ≈ √mean) and every weekday has the same per-occurrence rate, the file is generated. Apparent weekday gaps can be a calendar artifact (July 2024 has five Mon/Tue/Wed), so divide by weekday occurrences before judging.

## Confirmed generated

| Source | Role it falsely claimed | Evidence |
| --- | --- | --- |
| `anilrohan/uber-data-india` (NCR, all 2024) | real NCR Uber bookings | every weekday ≈ 21.4k rows; daily counts mean 411, std 20.6 (Poisson noise); round status totals (93k/27k/10.5k/10.5k/9k = 150k) |
| `hetmengar/ola-and-uber-ride-booking-and-cancellation-data` (Bengaluru, Jul 2024) | real Bengaluru bookings, minute grain | flat ≈ 3,300/day; weekday "gaps" are the calendar, per-occurrence rate flat |
| `muhammadahmadmujahid/ola-dataset` (Bengaluru Ola, Jan 2024) | real Bengaluru Ola bookings | same test, flat ≈ 1,660/day |
| `nikhilkumar766/delhi-metro-dataset` | Delhi metro trips | station names padded with random spaces, sequential TripID 1–150k, fare/km from 0.26 to 400 |
| `leofire123/indian-flight-delay-datasets` | real flight delays | fabricated flight numbers (`IN8723`; IndiGo uses `6E`) |
| `satyakidas07/cityflow-bus-service-metro-cities` | bus ridership | contains customer and driver names; no operator publishes those |
| `asshridattaaigal/bangalore-traffic-analysis-dataset` | Bengaluru traffic | route simulation with `weather` and `road_capacity` columns |
| `vishaldeoprasad/bangalore-rapido-ride-services-dataset` | Rapido rides | invented place names ("Babusapalya Cove"), identical `.542646` microseconds across rows |
| `vinaykumarpanika/indore-ola-dataset` | Indore Ola | `anilrohan/uber-data-india` schema cloned to Indore, flat daily rate |
| `vengateshvengat/rapido-all-data` | Rapido July 2025 | sequential `RAP202507…` IDs, five pickup "cities" |
| `palvinder2006/ola-bike-ride-request` | Ola bike demand | 2011 Washington DC bikeshare set, relabeled (season/casual/registered/count columns) |
| `vancharmlab/traffic-management` | Asian ride-hailing demand | Grab Seattle geohash demand set |
| `shayanzk/ola-ride-bookings-dataset`, `yashdevladdha/uber-ride-analytics-dashboard`, `crazyelon/ncr-uber-ride-data-with-festivals-and-events-2024` | Ola/Uber rides | clones of `anilrohan/uber-data-india` (same size or schema) |
| `nikhilkumar766`-style CMRL packs (`subhashb21022/cmrl-datasets`, `chennai-metro-dataset-2026-latest`) | Chennai metro history | monthly totals plus a rolling ~10-day window; no usable history (not synthetic, but not a clock) |

## Consequences

- `city=ncr` has no real data behind it. It is not a city in this project.
- `hetmengar/ola-and-uber-ride-booking-and-cancellation-data` and `muhammadahmadmujahid/ola-dataset` are not evidence of real demand. They are not shape priors. [ADR-0011](adr/0011-bengaluru-source-table-v2.md) keeps them out of fitting, calibration, and generation.
- Kaggle search remains the discovery channel for candidates; nothing in this table may be re-added without new evidence passing the weekly-rhythm test plus a content inspection.
