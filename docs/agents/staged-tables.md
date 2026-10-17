# Staged source tables

#13, #14, and #15. The command is `make stage`. It reads `data/raw/` and writes `data/staged/`. It does not download. `make stage DATASET=aviation` writes the DGCA table only. It does not call OpenSky.

These tables are not `RIDE_COLUMNS`. Ride-grain events stay with the resampler.

## Ride-hail

`data/staged/namma_yatri/city_day.parquet` is `city_code=BLR` and `booking_type=Normal`. Purple is excluded. `drvr_cancel` and `rider_cancel` are not columns. `earning_per_completed_ride_proxy` is `earning / done_ride`. The city-day file has no rate columns. Checked 2026-10-14: 1,322 rows, 2022-09-01 through 2026-04-15, and the only missing date in that span is 2026-02-14. The alignment window therefore has 536 city-day rows, not 537.

`city_hour.parquet` and `ward_hour.parquet` are the stitched archives, with their coverage JSON copied beside them. The city-hour file is a shape prior. It is not the city-day volume. Normal Auto `done_ride` in that archive, divided by city-day `done_ride` on the same dates, is 0.0037.

## Metro

`data/staged/metro/daily.parquet` drops tracker rows dated after 2026-07-15. The file checked on 2026-10-14 already ends that day, so the drop count is 0. It has 438 rows, 2024-10-26 through 2026-07-15, and 373 of those rows sit in the alignment window. 190 dates are missing inside that span. Missing dates stay missing. Use `total` alone.

`station_hour.parquet` is the long RTI table. August `measure` is `august_hourly`. September uses `entry` and `exit` on separate rows. A null cell is absent. A zero is kept. `23:00 Hrs To Last train` has a null `interval_end`. `line_open` is true from 2025-08-11. On the August sheet, 2025-08-10 has 69 stations and 2025-08-11 has 83.

The station string is the RTI label, such as `11-Baiyappanahalli`. It is not the `BYPH` code. That code lives in `station_codes.csv`.

Hive DDL is `src/flowstate/warehouse/ddl/metro_daily.sql` and `metro_station_hour.sql`. Neither table is `rides`. Beeline on 2026-10-18 counted `metro_daily` 438 and `metro_station_hour` 152040.

The sample envelope is `station_hour_event.json`. `provenance.status` is `observed`. ADR-0008 has no `observed_aggregate` status. A consumer read `bmrcl-2025-08-01-11-Baiyappanahalli-august_hourly-00:00` from `flowstate.metro.station_hour.v1`. No enrichment topic.

## Bus and DGCA

`data/staged/bmtc/` holds stops, routes, valid trips, their stop times, and `rejections.parquet`. The feed timezone is `Asia/Kolkata`. A trip longer than 24 hours is rejected. Checked 2026-10-14: 57,836 trips, and the structural checks rejected none. `scheduled_stop_event.json` is one stop call with `provenance.status` `derived`. The ridership object is `synthetic`. Boarding counts are null. #40 fills them. No boarding topic.

`data/staged/dgca/city_month.parquet` is one ordered `City1 -> City2` month. Do not add a row to its reverse. Do not expand a month into days. Passengers are not flight counts. There is no DGCA Kafka topic. Checked 2026-10-14: 7,812 Bengaluru rows, 2015-04 through 2026-07, 19 months inside the alignment window. 18 pair-months are repeated in the source. Both copies are kept. Do not add a repeat to its twin.

Hive DDL: `bmtc_stops.sql`, `dgca_city_month.sql`. Beeline on 2026-10-18 counted `bmtc_stops` 9960 and `dgca_city_month` 7812. A consumer read `bmtc-1-1` from `flowstate.bus.scheduled_stop.v1`. Stop-call status was `derived`. Ridership status was `synthetic`. `boardings` was null. Kafka had no DGCA topic.
