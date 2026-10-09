# Source table and column inventory (ticket #3 / T2)

This note supports ticket #3 (T2). It records the locked source table, the columns in the local extracts, and a map to the canonical ride schema.

It does not choose a `pickup_zone` / `drop_zone` id system. Ticket #4 (T3) owns that choice.

It does not lock Kafka payloads for metro, GTFS, or aviation. Ticket #5 (T4) owns that choice.

It does not lock how non-ride grains enter amplify. Ticket #6 (T5) owns that choice. Ride-event generation is ADR-0010 / T32.

## Locked source table

Ticket #3 locks this table. Do not add other mobility URLs.


| #   | Dataset                                          | Source                                 | URL                                                                             |
| --- | ------------------------------------------------ | -------------------------------------- | ------------------------------------------------------------------------------- |
| 1   | Uber Ride Analytics — India (2024)               | Kaggle (anilrohan)                     | `kaggle.com/datasets/anilrohan/uber-data-india`                                 |
| 2   | Ola and Uber: Ride Booking and Cancellation Data | Kaggle (hetmengar)                     | `kaggle.com/datasets/hetmengar/ola-and-uber-ride-booking-and-cancellation-data` |
| 3   | Namma Metro Daily Ridership (processed)          | GitHub (thecont1)                      | `github.com/thecont1/namma-metro-ridership-tracker` (`NammaMetro_Ridership_Processed.csv`) |
| 4   | BMTC Bus GTFS — Bengaluru                        | GitHub (Vonter/bmtc-gtfs)              | `github.com/Vonter/bmtc-gtfs`                                                   |
| 6   | India Aviation Traffic (DGCA)                    | GitHub (Vonter/india-aviation-traffic) | `github.com/Vonter/india-aviation-traffic`                                      |
| 7   | XRides Bangalore Trip Data                       | GitHub (adityakumar9/Xride_Bangalore)  | `github.com/adityakumar9/Xride_Bangalore`                                       |
| 8   | Bangalore Ola                                    | Kaggle                                 | `kaggle.com/datasets/muhammadahmadmujahid/ola-dataset`                          |


Two cities, two scopes. Do not mix them in one join.

- **Bengaluru** is multimodal: ride-hail (rows 2, 7, 8), metro (row 3), bus GTFS (row 4), aviation (row 6 filtered to `BENGALURU`).
- **Delhi NCR** is taxi only: row 1 Uber. `city` = `ncr`. `mode` = `ride_hail`. No NCR metro, bus, or aviation series.

Do not use Mumbai as a stand-in.

## How ingest must get the files

Kaggle rows (1, 2, 8): download at ingest with `KAGGLE_API_TOKEN` and kagglehub (ADR-0003). Do not commit Kaggle CSVs. Do not use the Kaggle CLI.

Other rows (3, 4, 6, 7): put extracts under `flowstate-datasets/` on the host. Git ignores that directory. Ingest copies from there into `data/raw/` then writes staged parquet.

Row 3 may also be fetched as one GitHub raw CSV (`NammaMetro_Ridership_Processed.csv`). Do not scrape `english.bmrc.co.in/ridership/` day by day.

Row 7 is GitHub, not Kaggle. The kagglehub loader must not claim row 7. The ride-hail path owns rows 1, 2, 7, and 8. Keep `city=ncr` on row 1. Filter row 6 to `BENGALURU` for Bengaluru aviation.

## Canonical ride schema (already locked)

Columns: `trip_id`, `city`, `mode`, `vehicle_type`, `timestamp`, `pickup_zone`, `drop_zone`, `status`, `fare`, `cancel_reason`, `source`.

Working enums from ticket #3:

- `status`: `completed` | `cancelled`
- `cancel_reason`: `driver` | `customer` | null
- `mode`: `ride_hail` | `metro` | `bus` | `aviation`
- `vehicle_type`: `auto` | `cab` | `train` | `airplane` | pass-through

Extra source columns may sit beside these in staged files. Zone columns are strings. The id system is not frozen.

## What is in `flowstate-datasets/` today

Present:

- `uber-data-india/ncr_ride_bookings.csv` (row 1, local copy for column checks only)
- `ola-uber-cancellation/Bookings.csv` (row 2, local copy for column checks only)
- `Bengaluru Ola.csv` (row 8, local copy for column checks only)
- `namma-metro/NammaMetro_Ridership_Processed.csv` (row 3)
- `bmtc/*.txt` (row 4, unzipped GTFS)
- `aviation/*.csv` (row 6)
- `xride-bangalore-data.csv` (row 7)

## Per-dataset columns (measured)

### Row 1 — Uber India 2024 (`ncr_ride_bookings.csv`)

Grain: one row per booking attempt. 150000 rows. 21 columns. Delhi NCR taxis only. Not Bengaluru.

Columns: `Date`, `Time`, `Booking ID`, `Booking Status`, `Customer ID`, `Vehicle Type`, `Pickup Location`, `Drop Location`, `Avg VTAT`, `Avg CTAT`, `Cancelled Rides by Customer`, `Reason for cancelling by Customer`, `Cancelled Rides by Driver`, `Driver Cancellation Reason`, `Incomplete Rides`, `Incomplete Rides Reason`, `Booking Value`, `Ride Distance`, `Driver Ratings`, `Customer Rating`, `Payment Method`.

Dates: 2024-01-01 to 2024-12-30. Pickup names are NCR places (Khandsa, Saket, Gurgaon Sector 56). This is not Bengaluru.

`Booking Status` counts: Completed 93000, Cancelled by Driver 27000, No Driver Found 10500, Cancelled by Customer 10500, Incomplete 9000.

`Vehicle Type`: Auto, Go Mini, Go Sedan, Bike, Premier Sedan, eBike, Uber XL.

No lat/lon. No platform flag.

Map to canonical:

- `trip_id` ← `Booking ID` (quoted in the file)
- `city` ← `ncr`. Do not write `bengaluru`
- `mode` ← `ride_hail`
- `vehicle_type` ← pass-through (`Auto`, `Go Mini`, …). `Auto` matches the enum. Cab-like values stay pass-through
- `timestamp` ← `Date` + `Time`
- `pickup_zone` / `drop_zone` ← place-name strings until T3
- `status` ← `Completed` → `completed`. Driver/customer cancel → `cancelled`. `No Driver Found` and `Incomplete` are extra. Keep them in an extra column
- `fare` ← `Booking Value` (null on non-completed rows)
- `cancel_reason` ← driver/customer from the two reason columns. Null otherwise
- `source` ← `anilrohan` or `kaggle_uber_india`

### Row 2 — hetmengar Ola/Uber (`Bookings.csv`)

Grain: one row per booking. 103024 rows. 21 columns, not 13.

Columns: `Date`, `Time`, `Booking_ID`, `Booking_Status`, `Customer_ID`, `Vehicle_Type`, `Pickup_Location`, `Drop_Location`, `V_TAT`, `C_TAT`, `Canceled_Rides_by_Customer`, `Canceled_Rides_by_Driver`, `Incomplete_Rides`, `Incomplete_Rides_Reason`, `Booking_Value`, `Payment_Method`, `Ride_Distance`, `Driver_Ratings`, `Customer_Rating`, `Vehicle Images`, plus a trailing empty header.

UTF-8 BOM on `Date`. `Date` is a datetime in July 2024 (`2024-07-01` to `2024-07-31`). Pickup names are Bengaluru areas (Banashankari, Whitefield). No `Platform` / aggregator column. You cannot split Ola vs Uber from this file.

`Booking_Status`: Success 63967, Canceled by Driver 18434, Canceled by Customer 10499, Driver Not Found 10124.

`Vehicle_Type`: Prime Sedan, eBike, Auto, Prime Plus, Bike, Prime SUV, Mini.

`Vehicle Images` is the Excel error `#NAME?`. Drop it. `Canceled_Rides_by_Customer` holds the reason text, not a 0/1 flag.

Map:

- `trip_id` ← `Booking_ID`
- `city` ← `bengaluru`
- `mode` ← `ride_hail`
- `status` ← `Success` → `completed`. The two cancel statuses → `cancelled`. `Driver Not Found` stays extra
- `fare` ← `Booking_Value`
- `cancel_reason` ← driver/customer from the two reason columns
- `source` ← `hetmengar`

### Row 8 — Bangalore Ola (`Bengaluru Ola.csv`)

Grain: one row per booking. 49999 rows. 21 columns.

Columns match row 1 naming, with these differences:

- `Cancelled  by Customer` (two spaces)
- `Reason for Cancelling by Customer`
- `Reason for Cancelling by Driver`
- `Payment Method` sits before `Ride Distance`

Dates: 01/01/2024 to 31/01/2024 (`DD/MM/YYYY`). Pickup/drop values are `Area-1` … `Area-50`, not street names.

`Booking Status`: Success 33484, Cancelled by Driver 9610, Cancelled by Customer 3799, Incomplete 3106. No “no driver” status.

Map like row 2 (`city` = `bengaluru`, `source` = `muhammadahmadmujahid` or `kaggle_ola_bangalore`). Keep `Area-*` as zone strings until T3.

### Row 4 — BMTC GTFS (`bmtc/`)

Unzipped GTFS text, not `bmtc.zip`. No `csv/` convenience exports. No `calendar_dates.txt`.


| File                  | Columns                                                                                                                    | Rows (incl. header) |
| --------------------- | -------------------------------------------------------------------------------------------------------------------------- | ------------------- |
| `agency.txt`          | `agency_id`, `agency_name`, `agency_url`, `agency_timezone`, `agency_lang`                                                 | 2                   |
| `calendar.txt`        | `monday`…`sunday`, `start_date`, `end_date`, `service_id`                                                                  | 2                   |
| `routes.txt`          | `route_long_name`, `route_short_name`, `agency_id`, `route_type`, `route_id`                                               | 4435                |
| `trips.txt`           | `route_id`, `service_id`, `trip_headsign`, `direction_id`, `shape_id`, `trip_id`                                           | 57837               |
| `stops.txt`           | `stop_name`, `parent_station`, `zone_id`, `stop_id`, `stop_desc`, `stop_lat`, `stop_lon`, `location_type`, `platform_code` | 9961                |
| `stop_times.txt`      | `trip_id`, `arrival_time`, `departure_time`, `stop_id`, `stop_sequence`, `stop_headsign`                                   | 1540687             |
| `shapes.txt`          | `shape_id`, `shape_pt_lat`, `shape_pt_lon`, `shape_pt_sequence`, `shape_dist_traveled`                                     | 2492305             |
| `fare_attributes.txt` | `fare_id`, `price`, `currency_type`, `payment_method`, `transfers`, `transfer_duration`, `agency_id`                       | 111                 |
| `fare_rules.txt`      | `fare_id`, `route_id`, `origin_id`, `destination_id`                                                                       | 170162              |
| `feed_info.txt`       | publisher, URL, lang, start/end, version, contact                                                                          | 2                   |
| `attributions.txt`    | attribution fields                                                                                                         | 3                   |
| `translations.txt`    | `table_name`, `field_name`, `record_id`, `language`, `translation`                                                         | 9504                |


`stops.zone_id` is a GTFS fare-zone code (`NBV12B`). It is not `pickup_zone` in the canonical schema.

GTFS has no ridership count. `route_type` is `3` (bus). `agency_timezone` is `Asia/Kolkata`. Feed version `20260907`.

Canonical map for a bus schedule row is incomplete by design. Extra GTFS columns stay extra. `mode` = `bus`. `vehicle_type` = pass-through or `bus`. `city` = `bengaluru`. `source` = `bmtc_gtfs`. Hive tables for GTFS stay separate from `rides` (issue #1).

### Row 3 — Namma Metro (`namma-metro/NammaMetro_Ridership_Processed.csv`)

Grain: one Bengaluru network-day total. 438 rows. Dates `2024-10-26` to `2026-07-15`. 190 calendar days are missing (scrape gaps, not a closed interval).

Columns: `Record Date`, `Day of Week`, `Traffic Band`, `Smart Cards`, `NCMC`, `Commute`, `Tokens`, `QR`, `Group Ticket`, `Casual`, `Total`.

Derived checks on this file hold for every row:

- `Commute` = `Smart Cards` + `NCMC`
- `Casual` = `Tokens` + `QR` + `Group Ticket`
- `Total` = `Commute` + `Casual`

`Traffic Band` values: Early Week, Mid Week, Saturday, Sunday. `Total` ranges 398878–1207223.

This is not station-level and not trip-level. `mode` = `metro`. `city` = `bengaluru`. `vehicle_type` = `train`. `timestamp` ← `Record Date`. `fare` / cancel fields are null. `trip_id` must be synthetic per date. Keep payment-method counts as extra columns. `source` = `namma_metro`.

### Row 6 — DGCA aviation (`aviation/`)

Several tables. Different grain. Not one ride-shaped CSV.


| File                                  | Grain                                   | Columns                                                                                                                               |
| ------------------------------------- | --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `daily.csv`                           | date, 1276 rows, 166 columns            | `Date` plus national daily metrics (movements, footfalls, cargo, OTP, grievances, UDAN, …)                                            |
| `domestic/city.csv`                   | year-month city pair, 65166 rows        | `Year`, `Month`, `City1`, `City2`, `PaxToCity2`, `PaxFromCity2`, `FreightToCity2`, `FreightFromCity2`, `MailToCity2`, `MailFromCity2` |
| `domestic/carrier.csv`                | year-month airline, 4117 rows           | `Type`, `Airline`, `Year`, `Month`, aircraft/pax/cargo totals, load factors                                                           |
| `international/city.csv`              | year-quarter city pair                  | `Year`, `Quarter`, `City1`, `City2`, pax and freight each way                                                                         |
| `international/country.csv`           | year-quarter country                    | `Year`, `Quarter`, `Country`, pax and freight each way                                                                                |
| `international/carrier.csv`           | year-quarter airline, month slots M1–M3 | pax and freight to/from India per month in the quarter                                                                                |
| `international/carrier_quarterly.csv` | year-quarter airline                    | `PaxToIndia`, `PaxFromIndia`, freight each way                                                                                        |


`daily.csv` has no city column. `domestic/city.csv` is the OD table. City name is `BENGALURU` (7655 pair-rows, 2015-04 to 2026-05). `Year` on international files is two-digit (`15`, `22`).

`mode` = `aviation`. `vehicle_type` = `airplane`. `city` ← `bengaluru` when `City1` or `City2` is `BENGALURU`. Do not force these rows into hourly zones. Warehouse tables stay separate from `rides`. Kafka shape waits on T4.

### Row 7 — XRides (`xride-bangalore-data.csv`)

Grain: one booking. 43431 rows. 19 columns.

Columns: `id`, `user_id`, `vehicle_model_id`, `package_id`, `travel_type_id`, `from_area_id`, `to_area_id`, `from_city_id`, `to_city_id`, `from_date`, `to_date`, `online_booking`, `mobile_site_booking`, `booking_created`, `from_lat`, `from_long`, `to_lat`, `to_long`, `Car_Cancellation`.

The cancel header is `Car_Cancellation`, not `car_cancellation`. `booking_created` range: 2013-01-01 01:39 to 2013-11-24 14:50. 93 rows have empty `from_lat`.

`travel_type_id`: `2` = 34292, `3` = 7550, `1` = 1589. Area and city ids are integers with no lookup file in this extract. `vehicle_model_id` is an integer code (27 values). `Car_Cancellation`: 0 = 40299, 1 = 3132.

This is the only ride-hail extract with point coordinates and a booking timestamp.

Map:

- `trip_id` ← `id`
- `city` ← `bengaluru`
- `mode` ← `ride_hail`
- `vehicle_type` ← pass-through from `vehicle_model_id` until a lookup exists
- `timestamp` ← `booking_created`
- `pickup_zone` / `drop_zone` ← strings derived later (T3). Do not use opaque area ids as the frozen system
- `status` ← `Car_Cancellation` 1 → `cancelled`, else `completed`
- `cancel_reason` ← null (no driver/customer split)
- `fare` ← null (no fare column)
- `source` ← `xride_bangalore`

Keep lat/lon and travel type as extra columns.

## Canonical coverage vs enums


| Source column family                            | Fits ticket #3 enums?                                                        |
| ----------------------------------------------- | ---------------------------------------------------------------------------- |
| Ride-hail completed / success                   | `status=completed`                                                           |
| Ride-hail driver/customer cancel                | `status=cancelled` plus `cancel_reason`                                      |
| No Driver Found / Driver Not Found / Incomplete | extra column. Do not fold into the two-value `status` without a later ticket |
| Vehicle names (Go Mini, Prime SUV, Bike, eBike) | pass-through                                                                 |
| Metro daily totals                              | `mode=metro`, `vehicle_type=train`, no fare/cancel                           |
| GTFS                                            | `mode=bus`. Not a ride event                                                 |
| Aviation aggregates                             | `mode=aviation`, `vehicle_type=airplane`. Not a ride event                   |


Issue #1: first city is Bengaluru and first mode is `ride_hail`. NCR is a second city for taxis only.

## City and time alignment

Keep Bengaluru multimodal joins on `city=bengaluru`. Keep NCR taxi jobs on `city=ncr`. Do not join the two cities.

| Source | City | Role | Time window | Grain |
| --- | --- | --- | --- | --- |
| Row 8 Bangalore Ola | bengaluru | ride-hail | 2024-01-01 to 2024-01-31 | booking |
| Row 2 hetmengar | bengaluru | ride-hail | 2024-07-01 to 2024-07-31 | booking |
| Row 3 Namma Metro | bengaluru | metro | 2024-10-26 to 2026-07-15 (190 gaps) | day |
| Row 6 DGCA domestic city | BENGALURU pairs | aviation | monthly 2015-04 to 2026-05 | month × city-pair |
| Row 4 BMTC GTFS | bengaluru | bus | feed `20260907` (schedule, not ridership) | stop / trip |
| Row 7 XRides | bengaluru | ride-hail coords | 2013-01-01 to 2013-11-24 | booking + lat/lon |
| Row 1 Uber India | ncr | taxi only | 2024-01-01 to 2024-12-30 | booking |

Bengaluru overlap that actually exists:

- Metro trends + payment split: row 3 alone.
- Metro vs aviation (month grain): row 3 rolled to month vs row 6 `BENGALURU` from 2024-10 through 2026-05.
- Cancel / fare / vehicle (Bengaluru): rows 2 and 8, not the same months as metro.
- Zone heatmaps with coordinates (Bengaluru): row 7 only (2013).
- BMTC route/stop/timetable: row 4. No ticketing count.

NCR taxi overlap: row 1 covers calendar 2024. Place names, not lat/lon. Cancel, fare, vehicle type, and zone/hour demand (name-level) are valid. Do not correlate row 1 with Namma Metro, BMTC, or DGCA Bengaluru.

Gap: no Bengaluru ride-hail extract covers 2024-10-26 onward. Rows 2 and 8 end before row 3 starts. Do not use NCR taxis to fill Bengaluru metro days.

The **Bengaluru alignment window** is 2024-10-26 through the last shared metro/aviation month (DGCA `BENGALURU` through 2026-05). Real metro and aviation series live on that clock. The RNN resampler (ADR-0010) emits **taxi, metro, bus, and aviation** events on that clock for both the lake and Kafka. It must learn cross-modal correlation (example: aviation event at the airport zone, then taxi fare/demand in that region). Jan/Jul 2024 Bengaluru bookings stay veracity seeds. DGCA has no per-flight landings; aviation events in the stream are synthetic event-grain rows. NCR Uber stays a separate taxi batch.

## Analytics scope (what these files can answer)

Descriptive:

- Demand heatmaps by pickup zone/hour: Bengaluru coords from row 7. Bengaluru names/`Area-*` from rows 2 and 8. NCR names from row 1. T3 still owns zone ids.
- Booking value and revenue by vehicle type: Bengaluru rows 2 and 8. NCR row 1. Row 7 has no fare.
- Metro ridership trends and payment-method split: row 3 (Bengaluru only).
- BMTC network: row 4 (Bengaluru only).
- DGCA city-pairs and carrier share: row 6, filter `BENGALURU`.

Diagnostic:

- Cancellation driver vs customer by time/location/vehicle/value: Bengaluru rows 2 and 8. NCR row 1 (taxi only). Partition or filter on `city`.
- Correlations across modes: Bengaluru only. Train and query the resampler stream (ADR-0010). Do not join NCR taxis to Namma or DGCA.
- Aviation dips/recoveries and seasonality: row 6 Bengaluru pairs, monthly.

Predictive (if time allows):

- Metro ridership forecast: row 3 (438 days, 190 holes — impute or skip gaps).
- Bengaluru predictive (airport landing → taxi surge, and similar): multimodal RNN resampler (ADR-0010) for lake and Kafka. Needs an airport / catchment zone (T3). Streaming surge job (T29) conditions on aviation events in the window.
- NCR taxi surge: row 1 on its own 2024 timestamps. Do not feed it into ADR-0010.

## Ticket #3 work this inventory supports

- Put this table in an ADR (new or an amendment of ADR-0005). That ADR write is not this file.
- Keep JSON schema and Spark StructType aligned with `RIDE_COLUMNS`.
- Fixture seed: use a few Bengaluru ride-hail rows plus one extra-column example (lat/lon or `Incomplete`). Do not download Kaggle in T2.
- Zone fields: type as string. Do not pick H3, geohash, or stop_id here.

## Explicitly out of this ticket

Do not treat these as T2 decisions:

- H3 / geohash / Nominatim geocoding
- A multi-state stream event (`REQUESTED`, `DRIVER_ASSIGNED`, …)
- Amplify row counts (ADR-0006 / T5)
- Hive star-schema fact names

Ride-event resampler behaviour is ADR-0010. Ticket #3 still does not implement the RNN. T32 (#34) does.