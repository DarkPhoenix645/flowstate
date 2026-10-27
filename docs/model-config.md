# Model config

Model config is the scenario policy and the resample config for a run. Postgres stores it. The process reads the active version of each kind. It does not read `scenario_policy.json` or the `FLOWSTATE_ASSUMED_*` variables at run time.

Mobility data stays on disk and on HDFS. This database does not hold rides.

Hive still uses Derby for the metastore. Derby records Hive table names and partition locations. Postgres does not replace that catalog. See [ADR-0002](adr/0002-lambda-pipeline-kafka-spark-hdfs-hive.md) and [ADR-0018](adr/0018-postgres-for-model-config.md).

## Start

1. `make up`
2. `make config-seed`
3. `make api`

The API listens on `http://127.0.0.1:8090`. Bruno collection: `bruno/`. Select the `DEV` environment.

`make config-seed` inserts a version only when that kind has no active row. The scenario policy seed is `src/flowstate/resample/scenario_policy.json`. The resample seed is the code default document. A later edit is a new version through the API.

## Deploy settings

These values stay in the environment. They are not model config. A blank `FLOWSTATE_CONFIG_DATABASE_URL` uses the local default.

| Variable | Default | Meaning |
| --- | --- | --- |
| `FLOWSTATE_DATA_DIR` | `data` | Host data root. |
| `FLOWSTATE_DATASETS_DIR` | `data/raw` | Source extracts. Same directory as `data/raw`. |
| `FLOWSTATE_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9094` | Host Kafka listener. Containers use `kafka:9092`. |
| `FLOWSTATE_SPARK_MASTER` | `spark://spark:7077` | Spark master URL. |
| `FLOWSTATE_HDFS_NAMENODE` | `hdfs://namenode:9000` | Lake URI. |
| `FLOWSTATE_GOOGLE_MAPS_API_KEY` | empty | Geocode key for station and airport pins. Cached under `data/geocode/`. |
| `FLOWSTATE_CONFIG_DATABASE_URL` | `postgresql://flowstate:flowstate@127.0.0.1:5432/flowstate` | Postgres URL. The password is the Compose `POSTGRES_PASSWORD`. |
| `FLOWSTATE_API_HOST` | `127.0.0.1` | API bind address. |
| `FLOWSTATE_API_PORT` | `8090` | API port. |
| `FLOWSTATE_OPENSKY_CLIENT_ID` | empty | OpenSky client id. Pair 1. |
| `FLOWSTATE_OPENSKY_CLIENT_SECRET` | empty | OpenSky client secret. Pair 1. |

Pairs 2–10 use the same names with a `_2` … `_10` suffix. Set both values or neither.

`FLOWSTATE_ASSUMED_*` still parse into the settings object. A run does not read them. `GET /api/config` reads them only when `resample_config` has no active version. Change those numbers with `PUT /api/config` or `PUT /api/model-config/resample_config`.

## Kinds

`scenario_policy` and `resample_config`. Each active row is one full document. An edit inserts a new row and makes it active. Rows are immutable.

`config_hash` is the hash of the two document hashes. The same content keeps the same hash. A whitespace-only edit does not change it.

### scenario_policy

| Field | Meaning |
| --- | --- |
| `schema_version` | Policy document version. The seed uses `"1"`. |
| `kia_catchment_wards` | Ward ids that receive synthetic flight-response rides. |
| `notes` | Free text. A text change changes `config_hash`. |
| `multiplier_floor` | Lowest metro suppression factor. Default `0.2`. |
| `scenarios` | One object per known scenario type. |

Known types: `flight_arrival`, `flight_departure`, `metro_disruption`, `custom_shock`, `local_holiday_event`. The key and `scenario_type` must match. An unknown type is rejected.

Each scenario object:

| Field | Meaning |
| --- | --- |
| `enabled` | The producer accepts commands of this type only when this is true. |
| `affected_modes` | Modes the command may name. |
| `time_lag_profile` | `min_minutes` and `max_minutes` for the response lag. |
| `intensity_multiplier` | Default command multiplier. |
| `shift_fraction` | Share moved when `shift_shares` is absent. Must be in `[0, 1]`. |
| `maximum_multiplier` | Cap for a non-metro multiplier. |
| `shift_shares` | Optional map of mode to share. Each share is in `[0, 1]`. The sum must be at most 1. |
| `evidence_grade` | Label such as `assumption_driven`. |
| `evidence_window` | Optional evidence window. |
| `rides_per_arrival` | Ride-hail lifecycles per injected arrival. |
| `rides_per_departure` | Ride-hail lifecycles per injected departure. |
| `notes` | Free text on that scenario. |

### resample_config

| Field | Default | Meaning |
| --- | --- | --- |
| `cab_share` | `0.22` | Ride-hail share of the thin city-hour slice. A prior, not a measurement. |
| `bus_hour_weight_metro` | `0.5` | Metro weight in the bus hour blend. |
| `bus_hour_weight_ridehail` | `0.5` | Ride-hail weight in the bus hour blend. |
| `assumed_weekday_boardings` | `4600000` | Synthetic weekday boardings. Not a BMTC count. |
| `assumed_bus_capacity` | `41.3` | Seats. Not seats plus standing. |
| `assumed_max_load_factor` | `1.4` | Synthetic load guardrail. |
| `metro_substitution_multiplier` | `1.0` | Metro substitution factor. No measured citywide coefficient. |
| `bus_hour_profile` | `metro_ridehail_blend` | Hour-shape name. |
| `bus_peak_share_low` | `0.47` | Low end of the peak-window share band. Must be at most `bus_peak_share_high`. |
| `bus_peak_share_high` | `0.51` | High end of that band. |
| `bus_trip_length_km_low` | `10.7` | Low end of the trip-length band, in km. |
| `bus_trip_length_km_high` | `10.9` | High end of that band. |
| `fare_sigma` | `0.30` | Fare noise. |
| `clock_vehicle_type` | `Auto` | Archive token for the clock vehicle. |
| `provenance_note` | see the model | Text stored with the document. |
| `outside_widen` | `0.5` | How far an out-of-window day moves toward a uniform ward share. |
| `coverage.sparse_fraction` | `0.5` | A covered day needs at least this fraction of the reference quantile. |
| `coverage.reference_quantile` | `75` | Quantile used as the coverage reference. |
| `coverage.failed_statuses` | `failed`, `quota` | OpenSky statuses that fail a window. |
| `coverage.minimum_covered_days` | `60` | Minimum covered days for the fit. |

## API

No auth in v1. An invalid document is HTTP 422 and writes no row.

List routes take `limit` (1 to 100, default 50) and `cursor`. The body includes `next_cursor`. The last page has `next_cursor` null. There is no offset.

| Method | Path | Effect |
| --- | --- | --- |
| `GET` | `/api/model-config/{kind}` | Active version, `source` (`db`), and `config_hash` when both kinds are active. |
| `GET` | `/api/model-config/{kind}/versions` | Versions, newest first. |
| `GET` | `/api/model-config/{kind}/diff?from=&to=` | Field diff of two versions of that kind. |
| `PUT` | `/api/model-config/{kind}` | Validate, insert, and activate. Body is the document, or `{"content": {...}, "note": "..."}`. |
| `POST` | `/api/model-config/{kind}/activate` | Body `{"version_id": "1"}`. Same kind only. |
| `POST` | `/api/model-config/{kind}/deactivate` | Clear the active pointer. Versions stay. The next run stops until you activate or seed again. |
| `GET` | `/api/config` | Knobs. Each knob has `value`, `source` (`db`, `env`, or `default`), and `editable`. |
| `PUT` | `/api/config` | Merge writable fields into the resolved resample document and save a new version. `replay_seed` is stored on `run_config`. |
| `POST` | `/api/config/reset` | Clear the `resample_config` pointer and the `run_config` row. Versions stay. |
| `GET` | `/api/system` | TCP check of the namenode, Kafka, Spark, Hive, and Grafana host ports. |
| `GET` | `/api/datasets` | Rows from `datasets_dir/provenance/manifest.json`. |
| `GET` | `/api/datasets/opensky?direction=arrivals\|departures` | Cached window count. Incomplete departures are HTTP 409. |
| `GET` | `/api/benchmarks` | Rows from `benchmark`. HTTP 404 when the table is empty. |
| `GET` | `/api/events/snapshot` | One ADR-0008 envelope. The response `fixture` is true. The event stays a valid envelope. |
| `GET` | `/api/jobs` | The job registry and each parameter schema. |
| `POST` | `/api/runs` | Body `{kind, job, params, backend?}`. HTTP 202 and the `run_id`. |
| `GET` | `/api/runs` | Runs. Optional `kind` and `status` queries. |
| `GET` | `/api/runs/{run_id}` | One run. |
| `POST` | `/api/runs/{run_id}/cancel` | Cancel a queued or running run. |

`{kind}` is `scenario_policy` or `resample_config`.

`init.sql` is schema version 3. It runs only on a fresh volume. A volume from an older version does not gain `run`. Recreate that volume.

Writable knobs are `assumed_weekday_boardings` (`4600000`, `4000000`, `4800000`, `3310000`), `assumed_bus_capacity` (`41.3`), and `assumed_max_load_factor` (`1.0`, `1.2`, `1.4`). `replay_seed` is an integer. Any other field is HTTP 422 and writes no row.
