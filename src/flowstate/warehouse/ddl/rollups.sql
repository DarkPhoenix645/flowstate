-- #27 publishes the six rollups. Spark writes the parquet.
-- A panel reads these names. It does not read the corpus.

DROP TABLE IF EXISTS rollup_ridehail_day;
CREATE EXTERNAL TABLE rollup_ridehail_day (
  service_date STRING,
  bookings BIGINT,
  completions BIGINT,
  cancels BIGINT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/rollup_ridehail_day';

DROP TABLE IF EXISTS rollup_ridehail_hour;
CREATE EXTERNAL TABLE rollup_ridehail_hour (
  hour BIGINT,
  bookings BIGINT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/rollup_ridehail_hour';

DROP TABLE IF EXISTS rollup_metro_station_hour;
CREATE EXTERNAL TABLE rollup_metro_station_hour (
  station_id STRING,
  hour BIGINT,
  provenance_status STRING,
  entries_count BIGINT,
  exits_count BIGINT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/rollup_metro_station_hour';

DROP TABLE IF EXISTS rollup_metro_day;
CREATE EXTERNAL TABLE rollup_metro_day (
  service_date STRING,
  provenance_status STRING,
  volume BIGINT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/rollup_metro_day';

DROP TABLE IF EXISTS rollup_bus_hour;
CREATE EXTERNAL TABLE rollup_bus_hour (
  hour BIGINT,
  boardings BIGINT,
  alightings BIGINT,
  peak_onboard_leaving BIGINT,
  mean_onboard_leaving DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/rollup_bus_hour';

DROP TABLE IF EXISTS rollup_aviation_day;
CREATE EXTERNAL TABLE rollup_aviation_day (
  service_date STRING,
  direction STRING,
  provenance_status STRING,
  events BIGINT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/rollup_aviation_day';

DROP TABLE IF EXISTS sink_active_rides;
CREATE EXTERNAL TABLE sink_active_rides (
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  ward_id STRING,
  provenance_status STRING,
  scenario_id STRING,
  event_type STRING,
  source_run_id STRING,
  activity_count BIGINT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/stream-sink/active_rides';

DROP TABLE IF EXISTS sink_cancel_rate;
CREATE EXTERNAL TABLE sink_cancel_rate (
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  ward_id STRING,
  provenance_status STRING,
  scenario_id STRING,
  activity_count BIGINT,
  cancels BIGINT,
  cancel_rate DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/stream-sink/cancel_rate';

DROP TABLE IF EXISTS sink_surge;
CREATE EXTERNAL TABLE sink_surge (
  hour_start TIMESTAMP,
  ward_id STRING,
  hour_of_day INT,
  day_type STRING,
  scenario_id STRING,
  evidence_grade STRING,
  demand BIGINT,
  expected DOUBLE,
  z_score DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/stream-sink/surge';
