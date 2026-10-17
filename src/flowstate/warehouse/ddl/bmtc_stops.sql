-- BMTC stop list from the GTFS feed. This is not the rides table.
-- Hive-on-HDFS location is still the T7 spike. Do not apply this until then.

CREATE TABLE IF NOT EXISTS bmtc_stops (
  stop_id STRING,
  stop_name STRING,
  stop_lat STRING,
  stop_lon STRING
)
STORED AS PARQUET;
