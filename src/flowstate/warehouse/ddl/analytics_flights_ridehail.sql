CREATE EXTERNAL TABLE IF NOT EXISTS analytics_flights_ridehail (
  period_start STRING,
  n BIGINT,
  estimate DOUBLE,
  interval_low DOUBLE,
  interval_high DOUBLE,
  arrival_covered_days INT,
  departure_covered_days INT,
  required_covered_days INT,
  note STRING,
  corpus_rows BIGINT,
  seed BIGINT,
  bundle_hash STRING,
  window_start STRING,
  window_end STRING
)
PARTITIONED BY (run_id STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/analytics_flights_ridehail';

MSCK REPAIR TABLE analytics_flights_ridehail;
