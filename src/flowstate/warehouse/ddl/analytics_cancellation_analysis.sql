CREATE EXTERNAL TABLE IF NOT EXISTS analytics_cancellation_analysis (
  ward_id STRING,
  hour_of_day INT,
  day_type STRING,
  vehicle_type STRING,
  bookings BIGINT,
  cancels BIGINT,
  cancel_rate DOUBLE,
  corpus_rows BIGINT,
  seed BIGINT,
  bundle_hash STRING,
  window_start STRING,
  window_end STRING
)
PARTITIONED BY (run_id STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/analytics_cancellation_analysis';

MSCK REPAIR TABLE analytics_cancellation_analysis;
