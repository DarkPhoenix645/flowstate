CREATE EXTERNAL TABLE IF NOT EXISTS analytics_ridership_trends (
  grain STRING,
  period_start DATE,
  mode STRING,
  volume DOUBLE,
  station_id STRING,
  peak_share DOUBLE,
  payment_method STRING,
  line_phase STRING,
  corpus_rows BIGINT,
  seed BIGINT,
  bundle_hash STRING,
  window_start STRING,
  window_end STRING
)
PARTITIONED BY (run_id STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/analytics_ridership_trends';

MSCK REPAIR TABLE analytics_ridership_trends;
