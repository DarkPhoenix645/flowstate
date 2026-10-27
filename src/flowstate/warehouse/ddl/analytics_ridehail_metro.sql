CREATE EXTERNAL TABLE IF NOT EXISTS analytics_ridehail_metro (
  period_start STRING,
  grain STRING,
  n BIGINT,
  estimate DOUBLE,
  interval_low DOUBLE,
  interval_high DOUBLE,
  note STRING,
  corpus_rows BIGINT,
  seed BIGINT,
  bundle_hash STRING,
  window_start STRING,
  window_end STRING
)
PARTITIONED BY (run_id STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/analytics_ridehail_metro';

MSCK REPAIR TABLE analytics_ridehail_metro;
