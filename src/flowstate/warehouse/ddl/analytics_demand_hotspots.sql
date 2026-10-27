CREATE EXTERNAL TABLE IF NOT EXISTS analytics_demand_hotspots (
  ward_id STRING,
  hour_of_day INT,
  day_type STRING,
  demand_count BIGINT,
  corpus_rows BIGINT,
  seed BIGINT,
  bundle_hash STRING,
  window_start STRING,
  window_end STRING
)
PARTITIONED BY (run_id STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/analytics_demand_hotspots';

MSCK REPAIR TABLE analytics_demand_hotspots;
