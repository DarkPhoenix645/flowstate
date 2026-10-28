CREATE EXTERNAL TABLE IF NOT EXISTS analytics_scenario_effect (
  replay_run_id STRING,
  mode STRING,
  place_id STRING,
  hour_of_day INT,
  direction STRING,
  baseline_value DOUBLE,
  fork_value DOUBLE,
  difference DOUBLE,
  ratio DOUBLE,
  evidence_grade STRING,
  cancel_rate_baseline DOUBLE,
  cancel_rate_fork DOUBLE,
  corpus_rows BIGINT,
  seed BIGINT,
  bundle_hash STRING,
  window_start STRING,
  window_end STRING
)
PARTITIONED BY (run_id STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/analytics_scenario_effect';

MSCK REPAIR TABLE analytics_scenario_effect;
