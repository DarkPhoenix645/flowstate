-- BMTC stops. Not the rides table.

CREATE EXTERNAL TABLE IF NOT EXISTS bmtc_stops (
  stop_id STRING,
  stop_name STRING,
  stop_lat STRING,
  stop_lon STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/bmtc_stops';
