-- Metro daily tracker. Not the rides table.
-- Files are loaded onto the namenode before this DDL runs.

CREATE EXTERNAL TABLE IF NOT EXISTS metro_daily (
  record_date STRING,
  smart_cards BIGINT,
  ncmc BIGINT,
  commute BIGINT,
  tokens BIGINT,
  qr BIGINT,
  group_ticket BIGINT,
  casual BIGINT,
  total BIGINT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/metro_daily';
