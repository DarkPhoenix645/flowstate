-- Metro daily tracker. This is not the rides table.
-- Hive-on-HDFS location is still the T7 spike. Do not apply this until then.

CREATE TABLE IF NOT EXISTS metro_daily (
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
STORED AS PARQUET;
