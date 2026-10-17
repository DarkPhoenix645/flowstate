-- RTI station-hour long table. Not the rides table.

CREATE EXTERNAL TABLE IF NOT EXISTS metro_station_hour (
  service_date STRING,
  station STRING,
  measure STRING,
  interval_label STRING,
  interval_start STRING,
  interval_end STRING,
  ends_at_last_train BOOLEAN,
  line_open BOOLEAN,
  count BIGINT,
  source_sheet STRING,
  source_row BIGINT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/metro_station_hour';
