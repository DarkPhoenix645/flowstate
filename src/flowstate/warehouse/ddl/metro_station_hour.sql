-- RTI station-hour long table. This is not the rides table.
-- Hive-on-HDFS location is still the T7 spike. Do not apply this until then.

CREATE TABLE IF NOT EXISTS metro_station_hour (
  service_date STRING,
  station STRING,
  measure STRING,
  interval_label STRING,
  interval_start STRING,
  interval_end STRING,
  ends_at_last_train BOOLEAN,
  yellow_line_open BOOLEAN,
  count BIGINT,
  source_sheet STRING,
  source_row BIGINT
)
STORED AS PARQUET;
