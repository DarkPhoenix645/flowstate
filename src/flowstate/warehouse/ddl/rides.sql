-- City-day ride-hail clock. One row per day. Not a passenger trip.
-- city_hour and ward_hour stay on hdfs://namenode:9000/lake/namma_yatri/.
-- Partition columns are directory names. They are not stored in the parquet.

CREATE EXTERNAL TABLE IF NOT EXISTS rides (
  trip_id STRING,
  mode STRING,
  `timestamp` STRING,
  pickup_zone STRING,
  drop_zone STRING,
  status STRING,
  fare DOUBLE,
  cancel_reason STRING,
  source STRING,
  booking BIGINT,
  done_ride BIGINT,
  cancel_ride BIGINT,
  earning_per_completed_ride_proxy DOUBLE
)
PARTITIONED BY (city STRING, vehicle_type STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/rides';
