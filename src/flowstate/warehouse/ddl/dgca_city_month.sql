-- DGCA monthly Bengaluru city pairs. Not the rides table.
-- There is no Kafka topic for this table.

CREATE EXTERNAL TABLE IF NOT EXISTS dgca_city_month (
  year INT,
  month INT,
  city1 STRING,
  city2 STRING,
  year_month STRING,
  direction STRING,
  pax_to_city2 DOUBLE,
  pax_from_city2 DOUBLE,
  freight_to_city2 DOUBLE,
  freight_from_city2 DOUBLE,
  mail_to_city2 DOUBLE,
  mail_from_city2 DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/dgca_city_month';
