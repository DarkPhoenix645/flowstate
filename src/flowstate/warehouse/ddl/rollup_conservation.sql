-- One row. Children match the amplify manifest parents.
-- conservation_check writes the parquet. This DDL only publishes it.
-- #27 draws the stat panel. It does not own the job.

CREATE EXTERNAL TABLE IF NOT EXISTS rollup_conservation (
  seed BIGINT,
  bundle_version STRING,
  `rows` BIGINT,
  checks BIGINT,
  failures BIGINT,
  ok BOOLEAN
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/rollup_conservation';
