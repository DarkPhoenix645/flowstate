"""Prove Spark can list and write the HDFS lake at replication 1.

Ticket #7. Submit on Compose Spark. Do not import host-only packages.
The apache/spark image has PySpark. It does not have pydantic.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

NAMENODE = "hdfs://namenode:9000"
LAKE = f"{NAMENODE}/lake"
PROBE = f"{NAMENODE}/tmp/spark_hdfs_probe"


def main() -> None:
    spark = SparkSession.builder.appName("flowstate-hdfs-probe").getOrCreate()
    jvm = spark._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    default_fs = conf.get("fs.defaultFS")
    print(f"fs.defaultFS={default_fs}")
    if default_fs != NAMENODE:
        raise SystemExit(
            f"expected fs.defaultFS={NAMENODE}, got {default_fs!r}"
        )

    # Hadoop loads hdfs-site.xml when the HDFS FileSystem class
    # initialises, not when Spark builds the bare Configuration.
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(
        jvm.java.net.URI(NAMENODE), conf
    )
    replication = conf.get("dfs.replication")
    print(f"dfs.replication={replication}")
    if replication != "1":
        raise SystemExit(
            f"expected dfs.replication=1, got {replication!r}"
        )
    Path = jvm.org.apache.hadoop.fs.Path
    lake = Path(LAKE)
    listed = LAKE if fs.exists(lake) else NAMENODE + "/"
    print(f"ls {listed}")
    for status in fs.listStatus(Path(listed)):
        print(f"  {status.getPath()}")

    spark.createDataFrame([(1, "ok")], ["n", "msg"]).write.mode(
        "overwrite"
    ).parquet(PROBE)
    got = [(r.n, r.msg) for r in spark.read.parquet(PROBE).collect()]
    print(f"read-back {PROBE} rows={got}")
    if got != [(1, "ok")]:
        raise SystemExit(f"probe read-back did not match: {got}")

    files = 0
    for status in fs.listStatus(Path(PROBE)):
        if status.isDirectory():
            continue
        files += 1
        got = int(status.getReplication())
        print(f"  {status.getPath()} replication={got}")
        if got != 1:
            raise SystemExit(
                f"write used replication {got}, expected 1"
            )
    if files == 0:
        raise SystemExit(f"no files under {PROBE}")
    spark.stop()
    print("hdfs probe passed")


if __name__ == "__main__":
    main()
