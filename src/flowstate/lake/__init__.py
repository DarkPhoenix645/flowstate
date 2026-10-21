"""HDFS data lake. Host ``data/`` is the copy-in, not the lake."""

from flowstate.lake.ridehail import HDFS_RIDEHAIL_DIR, put_ridehail

__all__ = ["HDFS_RIDEHAIL_DIR", "put_ridehail"]
