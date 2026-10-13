"""Kafka envelope for the producer and the consumer.

ADR-0008. This module is not the staged-parquet column set.
That contract is ``flowstate.schema`` (``RIDE_COLUMNS``, the JSON schema,
and the Spark StructType). The envelope is filled in with the producer.
"""
