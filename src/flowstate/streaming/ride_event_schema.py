"""JSON event schema shared by producer and consumer."""

from flowstate.schema import RIDE_COLUMNS

# Spark StructType goes here once Structured Streaming is wired.
# Keep field names identical to RIDE_COLUMNS.

__all__ = ["RIDE_COLUMNS"]
