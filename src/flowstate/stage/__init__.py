"""Stage parquet from the files already under flowstate-datasets/.

This package does not download. OpenSky is not a staging dataset.
"""

from flowstate.stage.bus import stage_bus
from flowstate.stage.dgca import stage_dgca
from flowstate.stage.metro import stage_metro
from flowstate.stage.ridehail import stage_ridehail

__all__ = [
    "stage_bus",
    "stage_dgca",
    "stage_metro",
    "stage_ridehail",
]
