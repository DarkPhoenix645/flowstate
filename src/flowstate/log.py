"""One logging setup for FlowState commands.

The library is the stdlib ``logging`` module. Call
``configure_logging()`` once from each command entry point. Modules
log with ``logging.getLogger(__name__)``.
"""

from __future__ import annotations

import logging
import sys

_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"
_DATE = "%H:%M:%S"


def configure_logging(level: int = logging.INFO) -> None:
    """Attach one stdout handler to the root logger.

    A second call only raises the level. It does not add another
    handler.
    """
    root = logging.getLogger()
    root.setLevel(level)
    if root.handlers:
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATE))
    root.addHandler(handler)
    # kafka-python logs every dial at INFO. Failures stay at ERROR.
    logging.getLogger("kafka").setLevel(logging.WARNING)
