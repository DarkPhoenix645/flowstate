"""Canonical analysis tables for one registered city.

The command reads data/raw/ and writes data/normalized/.
Table builders live in ``tables``. Pin lookup lives in ``places``.
City facts live in ``cities``. It does not call OpenSky.
"""

from flowstate.normalize.run import run

__all__ = ["run"]
