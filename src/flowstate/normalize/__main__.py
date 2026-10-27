"""python -m flowstate.normalize

One command. Reads the files already on disk.
Downloads the ward polygon when that file is missing.
Sends Places lookups for station and airport cache misses.
"""

from __future__ import annotations

import argparse
import logging

from pydantic import BaseModel, ConfigDict

from flowstate.config import get_settings
from flowstate.log import configure_logging
from flowstate.normalize.cities import DEFAULT_CITY_ID
from flowstate.normalize.places.geocode import PlacesDisabled
from flowstate.normalize.run import run
from flowstate.provenance import ProvenanceError

log = logging.getLogger("normalize")


class NormalizeRequest(BaseModel):
    """One normalize run. Geocode stays on unless the flag turns it off."""

    model_config = ConfigDict(extra="forbid")

    city: str = DEFAULT_CITY_ID
    geocode: bool = True


def parse_normalize(argv: list[str] | None = None) -> NormalizeRequest:
    parser = argparse.ArgumentParser(prog="flowstate.normalize")
    parser.add_argument(
        "--city",
        default=NormalizeRequest.model_fields["city"].default,
    )
    parser.add_argument(
        "--geocode",
        action=argparse.BooleanOptionalAction,
        default=NormalizeRequest.model_fields["geocode"].default,
        help="look up station and airport cache misses (default %(default)s)",
    )
    args = parser.parse_args(argv)
    return NormalizeRequest(city=args.city, geocode=args.geocode)


def main(argv: list[str] | None = None) -> None:
    configure_logging()
    request = parse_normalize(argv)
    settings = get_settings()
    try:
        result = run(
            settings, city_id=request.city, geocode=request.geocode
        )
    except ValueError as exc:
        log.error("%s", exc)
        raise SystemExit(1) from exc
    except ProvenanceError as exc:
        log.error("%s", exc)
        raise SystemExit(1) from exc
    except PlacesDisabled as exc:
        log.error("%s", exc)
        raise SystemExit(1) from exc
    stats = result["geocode"]
    log.info(
        "normalize:geocode cached=%s fetched=%s missed=%s",
        stats["cached"],
        stats["fetched"],
        stats["missed"],
    )
    for name, coverage in result["tables"].items():
        log.info(
            "normalize:%s rows=%s sha256=%s",
            name,
            coverage["rows"],
            coverage["sha256"],
        )
    failed = [
        name
        for name, gate in result["gates"].items()
        if not gate["pass"]
    ]
    for name, gate in result["gates"].items():
        log.info("normalize:%s pass=%s", name, gate["pass"])
    pending = result["geocode"].get("needs_review") or []
    if pending:
        log.error("normalize:needs_review %s", ", ".join(pending))
    raise SystemExit(1 if failed or pending else 0)


if __name__ == "__main__":
    main()
