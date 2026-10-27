"""Insert the first model-config versions.

The scenario policy file is the seed input. The running process
does not read it. A kind that already has an active version is left
as it is.
"""

from __future__ import annotations

import logging

from flowstate.config import get_settings
from flowstate.config.model import ResampleConfig
from flowstate.config.postgres import KINDS, ConfigStore
from flowstate.log import configure_logging
from flowstate.resample.scenario import load_policy

log = logging.getLogger(__name__)


def seed(database_url: str) -> list[str]:
    """Activate a seed version for each kind that has none."""
    store = ConfigStore(database_url)
    active = store.active()
    inserted: list[str] = []
    if "scenario_policy" not in active:
        policy = load_policy()
        store.put_version(
            "scenario_policy",
            policy.model_dump(mode="json"),
            note="seed",
        )
        inserted.append("scenario_policy")
    if "resample_config" not in active:
        store.put_version(
            "resample_config",
            ResampleConfig().model_dump(mode="json"),
            note="seed",
        )
        inserted.append("resample_config")
    return inserted


def main() -> None:
    configure_logging()
    settings = get_settings()
    inserted = seed(settings.config_database_url)
    if inserted:
        log.info("seeded %s", ", ".join(inserted))
    else:
        log.info("active versions already present for %s", ", ".join(KINDS))


if __name__ == "__main__":
    main()
