"""Download the BMRCL station-hour RTI workbooks from OpenCity."""

from __future__ import annotations

import logging

import requests

from flowstate.config import Settings

log = logging.getLogger("ingest.bmrcl")

RTI_URLS = {
    "aug_station_hourly.xlsx": (
        "https://data.opencity.in/dataset/"
        "369f18e0-4342-4809-b380-44f1d21d904f/resource/"
        "45259d6e-41b4-4012-8553-0d27219f83a7/download/"
        "a4ef58a3-29de-4787-b68e-56d716d0a95d.xlsx"
    ),
    "aug_entry_exit_od.xlsx": (
        "https://data.opencity.in/dataset/"
        "369f18e0-4342-4809-b380-44f1d21d904f/resource/"
        "e30ecb5f-e5f9-4971-8dd6-74d6966c33eb/download/"
        "12984f77-9bca-4854-a0d6-a527976080ac.xlsx"
    ),
    "sep_station_hourly.xlsx": (
        "https://data.opencity.in/dataset/"
        "369f18e0-4342-4809-b380-44f1d21d904f/resource/"
        "6127e293-0f45-4a36-8f6b-2a3e23ce1c17/download/"
        "station-hourly.xlsx"
    ),
    "station_codes.csv": (
        "https://data.opencity.in/dataset/"
        "369f18e0-4342-4809-b380-44f1d21d904f/resource/"
        "1ec4f39a-eede-44d4-8e1d-e8658cb89762/download/"
        "c47d24a2-8c27-4c3a-a9cd-05c0beafc83d.csv"
    ),
}


def run(settings: Settings) -> None:
    dest_dir = settings.datasets_dir / "bmrcl-rti"
    dest_dir.mkdir(parents=True, exist_ok=True)
    for name, url in RTI_URLS.items():
        log.info("GET %s", name)
        response = requests.get(url, timeout=180)
        response.raise_for_status()
        dest = dest_dir / name
        dest.write_bytes(response.content)
        log.info("saved %s (%s bytes)", name, dest.stat().st_size)
