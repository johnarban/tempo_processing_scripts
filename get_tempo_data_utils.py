import requests
from urllib.parse import unquote
import datetime as dt
from datetime import datetime, timezone, timedelta
import numpy as np

from pathlib import Path

import logging

logging.basicConfig(
    format="%(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p", level=logging.INFO
)

CMR_DATE_FMT = "%Y-%m-%dT%H:%M:%SZ"  # format requirement for datetime search


def times_are_close(time1, time2, tolerance):
    """
    Check if two times are the same within a given tolerance.

    :param time1: First time as a datetime object
    :param time2: Second time as a datetime object
    :param tolerance: Tolerance as a timedelta object
    :return: True if the times are within the tolerance, False otherwise
    """
    return time2 <= time1 or abs(time1 - time2) <= tolerance


def get_date_limits():
    url = "https://raw.githubusercontent.com/johnarban/tempo-data-holdings/main/manifest.json"
    manifest = requests.get(url).json()
    ts = manifest["released"]["timestamps"]
    times = np.array([int(t) for t in ts])

    last_time = times[-1] / 1000
    last_time_dt = dt.datetime.fromtimestamp(last_time, tz=timezone.utc)

    logging.debug(f"Last time: {last_time_dt.strftime(CMR_DATE_FMT)}")

    # Define the temporal range for the search
    start_date = last_time_dt
    end_date = dt.datetime.now(tz=timezone.utc)
    logging.info(f"Search Start Date: {start_date.strftime(CMR_DATE_FMT)}")
    logging.info(f"Search End Date: {end_date.strftime(CMR_DATE_FMT)}")

    return start_date, end_date, last_time_dt


def search_for_granules(
    concept_id, start_date, end_date, last_downloaded_time, verbose=False, dry_run=False
):
    granule_search_url = (
        "https://search.earthdata.nasa.gov/search/granules?p=C2930763263-LARC_CLOUD"
    )
    concept_id = concept_id  # TEMPO NO2 V03 L# Data

    temporal_str = (
        start_date.strftime(CMR_DATE_FMT) + "," + end_date.strftime(CMR_DATE_FMT)
    )
    logging.debug(f"Temporal String: {temporal_str}")

    cmr_url = "https://cmr.earthdata.nasa.gov/search/granules"

    search_params = {
        "concept_id": concept_id,
        "temporal": temporal_str,
        "page_size": 1000,
    }

    headers = {
        "Accept": "application/json",
    }

    if dry_run:
        return ["https://not.a.real.url"]

    cmr_response = requests.get(cmr_url, params=search_params, headers=headers)

    if verbose:
        encoded_url = cmr_response.url
        decoded_url = unquote(encoded_url)
        logging.debug(f"CMR Request URL: {decoded_url}")

    granules = cmr_response.json()["feed"]["entry"]

    granule_urls = []

    logging.info(f"Found {len(granules)} granules in search")

    for granule in granules:
        # item = next((item['href'] for item in granule['links'] if "opendap" in item["href"]), None)
        item = next(
            (
                item["href"]
                for item in granule["links"]
                if "asdc-prod-protected" in item["href"]
            ),
            None,
        )
        # print(urlTimeNearOrEarlier(item, last_downloaded_time), last_downloaded_time, item)
        if item != None and not urlTimeNearOrEarlier(item, last_downloaded_time):
            logging.debug("added")
            granule_urls.append(item)

    logging.info(f"Found {len(granule_urls)} new granules")

    if len(granule_urls) == 0:
        logging.info("No new data found")
        exit(0)
    return granule_urls


def urlTimeNearOrEarlier(urlString, time2):
    time1 = datetime.strptime(urlString.split("_")[-2], "%Y%m%dT%H%M%SZ").replace(
        tzinfo=timezone.utc
    )
    # print(time1, time2)
    return times_are_close(time2, time1, timedelta(minutes=1))
