import requests
from urllib.parse import unquote
import datetime as dt
from datetime import datetime, timezone
import numpy as np

from pathlib import Path

import logging
logging.basicConfig(
    format='%(asctime)s %(message)s', 
    datefmt='%m/%d/%Y %I:%M:%S %p',
    level = logging.INFO)



CMR_DATE_FMT = '%Y-%m-%dT%H:%M:%SZ' # format requirement for datetime search

def get_date_limits():
    url = "https://raw.githubusercontent.com/johnarban/tempo-data-holdings/main/manifest.json";
    manifest = requests.get(url).json()
    ts = manifest['released']['timestamps']
    times = np.array([int(t) for t in ts])    

    last_time = times[-1] / 1000
    last_time_dt = dt.datetime.fromtimestamp(last_time, tz=timezone.utc)

    print(f"Last time: {last_time_dt.strftime(CMR_DATE_FMT)}")

    # Define the temporal range for the search
    start_date = last_time_dt + dt.timedelta(hours=1)
    end_date = dt.datetime.now(tz=timezone.utc)
    print(f"Search Start Date: {start_date.strftime(CMR_DATE_FMT)}")
    print(f"Search End Date: {end_date.strftime(CMR_DATE_FMT)}")
    
    return start_date, end_date


def search_for_granules(concept_id, start_date, end_date, verbose =  False, dry_run = False):   
    granule_search_url = 'https://search.earthdata.nasa.gov/search/granules?p=C2930763263-LARC_CLOUD'
    concept_id =  concept_id# TEMPO NO2 V03 L# Data
    

    temporal_str = start_date.strftime(CMR_DATE_FMT) + ',' + end_date.strftime(CMR_DATE_FMT)
    print(f"Temporal String: {temporal_str}")


    cmr_url = 'https://cmr.earthdata.nasa.gov/search/granules'

    search_params = {
        'concept_id': concept_id,
        'temporal': temporal_str,
        'page_size': 1000,
    }

    headers = {
        'Accept': 'application/json',
    }
    
    if dry_run:
        return ['https://not.a.real.url']

    cmr_response = requests.get(cmr_url, params=search_params, headers=headers)
    
    if verbose:
        encoded_url = cmr_response.url
        decoded_url = unquote(encoded_url)
        print(f"CMR Request URL: {decoded_url}")
    
    granules = cmr_response.json()['feed']['entry']

    granule_urls = []

    for granule in granules:
        # item = next((item['href'] for item in granule['links'] if "opendap" in item["href"]), None)
        item = next((item['href'] for item in granule['links'] if "asdc-prod-protected" in item["href"]), None)
        if item != None:
            granule_urls.append(item)
        
    print(f"Found {len(granule_urls)} new granules")

    if len(granule_urls) == 0:
        print("No new data found")
        exit(0)
    return granule_urls