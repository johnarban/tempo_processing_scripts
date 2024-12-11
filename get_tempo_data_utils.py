import os, sys, subprocess
import requests
from urllib.parse import unquote
import datetime as dt
from datetime import datetime, timezone, timedelta
import numpy as np

from pathlib import Path
from logger import setup_logging


logger = setup_logging(debug = False, name = 'get_utils')

TEMPO_CONCEPT_ID = "C2930763263-LARC_CLOUD"  # TEMPO NO2 V03 L# Data
CMR_DATE_FMT = "%Y-%m-%dT%H:%M:%SZ"  # format requirement for datetime search


def to_datetime(date_str, format = "%Y-%m-%d"):
    return datetime.strptime(date_str, format).replace(tzinfo=timezone.utc)


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

    logger.debug(f"Last time: {last_time_dt.strftime(CMR_DATE_FMT)}")

    # Define the temporal range for the search
    start_date = last_time_dt
    end_date = dt.datetime.now(tz=timezone.utc)
    logger.info(f"Search Start Date: {start_date.strftime(CMR_DATE_FMT)}")
    logger.info(f"Search End Date: {end_date.strftime(CMR_DATE_FMT)}")

    return start_date, end_date, last_time_dt


def search_for_granules(
    concept_id, start_date, end_date, last_downloaded_time, verbose=False, dry_run=False
):
    granule_search_url = (
        f"https://search.earthdata.nasa.gov/search/granules?p={concept_id}"
    )

    temporal_str = (
        start_date.strftime(CMR_DATE_FMT) + "," + end_date.strftime(CMR_DATE_FMT)
    )
    logger.debug(f"Temporal String: {temporal_str}")

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
        logger.debug(f"CMR Request URL: {decoded_url}")

    granules = cmr_response.json()["feed"]["entry"]

    granule_urls = []

    logger.info(f"Found {len(granules)} granules in search")

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
        if last_downloaded_time is None:
            granule_urls.append(item)
        elif item != None and not urlTimeNearOrEarlier(item, last_downloaded_time):
            logger.debug("added")
            granule_urls.append(item)

    logger.info(f"Found {len(granule_urls)} new granules")

    if len(granule_urls) == 0:
        logger.info("No new data found")
        exit(0)
    return granule_urls


def urlTimeNearOrEarlier(urlString, time2):
    time1 = to_datetime(urlString.split("_")[-2], "%Y%m%dT%H%M%SZ")
    # print(time1, time2)
    return times_are_close(time2, time1, timedelta(minutes=1))

def validate_directory_exists(path: Path | list[Path]):
    all_exist = True
    if not isinstance(path, list):
        path = [path]
    for p in path:
        if not p.exists():
            all_exist = False
            if p.is_file():
                logger.error(f"File does not exist: {p}")
            else:
                logger.error(f"Path does not exist: {p}")
    return all_exist
    
            
def ensure_directory(path: Path | str, *args, **kwargs):

    # check if path is a file, if so delete and create directory
    # if directory exists, do nothing
    # if directory does not exist, create it
    path = Path(path)
    if path.is_file():
        raise ValueError(f"Path is a file: {path}")
        path.unlink()
        path.mkdir(*args, **kwargs)
    elif not path.exists():
        path.mkdir(*args, **kwargs)
    logger.debug(f"Ensured directory: {path}")


def check_cp_command(command: list[str]):
    if command[0] == "cp":
        source_file = Path(command[1])
        destination_dir = os.path.basename(Path(command[2]))

        if not os.path.exists(source_file):
            logger.error(f"Source file does not exist: {source_file}")
            sys.exit(1)

        if not os.path.exists(destination_dir):
            logger.error(f"Destination directory does not exist: {destination_dir}")
            sys.exit(1)


def run_command(command: list[str], 
                dry_run=False,
                run_anyway=False, 
                background=False,
                cwd: Path | str = "."):
    if dry_run:
        logger.info(f'{" ".join(map(str,command))} (cwd: {cwd or "."})')
    if (not dry_run) or run_anyway:
        logger.info(f'Running: {" ".join(map(str,command))} (cwd: {cwd or "."})')


        try:
            if background:
                subprocess.Popen(command, cwd=cwd)
            else:
                subprocess.run(command, cwd=cwd, check=True)
        except subprocess.CalledProcessError as e:
            check_cp_command(command)
            logger.error(f"Error running command: {e}")
            subprocess.run(["pwd"], cwd=cwd, check=True)
            sys.exit(1)
        except:
            subprocess.run(["pwd"], cwd=cwd, check=True)
            subprocess.run(["python", "./test.py"], cwd=cwd, check=True)
            sys.exit(1)


def setup_data_folder(data_dir=None):
    if data_dir is not None:
        # path is not absolute, assume is is relative
        if not Path(data_dir).is_absolute():
            folder = Path(f"./{data_dir}")

        else:
            folder = Path(data_dir)
        if not folder.exists():
            folder.mkdir(exist_ok=False)
    else:
        today = datetime.now().strftime("%b_%d").lower()
        folder = Path(f"./{today}")
        # ensure that the folder does not already exist
        aToZ = (letter for letter in "abcdefghijklmnopqrstuvwxyz")
        while folder.exists():
            folder = Path(f"./{today}{next(aToZ)}")
        if not folder.exists():
            folder.mkdir(exist_ok=False)
    logger.debug(f"Data folder set up: {folder}")
    return folder



def create_download_list(granule_urls: list[str], download_list: Path, data_dir: Path):
    # Create a list of files to download
    with open(download_list, "w") as f:
        for url in granule_urls[:]:
            filename = url.split("/")[-1]
            exists = os.path.exists(f"{data_dir}/{filename}") or os.path.exists(
                f"{data_dir}/subsetted_netcdf/{filename}"
            )
            if exists:
                logger.info(f"Skipping {filename}, already in {data_dir}")
                continue
            f.write(url + "\n")
    logger.debug(f"Download list created: {download_list}")

def download_data(download_script_template, download_script, dry_run = False):
    # check if a .netrc file is on the path
    netrc = Path("~/.netrc").expanduser()
    if not netrc.exists():
        logger.error("No .netrc file found in home directory.")
        logger.error("Please create a .netrc file with your Earthdata login credentials.")
        logger.error("See https://urs.earthdata.nasa.gov/documentation/for_users/data_access/curl_and_wget")
        sys.exit(1)
    run_command(['cp', str(download_script_template), str(download_script)], dry_run = dry_run)
    run_command(['sh', str(download_script.name)], cwd=download_script.parent, dry_run = dry_run)

# def download_data(download_list: Path, template: Path, download_dir: Path, dry_run = False):
#     run_command(
#         ["cp", str(template), str(download_dir)],
#         dry_run=dry_run,
#     )
#     run_command(
#         ["sh", str(download_dir.name)],
#         cwd=download_dir.parent,
#         dry_run=dry_run,
#     )

def fetch_granule_data(start_date, end_date, folder: Path, download_list: Path, download_script_template: Path, download_script: Path, skip_download = False, verbose = False, dry_run = False, only_one_file = False):
    if not skip_download:
    # Determine the date range for the data download
        if start_date and end_date:
            try:
                start_date = to_datetime(start_date, "%Y-%m-%d")
                end_date = to_datetime(end_date, "%Y-%m-%d")
                last_downloaded_time = None
            except ValueError:
                logger.error("Date format should be YYYY-MM-DD")
                sys.exit(1)
            
        else:
            start_date, end_date, last_downloaded_time = get_date_limits()
        granule_urls = search_for_granules(
        TEMPO_CONCEPT_ID,
        start_date,
        end_date,
        last_downloaded_time,
        verbose,
        dry_run=dry_run,
    )

        if len(granule_urls) == 0:
            logger.info("No new data found")
            exit(0)

        if only_one_file:
            granule_urls = granule_urls[:1]

        create_download_list(granule_urls, download_list, folder)

        if dry_run and not skip_download:
            logger.info(" ==== Download List  ==== ")
            with open(download_list, "r") as f:
                logger.info(f.read())
        
        download_data(download_script_template, download_script, dry_run = dry_run)
        # download_data(download_list = download_list, template = download_script_template, download_dir = folder, dry_run=dry_run)
