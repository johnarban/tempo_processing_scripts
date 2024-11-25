import numpy as np
import requests
import datetime as dt
from datetime import datetime, timezone
import os
from pathlib import Path
import argparse
from dateutil import parser as date_parser

# Set up argument parser
parser = argparse.ArgumentParser(description='Download new TEMPO data')
parser.add_argument('--skip-download', action='store_true', help='Skip the download step')
parser.add_argument('--data-dir', type=str, help='The directory to search for new data')
parser.add_argument('--dry-run', action='store_true', help='Print the commands that would be run, but do not run them')
parser.add_argument('--date', type=str, help='The specific date for which to download data (format: YYYY-MM-DD)')

args = parser.parse_args()
skip_download = args.skip_download
data_dir = args.data_dir or 'manual_data'
specific_date = args.date

if skip_download and not data_dir:
    parser.error("--skip-download requires --data-dir")
    import sys
    sys.exit(1)

if specific_date:
    try:
        try:
            target_date = datetime.strptime(specific_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        except ValueError:
            target_date = date_parser.parse(specific_date).replace(tzinfo=timezone.utc)
        start_date = target_date
        end_date = target_date + dt.timedelta(days=1)
    except ValueError:
        parser.error("Date format should be YYYY-MM-DD")
        import sys
        sys.exit(1)
else:
    parser.error("Date format should be YYYY-MM-DD")
    # examplec command with date anddirectory
    # python get_tempo_data_for_day.py --date 2021-07-01 --data-dir 2021-07-01
    parser.error("Example > python get_tempo_data_for_day.py --date=2021-07-01 --data-dir=2021-07-01")
    import sys
    sys.exit(1)
    # today = datetime.now(tz=timezone.utc)
    # start_date = today.replace(hour=0, minute=0, second=0, microsecond=0)
    # end_date = start_date + dt.timedelta(days=1)

if data_dir:
    printfolder = Path(f'./{data_dir}')

if not skip_download:
    # Define the search URL
    granule_search_url = 'https://search.earthdata.nasa.gov/search/granules?p=C2930763263-LARC_CLOUD'
    concept_id = 'C2930763263-LARC_CLOUD' # TEMPO NO2 V03 L# Data
    dt_format = '%Y-%m-%dT%H:%M:%SZ' # format requirement for datetime search

    temporal_str = start_date.strftime(dt_format) + ',' + end_date.strftime(dt_format)
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

    cmr_response = requests.get(cmr_url, params=search_params, headers=headers)
    granules = cmr_response.json()['feed']['entry']
    granule_urls = []

    for granule in granules:
        item = next((item['href'] for item in granule['links'] if "asdc-prod-protected" in item["href"]), None)
        if item:
            granule_urls.append(item)
        
    print(f"Found {len(granule_urls)} new granules")

    if len(granule_urls) == 0:
        print("No new data found")
        exit(0)

folder = Path(f'./{data_dir}')
folder.mkdir(exist_ok=True)

if not skip_download:
    with open(folder / 'download_list.txt', 'w') as f:
        for url in granule_urls[:]:
            filename = url.split('/')[-1]
            if not os.path.exists(f'{data_dir}/{filename}'):
                f.write(url + '\n')
            else:
                print(f"Skipping {filename}, already in {data_dir}")

if args.dry_run:
    print("Dry run")

if args.dry_run and not skip_download:
    print(" ==== Download List  ==== ")
    with open(folder / 'download_list.txt', 'r') as f:
        print(f.read())

if not skip_download:
    print(f'cp download_template.sh {folder}/download_template.sh')
    if not args.dry_run:
        os.system(f'cp download_template.sh {folder}/download_template.sh')

    print(f'cd {folder} && sh ./download_template.sh')
    if not args.dry_run:
        os.system(f'cd {folder} && sh ./download_template.sh')

print(f'./process_data.py -d ./{folder.name} -o ./{folder.name}/images -p "*.nc"')
if not args.dry_run:
    os.system(f'./process_data.py -d ./{folder.name} -o ./{folder.name}/images -p "*.nc"')

print(f'cp compress_and_diff.sh ./{folder.name}/images')
if not args.dry_run:
    os.system(f'cp compress_and_diff.sh ./{folder.name}/images')

print(f'cp compress_and_diff.sh ./{folder.name}/images/resized_images')
if not args.dry_run:
    os.system(f'cp compress_and_diff.sh ./{folder.name}/images/resized_images')

print(f'cd ./{folder.name}/images && sh compress_and_diff.sh')
if not args.dry_run:
    os.system(f'cd ./{folder.name}/images && sh compress_and_diff.sh')

print(f'cd ./{folder.name}/images/resized_images && sh compress_and_diff.sh')
if not args.dry_run:
    os.system(f'cd ./{folder.name}/images/resized_images && sh compress_and_diff.sh')

# print(f'sh subset_files.sh {folder.name}')
# if not args.dry_run:
#     os.system(f'sh subset_files.sh {folder.name}')

# print(f'sh merge.sh {folder.name}')
# if not args.dry_run:
#     os.system(f'sh merge.sh {folder.name}')