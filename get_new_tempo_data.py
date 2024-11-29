import numpy as np
import requests
from urllib.parse import unquote
import datetime as dt
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import sys
import logging
import yaml

def setup_logging(debug: bool) -> None:
    """
    Set up logging configuration.
    """
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format='%(asctime)s - %(levelname)s - %(message)s')

def ensure_directory(path: Path | str, skip = False):
    if skip:
        return
    # check if path is a file, if so delete and create directory
    # if directory exists, do nothing
    # if directory does not exist, create it
    path = Path(path)
    if path.is_file():
        path.unlink()
        path.mkdir()
    elif not path.exists():
        path.mkdir()
    logging.debug(f"Ensured directory: {path}")

def check_cp_command(command):
    if command[0] == 'cp':
        source_file = command[1]
        destination_dir = os.path.dirname(command[2])
        
        if not os.path.exists(source_file):
            logging.error(f"Source file does not exist: {source_file}")
            sys.exit(1)
        
        if not os.path.exists(destination_dir):
            logging.error(f"Destination directory does not exist: {destination_dir}")
            sys.exit(1)

def run_command(command, dry_run=False, run_anyway=False, cwd: Path | str ='.'):
    if dry_run:
        logging.info(f'DRY RUN: {" ".join(command)} (cwd: {cwd or "."})')
    if (not dry_run) or run_anyway:
        logging.info(f'Running: {" ".join(command)} (cwd: {cwd or "."})')
        
        check_cp_command(command)
        
        try:
            subprocess.run(command, cwd=cwd, check=True)
        except subprocess.CalledProcessError as e:
            logging.error(f"Error running command: {e}")
            subprocess.run(['pwd'], cwd=cwd, check=True)
            sys.exit(1)

def setup_data_folder(data_dir = None):
    if data_dir:
        # path is not absolute, assume is is relative
        if not Path(data_dir).is_absolute():
            folder = Path(f'./{data_dir}')

        else:
            folder = Path(data_dir)
        folder.mkdir(exist_ok=True)
    else:
        today = datetime.now().strftime("%b_%d").lower()
        folder = Path(f'./{today}')
        # ensure that the folder does not already exist
        aToZ = (letter for letter in 'abcdefghijklmnopqrstuvwxyz')
        while folder.exists():
            folder = Path(f'./{today}{next(aToZ)}')
        folder.mkdir(exist_ok=False)
    logging.debug(f"Data folder set up: {folder}")
    return folder

def create_download_list(granule_urls, download_list, data_dir):
    # Create a list of files to download
    with open(download_list, 'w') as f:
        for url in granule_urls[:]:
            filename = url.split('/')[-1]
            exists = os.path.exists(f'{data_dir}/{filename}') or os.path.exists(f'{data_dir}/subsetted_netcdf/{filename}')
            if exists:
                logging.info(f"Skipping {filename}, already in {data_dir}")
                continue
            f.write(url + '\n')
    logging.debug(f"Download list created: {download_list}")

def download_data(download_list, data_dir):
    run_command(['cp', str(download_script_template), str(download_script)], dry_run = args.dry_run)
    run_command(['sh', str(download_script.name)], cwd=download_script.parent, dry_run = args.dry_run)




# command line arguments
import argparse
# get arguments
parser = argparse.ArgumentParser(description='Download new TEMPO data')
parser.add_argument('--config', type=str, help='Path to the YAML configuration file', default='default_config.yaml')
# root directory
parser.add_argument('--root-dir', type=str, help='Root directory for all paths')
# --skip-download
parser.add_argument('--skip-download', action='store_true', help='Skip the download step')
# if there is a --skip-download there should be a --data-dir
parser.add_argument('--data-dir', type=str, help='The directory to search for new data')
#  dry-run
parser.add_argument('--dry-run', action='store_true', help='Print the commands that would be run, but do not run them')
# verbose
parser.add_argument('--verbose', action='store_true', help='Print the commands that would be run, but do not run them')
# skip subset
parser.add_argument('--skip-subset', action='store_true', help='Skip the subset step')
# use subsetted data
parser.add_argument('--use-subset', action='store_true', help='Use subsetted data')
# skip clouds
parser.add_argument('--skip-clouds', action='store_true', help='Skip the clouds step')
# don't reproject
parser.add_argument('--no-reproject', action='store_true', help='Do not reproject the images')
# only get 1 file
parser.add_argument('--one-file', action='store_true', help='Only get one file')
# resampling-methd
parser.add_argument('--reprojection-method', type=str, help='reprojection method', default='average')
# text files only
parser.add_argument('--text-files-only', action='store_true', help='Only process text files')
# name of the data directory
parser.add_argument('--name', type=str, help='Name of the data directory', default=None)
# set the merge directory
parser.add_argument('--merge-dir', type=str, help="Top level directory to place images in", default = "~/github/tempo-data-holdings" )
# add merge only options
parser.add_argument('--merge-only', action="store_true", help="set this to only perform file merges")
# add delete after merge option
parser.add_argument('--delete-after-merge', action="store_true", help="Delete images in orginal directory afer merge")
# add output name option
parser.add_argument('--output-name', type=str, help='Output name for images and text files', default=None)
# add date range options
parser.add_argument('--start-date', type=str, help='Start date for the data download (format: YYYY-MM-DD)')
parser.add_argument('--end-date', type=str, help='End date for the data download (format: YYYY-MM-DD)')

args = parser.parse_args()

# Load configuration from YAML file
with open(args.config, 'r') as file:
    config = yaml.safe_load(file)

# Override YAML config with command-line arguments if provided
for key, value in config.items():
    if getattr(args, key, None) is None:
        setattr(args, key, value)

setup_logging(args.verbose)

# Print out the final configuration
if args.verbose:
    print("Configuration:")
    for key, value in vars(args).items():
        print(f"{key}: {value}")

# Ensure root directory is absolute
root_dir = Path(args.root_dir).resolve()
print(f"root_dir: {root_dir}")

# Ensure all paths are absolute
def make_absolute(path):
    path = Path(path).expanduser()
    return root_dir / path if not Path(path).is_absolute() else Path(path)

from typing import cast
args.data_dir = make_absolute(args.data_dir) if args.data_dir else None
args.merge_dir = make_absolute(args.merge_dir)
args.output_name = make_absolute(args.output_name) if args.output_name else None

# Print out the directories
print(f"Data directory: {args.data_dir}")
print(f"Merge directory: {args.merge_dir}")
print(f"Output name: {args.output_name}")
# Print out the directories


skip_download = args.skip_download or args.merge_only
data_dir = args.data_dir

if skip_download and not data_dir:
    parser.error("--skip-download requires --data-dir. Exiting...")
    sys.exit(1)
    
if args.dry_run:
    logging.info("Dry run")
    
from get_tempo_data_utils import get_date_limits, search_for_granules



run_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
folder = setup_data_folder(data_dir)

# Setup output paths
output_name = args.output_name if args.output_name else folder.name
download_list = folder / 'download_list.txt'
download_script_template = Path('download_template.sh')
download_script = folder / 'download_template.sh'

image_directory = make_absolute(f'{output_name}/images')
resized_image_directory = make_absolute(f'{output_name}/images/resized_images')
ensure_directory(image_directory, args.dry_run)
ensure_directory(resized_image_directory, args.dry_run)

cloud_image_directory = make_absolute(f'{output_name}/cloud_images')
resized_cloud_image_directory = make_absolute(f'{output_name}/cloud_images/resized_images')
ensure_directory(cloud_image_directory, args.dry_run)
ensure_directory(resized_cloud_image_directory, args.dry_run)

merge_directory = args.merge_dir
image_merge_directory = merge_directory / 'released' / 'images'
cloud_merge_directory = merge_directory / 'clouds' / 'images'


if not skip_download:
    # Determine the date range for the data download
    if args.start_date and args.end_date:
        try:
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc) # + dt.timedelta(days=1)
            last_time = None
        except ValueError:
            parser.error("Date format should be YYYY-MM-DD")
            sys.exit(1)
    else:
        start_date, end_date, last_downloaded_time = get_date_limits()
    granule_urls = search_for_granules('C2930763263-LARC_CLOUD', start_date, end_date, last_downloaded_time, args.verbose, dry_run=args.dry_run)
    
    
    if len(granule_urls) == 0:
        logging.info("No new data found")
        exit(0)
    
    if args.one_file:
        granule_urls = granule_urls[:1] 

    create_download_list(granule_urls, download_list, folder)

    if args.dry_run and not skip_download:
        logging.info(" ==== Download List  ==== ")
        with open(download_list, 'r') as f:
            logging.info(f.read())


    download_data(download_list, folder)
    

# Check that the data directory is not empty
nc_files = list(folder.glob('*.nc'))
subset_nc_files = list(folder.glob('subsetted_netcdf/*.nc'))
doesnt_need_data = args.merge_only or args.text_files_only or args.use_subset or args.dry_run
if not doesnt_need_data and not nc_files and (not args.use_subset or not subset_nc_files):
    logging.info("No new data downloaded")
    exit(0)


if not args.merge_only:
    process_args = ['-d', str(folder / 'subsetted_netcdf') if args.use_subset else str(folder), 
                    '-o', str(image_directory), 
                    '--cloud-dir', str(cloud_image_directory),
                    '-p', '*.nc']
    # add --do-clouds if --skip-clouds is not set
    process_args += ['--do-clouds'] if not args.skip_clouds else []
    # add --dry-run if --dry-run is set
    process_args += ['--dry-run'] if args.dry_run else []
    # add --no-reproject if --no-reproject is set
    process_args += ['--no-reproject'] if args.no_reproject else []
    # add --method if --reprojection-method is set
    process_args += ['--method', args.reprojection_method] if args.reprojection_method else []
    # add --text-files-only if --text-files-only is set
    process_args += ['--text-files-only'] if args.text_files_only else []
    # set name to folder.name if name is not set
    process_args += ['--name', output_name] if args.name is None else ['--name', args.name]
    process_args += ['--debug'] if (args.verbose or args.dry_run) else []
    run_command(['./process_data.py'] + process_args, dry_run=args.dry_run, run_anyway=True)



# Compress & Merge NO2 Data
if not args.text_files_only:
    run_command(['cp', 'compress_and_diff.sh', str(image_directory)], args.dry_run)
    run_command(['cp', 'compress_and_diff.sh', str(resized_image_directory)], args.dry_run)

    run_command(['sh', 'compress_and_diff.sh'], cwd=image_directory, dry_run=args.dry_run)
    run_command(['sh', 'compress_and_diff.sh'], cwd=resized_image_directory, dry_run=args.dry_run)

# run_command(f'sh merge.sh {folder.name}', args.dry_run)
run_command(['sh', 'merge.sh', '-s', str(image_directory) +'/', '-d', str(image_merge_directory)], dry_run=args.dry_run)

# Compress & Merge Cloud Data
if not args.skip_clouds:
    if not args.text_files_only:
        run_command(['cp', 'compress_and_diff.sh', str(cloud_image_directory)], args.dry_run)
        run_command(['sh', 'compress_and_diff.sh'], cwd=cloud_image_directory, dry_run=args.dry_run)

        run_command(['cp', 'compress_and_diff.sh', str(resized_cloud_image_directory)], args.dry_run)
        run_command(['sh', 'compress_and_diff.sh'], cwd=resized_cloud_image_directory, dry_run=args.dry_run)

    # run_command(f"sh merge_clouds.sh {folder.name}", args.dry_run)
    run_command(['sh', 'merge.sh', '-s', str(cloud_image_directory)+'/', '-d', str(cloud_merge_directory), '-t' if args.dry_run else '', '-x' if args.delete_after_merge else ''], dry_run=args.dry_run)

# subset the data
if (not args.skip_subset) and (not args.use_subset) and (not args.text_files_only):
    run_command(['sh', 'subset_files.sh', folder.name], args.dry_run)

if args.dry_run:
    import shutil
    if Path(folder).exists():
        shutil.rmtree(folder)
        logging.info(f"Removed output directory: {folder}")


# ===============================================
