import numpy as np
import requests
from urllib.parse import unquote
import datetime as dt
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import sys

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

def run_command(command, dry_run=False, use_subprocess=False):
    if dry_run:
        if use_subprocess:
            print('DRY RUN: ', ' '.join(command))
        else:
            print('DRY RUN: ', command)
    else:
        if use_subprocess:
            # print(' '.join(command)) get the command as a string
            print(subprocess.list2cmdline(command))
            subprocess.run(command)
        else:
            print(command)
            os.system(command)

def setup_data_folder(data_dir = None):
    if data_dir:
        folder = Path(f'./{data_dir}')
        folder.mkdir(exist_ok=True)
    else:
        today = datetime.now().strftime("%b_%d").lower()
        folder = Path(f'./{today}')
        # ensure that the folder does not already exist
        aToZ = (letter for letter in 'abcdefghijklmnopqrstuvwxyz')
        while folder.exists():
            folder = Path(f'./{today}{next(aToZ)}')
        folder.mkdir(exist_ok=False)
    return folder

def create_download_list(granule_urls, download_list, data_dir):
    # Create a list of files to download
    with open(download_list, 'w') as f:
        for url in granule_urls[:]:
            filename = url.split('/')[-1]
            exists = os.path.exists(f'{data_dir}/{filename}') or os.path.exists(f'{data_dir}/subsetted_netcdf/{filename}')
            if exists:
                print(f"Skipping {filename}, already in {data_dir}")
                continue
            f.write(url + '\n')

def download_data(download_list, data_dir):
    run_command(f'cp {download_script_template} {download_script}', args.dry_run) 
    run_command(f'cd {download_script.parent} && sh {download_script.name}', args.dry_run)




# command line arguments
import argparse
# get arguments
parser = argparse.ArgumentParser(description='Download new TEMPO data')
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

args = parser.parse_args()



skip_download = args.skip_download or args.merge_only
data_dir = args.data_dir

if skip_download and not data_dir:
    parser.error("--skip-download requires --data-dir. Exiting...")
    sys.exit(1)
    
if args.dry_run:
    print("Dry run")
    
from get_tempo_data_utils import get_date_limits, search_for_granules



run_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
folder = setup_data_folder(data_dir)

# Setup output paths
download_list = folder / 'download_list.txt'
download_script_template = Path('download_template.sh')
download_script = folder / 'download_template.sh'

image_directory = Path(f'./{folder.name}/images')
resized_image_directory = Path(f'./{folder.name}/images/resized_images')
ensure_directory(image_directory, args.dry_run)
ensure_directory(resized_image_directory, args.dry_run)

cloud_image_directory = Path(f'./{folder.name}/cloud_images')
resized_cloud_image_directory = Path(f'./{folder.name}/cloud_images/resized_images')
ensure_directory(cloud_image_directory, args.dry_run)
ensure_directory(resized_cloud_image_directory, args.dry_run)

merge_directory = Path(args.merge_dir)
image_merge_directory = merge_directory / 'released' / 'images'
cloud_merge_directory = merge_directory / 'clouds' / 'images'


if not skip_download:
    start_date, end_date = get_date_limits()
    granule_urls = search_for_granules('C2930763263-LARC_CLOUD', start_date, end_date, args.verbose, dry_run=args.dry_run)
    
    
    if len(granule_urls) == 0:
        print("No new data found")
        exit(0)
    
    if args.one_file:
        granule_urls = granule_urls[:1] 

    create_download_list(granule_urls, download_list, folder)

    if args.dry_run and not skip_download:
        print(" ==== Download List  ==== ")
        with open(download_list, 'r') as f:
            print(f.read())


    download_data(download_list, folder)
    

# Check that the data directory is not empty
nc_files = list(folder.glob('*.nc'))
subset_nc_files = list(folder.glob('subsetted_netcdf/*.nc'))
if not args.dry_run and not nc_files and (not args.use_subset or not subset_nc_files):
    print("No new data downloaded")
    exit(0)


if not args.merge_only:
    process_args = ['-d', f'./{folder.name}/subsetted_netcdf' if args.use_subset else f'./{folder.name}', 
                    '-o', f'./{folder.name}/images', 
                    '--cloud-dir', f'./{folder.name}/cloud_images',
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
    process_args += ['--name', folder.name] if args.name is None else ['--name', args.name]
    run_command(['./process_data.py'] + process_args, use_subprocess=True, dry_run=False)



# Compress & Merge NO2 Data
if not args.text_files_only:
    run_command(f'cp compress_and_diff.sh {image_directory}', args.dry_run)
    run_command(f'cp compress_and_diff.sh {resized_image_directory}', args.dry_run)

    run_command(f'cd {image_directory} && sh compress_and_diff.sh', args.dry_run)
    run_command(f'cd {resized_image_directory} && sh compress_and_diff.sh', args.dry_run)

# run_command(f'sh merge.sh {folder.name}', args.dry_run)
run_command(f"sh merge.sh -s {image_directory}/ -d {image_merge_directory}")

# Compress & Merge Cloud Data
if not args.skip_clouds:
    if not args.text_files_only:
        run_command(f'cp compress_and_diff.sh {cloud_image_directory}', args.dry_run)
        run_command(f'cd {cloud_image_directory} && sh compress_and_diff.sh', args.dry_run)

        run_command(f'cp compress_and_diff.sh {resized_cloud_image_directory}', args.dry_run)
        run_command(f'cd {resized_cloud_image_directory} && sh compress_and_diff.sh', args.dry_run)

    # run_command(f"sh merge_clouds.sh {folder.name}", args.dry_run)
    run_command(f"sh merge.sh -s {cloud_image_directory}/ -d {cloud_merge_directory} {'-t' if args.dry_run else ''} {'-x' if args.delete_after_merge else ''}")

# subset the data
if (not args.skip_subset) and (not args.use_subset) and (not args.text_files_only):
    run_command(f'sh subset_files.sh {folder.name}', args.dry_run)

if args.dry_run:
    import shutil
    if Path(folder).exists():
        shutil.rmtree(folder)
        print(f"Removed output directory: {folder}")


# ===============================================
