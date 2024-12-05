import datetime as dt
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
import numpy as np
import yaml
from get_tempo_data_utils import (
                                  ensure_directory, 
                                  run_command, 
                                  setup_data_folder, fetch_granule_data, 
                                  setup_logging, validate_directory_exists
                                  )

VERBOSE = True


# command line arguments
import argparse

# get arguments
parser = argparse.ArgumentParser(description="Download new TEMPO data")
parser.add_argument(
    "--config",
    type=str,
    help="Path to the YAML configuration file",
    default="default_config.yaml",
)
# root directory
parser.add_argument("--root-dir", type=str, help="Root directory for all paths")
# --skip-download
parser.add_argument("--skip-download", action="store_true", help="Skip the download step")
# if there is a --skip-download there should be a --data-dir
parser.add_argument("--data-dir", type=str, help="The directory to search for new data")
#  dry-run
parser.add_argument(
    "--dry-run",
    action="store_true",
    help="Print the commands that would be run, but do not run them",
)
# verbose
parser.add_argument(
    "--verbose",
    action="store_true",
    help="Print the commands that would be run, but do not run them",
)
# skip subset
parser.add_argument("--skip-subset", action="store_true", help="Skip the subset step")
# use subsetted data
parser.add_argument("--use-subset", action="store_true", help="Use subsetted data")
# skip clouds
parser.add_argument("--skip-clouds", action="store_true", help="Skip the clouds step")
# don't reproject
parser.add_argument("--no-reproject", action="store_true", help="Do not reproject the images")
# only get 1 file
parser.add_argument("--one-file", action="store_true", help="Only get one file")
# resampling-methd
parser.add_argument("--reprojection-method", type=str, help="reprojection method", default="average")
# text files only
parser.add_argument("--text-files-only", action="store_true", help="Only process text files")
# name of the data directory
parser.add_argument("--name", type=str, help="Name of the data directory", default=None)
# set the merge directory
parser.add_argument(
    "--merge-dir",
    type=str,
    help="Top level directory to place images in",
    default="~/github/tempo-data-holdings",
)
# add merge only options
parser.add_argument("--merge-only", action="store_true", help="set this to only perform file merges")
# skip merge options
parser.add_argument("--skip-merge", action="store_true", help="skip merging to production directory")
# add delete after merge option
parser.add_argument(
    "--delete-after-merge",
    action="store_true",
    help="Delete images in orginal directory afer merge",
)
# add output name option
parser.add_argument(
    "--output-dir",
    type=str,
    help="Output name for images and text files",
    default=None,
)
# add date range options
parser.add_argument(
    "--start-date",
    type=str,
    help="Start date for the data download (format: YYYY-MM-DD)",
)
parser.add_argument("--end-date", type=str, help="End date for the data download (format: YYYY-MM-DD)")
# use the input file name
parser.add_argument(
    "--use-input-filename",
    action="store_true",
    help="Use the same name format as the input TEMPO files",
)
parser.add_argument("--no-output", action="store_true", help="Do not output images or text files")
# skip compress and diff
parser.add_argument("--skip-compress", action="store_true", help="Skip the compress")
parser.add_argument("--skip-process", action="store_true", help="Skip the data processing (image creation) step")

args = parser.parse_args()

# Load configuration from YAML file
with open(args.config, "r") as file:
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
def make_absolute(path, root_dir=Path('./')):
    path = Path(path).expanduser()
    return root_dir / path if not Path(path).is_absolute() else Path(path)


from typing import cast

args.data_dir = make_absolute(args.data_dir, root_dir) if args.data_dir else None
args.merge_dir = make_absolute(args.merge_dir, root_dir)
args.output_dir = make_absolute(args.output_dir, root_dir) if args.output_dir else None

# Print out the directories
# logging.info(f"Data directory: {args.data_dir}")
# logging.info(f"Merge directory: {args.merge_dir}")
# logging.info(f"Output name: {args.output_dir}")
# Create log summart of the what this script will do
#
logging.info("Log Summary:")
logging.info(f"Root directory: {root_dir}")
logging.info(f"Data directory: {args.data_dir}")
logging.info(f"Merge directory: {args.merge_dir}")
logging.info(f"Output directory: {args.output_dir}")
logging.info(f"Skip subset: {args.skip_subset}")
logging.info(f"Name: {args.name}")
# Print out the directories


skip_download = args.skip_download or args.merge_only
data_dir = args.data_dir

if skip_download and not data_dir:
    parser.error("--skip-download requires --data-dir. Exiting...")
    sys.exit(1)

if args.dry_run:
    logging.info("Dry run")


run_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
netcdf_data_location = setup_data_folder(data_dir)

# Setup input/output directories
output_dir = args.output_dir if args.output_dir else netcdf_data_location.name
if not Path(output_dir).exists():
    ensure_directory(Path(output_dir), parents=True, exist_ok=False)

download_list = netcdf_data_location / "download_list.txt"
download_script_template = Path("download_template.sh")
download_script = netcdf_data_location / "download_template.sh"
# check that these files exist and are files



image_directory = make_absolute(f"{output_dir}/images", root_dir)
resized_image_directory = make_absolute(f"{output_dir}/images/resized_images", root_dir)
ensure_directory(image_directory, args.dry_run)
ensure_directory(resized_image_directory, args.dry_run)

cloud_image_directory = make_absolute(f"{output_dir}/cloud_images", root_dir)
resized_cloud_image_directory = make_absolute(f"{output_dir}/cloud_images/resized_images", root_dir)
ensure_directory(cloud_image_directory, args.dry_run)
ensure_directory(resized_cloud_image_directory, args.dry_run)

merge_directory = args.merge_dir
image_merge_directory = merge_directory / "released" / "images"
cloud_merge_directory = merge_directory / "clouds" / "images"

logging.info(f"Output directory: {output_dir}")
logging.info(f"Image directory: {image_directory}")
logging.info(f"Resized image directory: {resized_image_directory}")
logging.info(f"Cloud image directory: {cloud_image_directory}")
logging.info(f"Resized cloud image directory: {resized_cloud_image_directory}")
logging.info(f"Merge directory: {merge_directory}")
logging.info(f"Image merge directory: {image_merge_directory}")
logging.info(f"Cloud merge directory: {cloud_merge_directory}")

# make sure all the directories exist
directories = [
    netcdf_data_location,
    image_directory,
    resized_image_directory,
    cloud_image_directory,
    resized_cloud_image_directory,
    merge_directory,
    image_merge_directory,
    cloud_merge_directory,
]
validate_directory_exists(directories)

fetch_granule_data(
    args.start_date,
    args.end_date,
    netcdf_data_location,
    download_list,
    download_script_template,
    download_script,
    skip_download,
    args.verbose,
    args.dry_run,
    args.one_file,
)
validate_directory_exists([download_list, download_script])

# Check that the data directory is not empty
nc_files = list(netcdf_data_location.glob("*.nc"))
subset_nc_files = list(netcdf_data_location.glob("subsetted_netcdf/*.nc"))
doesnt_need_data = (
    args.merge_only or args.text_files_only or args.use_subset or args.dry_run
)
if (
    not doesnt_need_data
    and not nc_files
    and (not args.use_subset or not subset_nc_files)
):
    logging.info("No new data downloaded")
    exit(0)
if (len(nc_files) > 0) or (len(subset_nc_files) > 0):
    if args.use_subset:
        logging.info(f"Using subsetted data: {len(subset_nc_files)} files")
    else:
        logging.info(f"Using {len(nc_files)} files")


if not args.merge_only and not args.skip_process:
    process_args = [
        "-d",
        str(netcdf_data_location / "subsetted_netcdf") if args.use_subset else str(netcdf_data_location),
        "-o",
        str(image_directory),
        "--cloud-dir",
        str(cloud_image_directory),
        "-p",
        "*.nc",
    ]
    # add --do-clouds if --skip-clouds is not set
    process_args += ["--do-clouds"] if not args.skip_clouds else []
    # add --dry-run if --dry-run is set
    process_args += ["--dry-run"] if args.dry_run else []
    # add --no-reproject if --no-reproject is set
    process_args += ["--no-reproject"] if args.no_reproject else []
    # add --method if --reprojection-method is set
    process_args += (["--method", args.reprojection_method] if args.reprojection_method else [])
    # add --text-files-only if --text-files-only is set
    process_args += ["--text-files-only"] if args.text_files_only else []
    # set name to folder.name if name is not set
    process_args += (["--name", output_dir] if args.name is None else ["--name", args.name])
    process_args += ["--debug"] if (args.verbose or args.dry_run) else []
    process_args += ["--use-input-filename"] if args.use_input_filename else []
    process_args += ["--no-output"] if args.no_output else []
    run_command(["./process_data.py"] + process_args, dry_run=args.dry_run, run_anyway=True)


# Compress & Merge NO2 Data
if not args.text_files_only and not args.no_output:
    run_command(["cp", "compress_and_diff.sh", str(image_directory)], args.dry_run)
    run_command(
        ["cp", "compress_and_diff.sh", str(resized_image_directory)], args.dry_run
    )
    
    if not args.skip_compress:
        run_command(
            ["sh", "compress_and_diff.sh"], cwd=image_directory, dry_run=args.dry_run
        )
        run_command(
            ["sh", "compress_and_diff.sh"],
            cwd=resized_image_directory,
            dry_run=args.dry_run,
        )


if not args.skip_merge:
    # run_command(f'sh merge.sh {folder.name}', args.dry_run)
    run_command(
        [
            "sh",
            "merge.sh",
            "-s",
            str(image_directory) + "/",
            "-d",
            str(image_merge_directory),
        ],
        dry_run=args.dry_run,
    )
else:
    logging.info("Skipping merge")

# Compress & Merge Cloud Data
if not args.skip_clouds:
    if not args.text_files_only and not args.no_output:
        run_command(
            ["cp", "compress_and_diff.sh", str(cloud_image_directory)], args.dry_run
        )
        if not args.skip_compress:
            run_command(
                ["sh", "compress_and_diff.sh"],
                cwd=cloud_image_directory,
                dry_run=args.dry_run,
            )

        run_command(
            ["cp", "compress_and_diff.sh", str(resized_cloud_image_directory)],
            args.dry_run,
        )
        if not args.skip_compress:
            run_command(
                ["sh", "compress_and_diff.sh"],
                cwd=resized_cloud_image_directory,
                dry_run=args.dry_run,
            )


    if not args.skip_merge:
        # run_command(f"sh merge_clouds.sh {folder.name}", args.dry_run)
        run_command(
            [
                "sh",
                "merge.sh",
                "-s",
                str(cloud_image_directory) + "/",
                "-d",
                str(cloud_merge_directory),
                "-t" if args.dry_run else "",
                "-x" if args.delete_after_merge else "",
            ],
            dry_run=args.dry_run,
        )

# subset the data
if (not args.skip_subset) and (not args.use_subset) and (not args.text_files_only):
    run_command(["sh", "subset_files.sh", netcdf_data_location.name], args.dry_run)

if args.dry_run:
    import shutil

    if Path(netcdf_data_location).exists():
        # shutil.rmtree(folder)
        logging.info(f"Removed output directory: {netcdf_data_location}")


# ===============================================
