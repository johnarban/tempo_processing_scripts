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
    setup_data_folder, 
    make_absolute,
    fetch_granule_data, 
    validate_directory_exists,
    escape_spaces
)
from typing import cast
import argparse
from logger import setup_logging, set_log_level
logger = setup_logging(name = 'main')

def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.
    """
    parser = argparse.ArgumentParser(description="Download new TEMPO data")
    parser.add_argument("--config", type=str, help="Path to the YAML configuration file", default="default_config.yaml")
    parser.add_argument("--root-dir", type=str, help="Root directory for all data output paths")
    parser.add_argument("--data-dir", type=str, help="The directory to search for new data")
    parser.add_argument("--merge-dir", type=str, help="Top level directory to place images in", default=None)
    parser.add_argument("--output-dir", type=str, help="Output directory for images and text files", default=None)
    
    parser.add_argument("--start-date", type=str, help="[Optional] Start date for the data download (format: YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="[Optional] End date for the data download (format: YYYY-MM-DD)")
    
    parser.add_argument("--skip-download", action="store_true", help="Skip the download step", default = None)
    parser.add_argument("--skip-subset", action="store_true", help="Skip the subset step")
    parser.add_argument("--skip-clouds", action="store_true", help="Skip the clouds step")
    parser.add_argument("--skip-merge", action="store_true", help="Skip merging to production directory")
    parser.add_argument("--skip-process", action="store_true", help="Skip the data processing (image creation) step")
    
    parser.add_argument("--text-files-only", action="store_true", help="Process step will only process text files")
    parser.add_argument("--merge-only", action="store_true", help="Only perform file merges")
    parser.add_argument("--use-subset", action="store_true", help="Use subsetted data")
    parser.add_argument("--no-reproject", action="store_true", help="Do not reproject the images")
    parser.add_argument("--reprojection-method", type=str, help="Reprojection method", default="average")
    parser.add_argument("--use-input-filename", action="store_true", help="Use the same name format as the input TEMPO files")
    parser.add_argument("--one-file", action="store_true", help="Only get one file")
    parser.add_argument("--delete-after-merge", action="store_true", help="Delete images in original directory after merge")
    parser.add_argument("--no-output", action="store_true", help="Do not output images or text files")
    parser.add_argument("--dry-run", action="store_true", help="Print the commands that would be run, but do not run them")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logger")
    parser.add_argument("--name", type=str, help="Name of the data directory", default=None)
    # parser.add_argument("--skip-compress", action="store_true", help="Skip the compress")
    return parser.parse_args()

def load_config(args: argparse.Namespace) -> None:
    """
    Load configuration from YAML file and override with command-line arguments.
    """
    with open(args.config, "r") as file:
        config = yaml.safe_load(file)
    for key, value in config.items():
        if getattr(args, key, None) is None:
            logger.debug(f"Setting {key} to {value} from config")
            setattr(args, key, value)
        
        # This is not a good habit, but we only use True/False values on things
        # that are by default False. Therefore it is True in the config file,
        # we should respect that. 
        elif getattr(args, key, None) is False:
            logger.debug(f"Setting {key} to {value} from config")
            setattr(args, key, value)
        else:
            logger.debug(f"Keeping {key} as {getattr(args, key)}")
            




def setup_directories(args: argparse.Namespace, root_dir: Path) -> None:
    """
    Set up input/output directories.
    """
    args.data_dir = make_absolute(args.data_dir, root_dir) if args.data_dir else None
    args.merge_dir = make_absolute(args.merge_dir, root_dir)
    args.output_dir = make_absolute(args.output_dir, root_dir) if args.output_dir else None

def log_summary(args: argparse.Namespace, root_dir: Path) -> None:
    """
    Log the summary of the configuration.
    """
    logger.info("Log Summary:")
    logger.info(f"Root directory: {root_dir}")
    logger.info(f"Data directory: {args.data_dir}")
    logger.info(f"Merge directory: {args.merge_dir}")
    logger.info(f"Output directory: {args.output_dir}")
    logger.info(f"Skip subset: {args.skip_subset}")
    logger.info(f"Name: {args.name}")

def check_and_create_directory(path: Path, dry_run: bool = False) -> None:
    """
    Check if a directory exists, and create it if it does not.
    """
    if not path.exists():
        if dry_run:
            logger.info(f"Would create directory: {path}")
        else:
            ensure_directory(path, parents=True, exist_ok=False)

def main() -> None:
    """
    Main function to process TEMPO data.
    """
    args = parse_arguments()
    set_log_level(args.verbose)
    
    load_config(args)

    root_dir = Path(args.root_dir).resolve()
    setup_directories(args, root_dir)
    # log_summary(args, root_dir)

    if args.skip_download and not args.data_dir:
        logger.error("--skip-download requires --data-dir. Exiting...")
        sys.exit(1)

    if args.dry_run:
        logger.info("Dry run")
        
    # get the script directory
    script_dir = Path(__file__).resolve().parent

    run_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    netcdf_data_location = setup_data_folder(args.data_dir, root_dir)

    output_dir = args.output_dir if args.output_dir else netcdf_data_location.name
    output_dir = make_absolute(output_dir, root_dir)
    check_and_create_directory(Path(output_dir), args.dry_run)

    download_list = netcdf_data_location / "download_list.txt"
    download_script_template = script_dir / Path("download_template.sh")
    download_script = netcdf_data_location / "download_template.sh"

    image_directory = make_absolute(f"{output_dir}/images", root_dir)
    resized_image_directory = make_absolute(f"{output_dir}/images/resized_images", root_dir)
    check_and_create_directory(image_directory, args.dry_run)
    check_and_create_directory(resized_image_directory, args.dry_run)

    cloud_image_directory = make_absolute(f"{output_dir}/cloud_images", root_dir)
    resized_cloud_image_directory = make_absolute(f"{output_dir}/cloud_images/resized_images", root_dir)
    check_and_create_directory(cloud_image_directory, args.dry_run)
    check_and_create_directory(resized_cloud_image_directory, args.dry_run)

    merge_directory = args.merge_dir
    image_merge_directory = merge_directory / "released" / "images"
    cloud_merge_directory = merge_directory / "clouds" / "images"
    check_and_create_directory(image_merge_directory, args.dry_run)
    check_and_create_directory(cloud_merge_directory, args.dry_run)

    directories = [
        Path(output_dir),
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

    if not args.skip_download:
        fetch_granule_data(
            args.start_date,
            args.end_date,
            netcdf_data_location,
            download_list,
            download_script_template,
            download_script,
            args.skip_download,
            args.verbose,
            args.dry_run,
            args.one_file,
        )
        validate_directory_exists([download_list, download_script])
    
    # log the relaveant directoris
    logger.info(f"Root directory: {root_dir}")
    logger.info(f"Output directory: {output_dir}")
    
    logger.info(f"NetCDF data location: {netcdf_data_location}")
    logger.info(f"Image directory: {image_directory}")
    logger.info(f"Resized image directory: {resized_image_directory}")
    logger.info(f"Cloud image directory: {cloud_image_directory}")
    logger.info(f"Resized cloud image directory: {resized_cloud_image_directory}")
    logger.info(f"Merge directory: {merge_directory}")
    logger.info(f"Image merge directory: {image_merge_directory}")
    logger.info(f"Cloud merge directory: {cloud_merge_directory}")
    
    if not merge_directory.exists():
        logger.error(f"Merge directory does not exist: {merge_directory}")
        exit(1)
    

    nc_files = list(netcdf_data_location.glob("*.nc"))
    subset_nc_files = list(netcdf_data_location.glob("subsetted_netcdf/*.nc"))
    doesnt_need_data = args.merge_only or args.text_files_only or args.use_subset or args.dry_run
    if not doesnt_need_data and not nc_files and (not args.use_subset or not subset_nc_files):
        logger.info("No new data downloaded")
        exit(0)
    if nc_files or subset_nc_files:
        logger.info(f"Using subsetted data: {len(subset_nc_files)} files" if args.use_subset else f"Using {len(nc_files)} files")

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
        process_args += ["--do-clouds"] if not args.skip_clouds else []
        process_args += ["--dry-run"] if args.dry_run else []
        process_args += ["--no-reproject"] if args.no_reproject else []
        process_args += ["--method", args.reprojection_method] if args.reprojection_method else []
        process_args += ["--text-files-only"] if args.text_files_only else []
        process_args += ["--name", output_dir] if args.name is None else ["--name", args.name]
        process_args += ["--debug"] if (args.verbose or args.dry_run) else []
        process_args += ["--use-input-filename"] if args.use_input_filename else []
        process_args += ["--no-output"] if args.no_output else []
        run_command(["python", str(script_dir / "process_data.py")] + process_args, dry_run=args.dry_run, run_anyway=True)



    if not args.skip_merge:
        run_command(["sh", str(script_dir / "merge.sh"), "-s", str(image_directory) + "/", "-d", str(image_merge_directory)], dry_run=args.dry_run)
        if not args.skip_clouds:
            run_command(["sh", str(script_dir / "merge.sh"), "-s", str(cloud_image_directory) + "/", "-d", str(cloud_merge_directory), "-t" if args.dry_run else "", "-x" if args.delete_after_merge else ""], dry_run=args.dry_run)
    else:
        logger.info("Skipping merge")


    if not args.skip_subset and not args.use_subset and not args.text_files_only:
        run_command(["sh", str(script_dir / "subset_files.sh"), escape_spaces(netcdf_data_location)], args.dry_run, cwd=script_dir)

    if args.dry_run:
        import shutil
        if Path(netcdf_data_location).exists():
            logger.info(f"Removed output directory: {netcdf_data_location}")
            user_input = input(f"Are you sure you want to remove the directory {netcdf_data_location}? (yes/no): ")
            if user_input.lower()[0] == 'y':
                shutil.rmtree(netcdf_data_location)
                logger.info(f"Removed output directory: {netcdf_data_location}")
            else:
                logger.info(f"Did not remove output directory: {netcdf_data_location}")


if __name__ == "__main__":
    main()
