# To add a new cell, type ''
# To add a new markdown cell, type ' [markdown]'

import os
import sys
import shutil
from pathlib import Path, PosixPath
from datetime import datetime, timedelta

# setup logging to file reorg_log.txt
import logging


# create logger with 'spam_application'
# logger = logging.getLogger('reorg')
# logger.setLevel(logging.INFO)
# # format
# formatter = logging.Formatter('%(levelname)s: %(message)s')
# create file handler which logs even debug messages
# fh = logging.FileHandler('reorg_log.txt')
# fh.setFormatter(formatter)
# fh.setLevel(logging.INFO)
# logger.addHandler(fh)
# create console handler with a higher log level
# ch = logging.StreamHandler()
# ch.setFormatter(formatter)
# ch.setLevel(logging.INFO)
# logger.addHandler(ch)

RUN = True
TEST = False

def create_directory_structure(base_dir: Path, date: str) -> Path:
    """
    Create the directory structure for the given date.
    """
    day_dir = base_dir / date
    if not day_dir.exists():
        # pass
        if RUN:
            day_dir.mkdir(parents=True, exist_ok=False)
        print(f"Created directory {day_dir}")
        # make subdirectories as well
    
    subdirs = ['subsetted_netcdf','cloud_images/resized_images','images/resized_images']
    for subdir in subdirs:
        subdir_path = day_dir / subdir
        if not subdir_path.exists():
            print(f"Created directory {subdir_path}")
            if RUN:
                subdir_path.mkdir(parents=True, exist_ok=False)
    
    return day_dir

def move_files_to_day_directory(base_dir: Path, in_dirs: list[Path], file_pattern: str, parser) -> None:
    """
    Move files matching the file_pattern to their respective day directories.
    """
    count = 0
    for data_dir in in_dirs:
        for file_path in data_dir.glob(file_pattern):
            if file_path.is_file():
                date = parser(file_path.name)
                day_dir = create_directory_structure(base_dir, date)
                new_path = day_dir / Path(*list(file_path.parts)[1:])
                
                if not new_path.exists():
                    # check the directory exists
                    if new_path.parent.exists():
                        print(f"Moving {file_path} to {new_path}")
                        if RUN:
                            shutil.move(str(file_path), str(new_path))
                            count += 1
                    else:
                        if RUN:
                            raise FileNotFoundError(f"Directory {new_path.parent} does not exist.")
                        else:
                            print(f"Directory {new_path.parent} does not exist.")
                        
                else:
                    pass
                    # logger.error(f"File already exists: original: {file_path} new: {new_path}")
                    # print(f"File already exists:\n\t original: {file_path}\n\t new: {new_path}")
                
                
    print(f"Moved {count} files.")




exclude_dirs = []



valid_directories =  list(set(d.parent.parent for d in  Path("./").glob('*_*/subsetted_netcdf/*.nc')) - set(exclude_dirs))
print("Moving: ", list(valid_directories))



# ['subsetted_netcdf','cloud_images/resized_images','images/resized_images']
base_dir = Path("./")
netcdf_pattern = "./subsetted_netcdf/*.nc"
image_pattern = "./images/*.png"
image_resized_pattern = "./images/resized_images/*.png"
cloud_image_pattern = "./cloud_images/*.png"
cloud_image_resized_pattern = "./cloud_images/resized_images/*.png"
valid_directories = list(set(d.parent.parent for d in  Path("./").glob('*_*/subsetted_netcdf/*.nc')) - set(exclude_dirs))
# valid_directories = [Path("may_01_onward")]

if TEST:
    sys.exit()

def netcdf_parser(filename: str) -> str:
    date_str = filename.split('_')[-2]
    return (datetime.strptime(date_str, "%Y%m%dT%H%M%SZ") - timedelta(hours=5)).strftime('%Y.%m.%d')
# Move NetCDF files
move_files_to_day_directory(base_dir, valid_directories, netcdf_pattern, parser=netcdf_parser)

def image_parser(filename: str) -> str:
    date_str = filename.split('_')[-1].split('.')[0]
    return (datetime.strptime(date_str, "%Y-%m-%dT%Hh%Mm")- timedelta(hours=5)).strftime('%Y.%m.%d')
# Move image files
move_files_to_day_directory(base_dir, valid_directories, image_pattern, parser=image_parser)
move_files_to_day_directory(base_dir, valid_directories, image_resized_pattern, parser=image_parser)
move_files_to_day_directory(base_dir, valid_directories, cloud_image_pattern, parser=image_parser)
move_files_to_day_directory(base_dir, valid_directories, cloud_image_resized_pattern, parser=image_parser)


