#!/usr/bin/env python
from pathlib import Path
import argparse, sys
import netCDF4 as nc
from netCDF4 import Dataset # type: ignore

from logger import setup_logging, set_log_level

logger = setup_logging()

class Timer:
    # via github copilot
    def __init__(self):
        self.start_time = None
        self.stop_timer = False
        self.thread = None

    def start(self):
        import time
        import threading

        def timer():
            while not self.stop_timer:
                print(
                    f"\rTime elapsed: {time.time() - self.start_time:.2f} seconds",
                    end="",
                )
                time.sleep(0.1)

        self.start_time = time.time()
        self.stop_timer = False
        t = threading.Thread(target=timer)
        t.start()
        self.thread = t

    def stop(self):
        self.stop_timer = True

class TimedContext(object):
    def __init__(self, use_timer = True):
        self.timer = Timer()
        self.use_timer = use_timer
    
    def __enter__(self):
        if self.use_timer:
            self.timer.start()
        return self.timer
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.timer.stop()

def subset_files(filein, fileout, show_time = True):
    

    variables_to_keep = [
        "geolocation/solar_zenith_angle",
        "latitude",
        "longitude",
        "product/vertical_column_troposphere",
        "product/vertical_column_troposphere_uncertainty",
        "product/main_data_quality_flag",
        "support_data/eff_cloud_fraction",
        # "suppord_data/snow_ice_fraction",
        "time",
    ]

    # print(" ======== Subsetting file ========= ")
    # print(f"\t Input file: {filein}")
    # print(f"\t Output file: {fileout}")
    # print(" ================================== ")
    logger.info(f"Subsetting file: {filein} to {fileout}")

    if fileout.exists():
        logger.error(f"\nOutput file {fileout} already exists\n")
        sys.exit(1)


    if args.dry_run:
        logger.info("Dry run: Subsetting file")
    else:
        with TimedContext(use_timer = show_time) as timer:
            # adapted from https://stackoverflow.com/a/49592545/11594175
            with Dataset(filein) as src, Dataset(fileout, "w") as dst:
                dst.setncatts(src.__dict__)
                for name, dimension in src.dimensions.items():
                    dst.createDimension(
                        name, (len(dimension) if not dimension.isunlimited() else None)
                    )
                # need to copy groups and variables in the list. groups have a / in their name
                for name, variable in src.variables.items():
                    if name in variables_to_keep:
                        chunksizes = variable.chunking()
                        if chunksizes == "contiguous":
                            chunksizes = (1,)
                        x = dst.createVariable(
                            name,
                            variable.datatype,
                            variable.dimensions,
                            chunksizes=chunksizes,
                            compression="zlib",
                        )
                        # x.set_var_chunk_cache(variable.get_var_chunk_cache())
                        dst[name].setncatts(src[name].__dict__)
                        dst[name][:] = src[name][:]

                groups_to_keep = [g.split("/")[0] for g in variables_to_keep if "/" in g]
                vars_to_keep = [v.split("/")[1] for v in variables_to_keep if "/" in v]
                for group_name, group in src.groups.items():
                    if group_name in groups_to_keep:
                        dst.createGroup(group_name)
                        for name, variable in group.variables.items():
                            if name in vars_to_keep:
                                name = group_name + "/" + name
                                chunksizes = variable.chunking()
                                if chunksizes == "contiguous":
                                    chunksizes = (1,)
                                x = dst.createVariable(
                                    name,
                                    variable.datatype,
                                    variable.dimensions,
                                    chunksizes=chunksizes,
                                    compression="zlib",
                                )
                                # x.set_var_chunk_cache(variable.get_var_chunk_cache())
                                dst[name].setncatts(src[name].__dict__)
                                dst[name][:] = src[name][:]

    if args.delete:
        if args.dry_run:
            logger.info(f"Dry run: Deleted {filein}")
        else:
            filein.unlink()
            logger.debug(f"\nDeleted {filein}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Subset TEMPO data")

    # The file we want to subset
    parser.add_argument("-f", "--file", type=str, help="File to subset", required=True)

    # The output file name
    parser.add_argument("-o", "--output", type=str, help="Output file name", default="")

    # delete original after subsetting
    parser.add_argument(
        "-d", "--delete", action="store_true", help="Delete original file after subsetting"
    )
    
    # add debug flag
    parser.add_argument("-v", "--debug", action="store_true", help="Verbose output")

    # dry run flag
    parser.add_argument("-n", "--dry-run", action="store_true", help="Dry run")

    args = parser.parse_args()
    # terminate and show help if no arguments are given
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)
        
    filein = Path(args.file)

    if not filein.exists():
        print(f"File {filein} does not exist")
        sys.exit(1)
    
    # Options for the args.output
    # 1. If the output is not given, use the file name with the suffix '_subset'
    # 2. If it is a directory with a file name, use that file name and create the file in that directory
    # 3. If it is just a file name, use that file name
    # 4. If it is just a directory with no file name, use the file name with the suffix '_subset' in that directory

    if args.output == "":
        fileout = filein.with_name(filein.stem + "_subset" + filein.suffix)
    elif Path(args.output).is_dir():
        # create the directory if it does not exist
        if not Path(args.output).exists():
            Path(args.output).mkdir(parents=True, exist_ok=False)
        fileout = Path(args.output) / filein.name
    else:
        fileout = Path(args.output)

    if args.debug  or args.dry_run:
        set_log_level(debug = True)
    
    subset_files(filein = filein, fileout = fileout)