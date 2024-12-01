#!/Users/jal194/anaconda3/bin/python
import glob
import json
from pathlib import Path
import argparse, sys
import numpy as np
import xarray as xr
from tempo_process_funcs import process_file, get_bounds, chunk_time_to_fname, chunk_time_to_jstime, svs_tempo_cmap, get_field_of_regards, reproject_data, save_image, cloud_cover_mask
import tqdm
from concurrent.futures import ThreadPoolExecutor
from matplotlib.colors import LinearSegmentedColormap
import logging
from typing import List, Tuple

cloud_cmap = LinearSegmentedColormap.from_list('gray_solid', ['#707070', '#707070'], N=256)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.
    """
    parser = argparse.ArgumentParser(description='Process TEMPO data')
    parser.add_argument('-d', '--directory', type=str, help='Directory containing TEMPO data', default='./data')
    parser.add_argument('-o', '--output', type=str, help='Output directory for images', default='.')
    parser.add_argument('--do-clouds', action='store_true', help='Process cloud data')
    parser.add_argument('--cloud-dir', type=str, help='Output directory for cloud images', default='.')
    parser.add_argument('-q', '--quality', type=str, help='Quality flag for data', default='svs')
    parser.add_argument('-s', '--sample', type=bool, help='Sample data', default=False)
    parser.add_argument('-i', '-p', '--input', '--pattern', type=str, help='Input files pattern', default=None)
    parser.add_argument('-n', '--name', type=str, help='Name for output files', default=None)
    parser.add_argument('-v', '--version', type=str, help='TEMPO version', default='1')
    parser.add_argument('-l', '--level', type=str, help='TEMPO time', default='3')
    parser.add_argument('--suffix', type=str, help='A suffix to append to filename', default='')
    parser.add_argument('--singlethreaded', help='Create singlethreaded only', action='store_true')
    parser.add_argument('--dry-run', help='Print the commands that would be run, but do not run them', action='store_true')
    parser.add_argument('--no-reproject', help='Do not reproject the images', action='store_true')
    parser.add_argument('--method', type=str, help='Method to use for reprojection', default='average')
    parser.add_argument('--text-files-only', help='Only process text files', action='store_true')
    parser.add_argument('--debug', help='Enable debug logging', action='store_true')
    parser.add_argument('--cloud-cmap', help='Set color map for clouds cover. Default is solid grey')
    parser.add_argument('--no-output', action='store_true', help='Do not create text and image files')
    return parser.parse_args()

def setup_logging(debug: bool) -> None:
    """
    Set up logging configuration.
    """
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format='%(asctime)s - %(levelname)s - %(message)s')

def setup_directories(args: argparse.Namespace, dry_run = False) -> Tuple[Path, Path, Path]:
    """
    Set up directories for input and output.
    """
    directory = Path(args.directory)
    if not directory.exists():
        logging.error(f"Directory {directory} does not exist")
        if not dry_run:
            sys.exit(1)

    output = Path(args.output)
    if not dry_run:
        output.mkdir(parents=True, exist_ok=True)

    cloud_output = Path(args.cloud_dir)
    if not dry_run:
        cloud_output.mkdir(parents=True, exist_ok=True)

    logging.debug(f"Directories set up: {directory}, {output}, {cloud_output}")
    return directory, output, cloud_output


def get_input_files(directory: Path, pattern: str, level: str, version: str) -> List[str]:
    """
    Get input files based on the provided pattern.
    """
    if pattern is None:
        pattern = f"TEMPO_NO2_L{level}_V0{version}*_S*.nc"
    files = glob.glob(f"{directory}/{pattern}")
    logging.debug(f"Found {len(files)} input files with pattern {pattern}")
    return files


def process_files(input_files: List[str], quality_flag: str, sample: bool,
                  process_func) -> Tuple[List[xr.Dataset], List[str], List[dict], List[xr.Dataset]]:
    """
    Process input files and extract relevant data.
    """
    if sample:
        input_files = input_files[0:10]
    input_data, datetimes, geospatial_bounds, support = [], [], [], []
    for input_file in tqdm.tqdm(input_files, desc="Reading in data"):
        out = process_func(input_file, quality_flag)
        input_data.append(out[0])
        datetimes.append(out[1])
        geospatial_bounds.append(out[2].geospatial_bounds)
        support.append(out[3])
    logging.debug(f"Processed {len(input_files)} files")
    return input_data, datetimes, geospatial_bounds , support


def combine_data(input_data: List[xr.Dataset],
                 support: List[xr.Dataset]) -> Tuple[xr.DataArray | xr.Dataset, xr.DataArray | xr.Dataset]:
    """
    Combine input data and support data into single datasets.
    """
    # Align coordinates of support datasets with input_data
    aligned_support = []
    for i, s in enumerate(support):
        aligned_support.append(s.assign_coords(input_data[i].coords))

    final_data = xr.combine_by_coords(input_data)
    support_data = xr.combine_by_coords(aligned_support)
    _ = final_data.rio.write_crs("epsg:4326", inplace=True)
    _ = support_data.rio.write_crs("epsg:4326", inplace=True)
    logging.debug("Combined input data and support data")
    return final_data, support_data


def output_text_data(rechunk: xr.DataArray, geospatial_bounds: List[dict], name: str, output: Path, suffix: str, no_output: bool) -> None:
    """
    Output the bounds data to files.
    """
    if no_output:
        logging.info('No output flag is set. Skipping text data output.')
        return

    logging.info('Bounds of the data')
    bounds = get_bounds(rechunk)
    with open(output / f'bounds_{name}.npy', 'w') as f:
        lonmin, lonmax, latmin, latmax = bounds
        lines = [f'lon_min: {lonmin}', f'lon_max: {lonmax}', f'lat_min: {latmin}', f'lat_max: {latmax}']
        logging.info('\n'.join(lines))
        f.write('\n'.join(lines))
        LLatLng = 'L.LatLng({},{})'
        LLatLngBounds = 'L.LatLngBounds({}, {})'
        bounds = [LLatLng.format(latmin, lonmin), LLatLng.format(latmax, lonmax)]
        bounds = LLatLngBounds.format(*bounds)
        f.write(f'\n{bounds}')

    fors = []
    for i, geo in enumerate(geospatial_bounds):
        fors.append(get_field_of_regards(geo))

    with open(output / f'bounds_{name}_geojson.json', 'w') as f:
        json.dump(fors, f)
        
    
    logging.info(f'Saving times to {output} as times_{name}.npy')
    times = list(sorted([chunk_time_to_jstime(ch) for ch in rechunk]))

    with open(output / f'times_{name}{suffix}.npy', 'w') as f:
        f.write(str(times))
    logging.debug(f"Output text data to {output}")

def process_and_save_chunk(chunk: xr.DataArray, cloud_data: xr.DataArray, cmap: LinearSegmentedColormap, vmin: float, vmax: float, output: Path,
                           suffix: str, bounds, reproject=True, method='average', cloud_threshold: float = 0.5, cloud_output = False, no_output=False) -> None:
    if no_output:
        logging.info('No output flag is set. Skipping image saving.')
        return

    logging.debug(f"Processing chunk with time {chunk.time.values}")
    
    # Reproject data without applying cloud mask
    full_res, half_res = reproject_data(chunk, bounds, reproject, method)
    
    # Reproject cloud data
    full_res_cloud, half_res_cloud = reproject_data(cloud_data, bounds, reproject, method)
    
    # Generate cloud masks for full and half resolution
    full_cloud_mask = full_res_cloud > cloud_threshold
    half_cloud_mask = half_res_cloud > cloud_threshold
    
    # Apply cloud mask after reprojection
    if not cloud_output:
        full_res_masked = np.where(~full_cloud_mask, full_res, np.nan)
        half_res_masked = np.where(~half_cloud_mask, half_res, np.nan)
    else:
        full_res_masked = np.where(full_cloud_mask, full_res, np.nan)
        half_res_masked = np.where(half_cloud_mask, half_res, np.nan)
    
    # Save full resolution image
    full_filename = output / chunk_time_to_fname(chunk, suffix)
    save_image(full_res_masked, cmap, vmin, vmax, full_filename)
    logging.debug(f"Saved full resolution image to {full_filename}")
    
    # Save half resolution image
    half_filename = output / 'resized_images' / chunk_time_to_fname(chunk, suffix)
    half_filename.parent.mkdir(parents=True, exist_ok=True)
    save_image(half_res_masked, cmap, vmin, vmax, half_filename)
    logging.debug(f"Saved half resolution image to {half_filename}")


def process_new_data(dataarray: xr.DataArray, cloud_data: xr.DataArray, geospatial_bounds: List[dict], name: str, suffix: str, output: Path,
                     args: argparse.Namespace, cmap: LinearSegmentedColormap, vmin: float, vmax: float, reproject = True, method = 'average', cloud_threshold: float = 0.5, cloud_output = False) -> None:

    logging.debug("Rechunking data")
    rechunk = dataarray.chunk(chunks={"longitude": 188, "latitude": 373, "time": 1})
    output_text_data(rechunk, geospatial_bounds, name, output, suffix, args.no_output)

    if args.text_files_only:
        return

    logging.info(f"Processing {name} data")

    def process_chunk(time):
        chunk = rechunk.sel(time=time)
        cloud_chunk = cloud_data.sel(time=time)
        process_and_save_chunk(chunk, cloud_chunk, cmap, vmin, vmax, output, suffix, get_bounds(chunk, pairs=True), reproject, method, cloud_threshold, cloud_output = cloud_output, no_output=args.no_output)

    if not args.singlethreaded and len(rechunk.time) >= 3:
        logging.debug('Using ThreadPool')
        with ThreadPoolExecutor(max_workers=10) as executor:
            list(tqdm.tqdm(executor.map(process_chunk, rechunk.time.values), total=len(rechunk.time), desc="Processing chunks"))
    else:
        for time in tqdm.tqdm(rechunk.time.values, desc="Processing chunks"):
            process_chunk(time)


def main() -> None:
    """
    Main function to process TEMPO data.
    """
    args = parse_arguments()
    setup_logging(args.debug)
    if args.dry_run:    
        logging.info("Dry run")
    directory, output, cloud_output = setup_directories(args, args.dry_run)
    input_files = get_input_files(directory, args.input, args.level, args.version)
    
    if args.name is None:
        args.name = directory.resolve().name

    logging.info(
        f"""
        Processing {len(input_files)} files from {directory} with pattern {args.input}.
        Quality flag: {args.quality}.
        Output will be saved in {output}.
        Doing Clouds: {args.do_clouds}.
        Cloud output will be saved in {cloud_output}.
        Reproject: {not args.no_reproject}.
        Reprojection method: {args.method}.
        """
    )
    
    if len(input_files) == 0:
        logging.error(f"No files found in {directory} with pattern {args.input}")
        if not args.dry_run:
            sys.exit(1)
    
    if args.dry_run:
        logging.info("Dry run: Skipping actual processing steps.")
        return

    input_data, datetimes, geospatial_bounds, support = process_files(input_files, args.quality, args.sample, 
                                                                      process_file)
    
    final_data, support_data = combine_data(input_data, support)
    final_data['vertical_column_troposphere'].name = 'NO2'
    support_data['eff_cloud_fraction'].name = 'Clouds'

    no2_data = final_data['vertical_column_troposphere']
    no2_data = no2_data.rio.write_nodata(np.nan, encoded=True)
    no2_data.data = no2_data.data / 10**16

    cloud_data = support_data['eff_cloud_fraction']
    cloud_data = cloud_data.rio.write_nodata(np.nan, encoded=True)
    cloud_data.data = cloud_data.data / 1

    cloud_threshold = cloud_cover_mask(args.quality)

    process_new_data(no2_data, cloud_data, geospatial_bounds, args.name, args.suffix, output, args, svs_tempo_cmap, 0.01, 1.5, not args.no_reproject, args.method, cloud_threshold)
    
    if args.do_clouds:
        # For cloud data, we use an inverted cloud threshold
        if args.cloud_cmap is None:
            use_cmap = cloud_cmap
        else:
            use_cmap = args.cloud_cmap
        process_new_data(cloud_data, cloud_data, geospatial_bounds, args.name, args.suffix, cloud_output, args, use_cmap, 0.5, 1, not args.no_reproject, args.method, cloud_threshold, cloud_output = True)



if __name__ == "__main__":
    main()
