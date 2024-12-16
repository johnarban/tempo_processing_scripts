"""
Adapted from code orignially by Jonathan Foster (@jfoster17 on github)
"""

from datetime import datetime, timezone
import numpy as np
import xarray as xr
import glob

import matplotlib.pyplot as plt
import matplotlib.colors as mc
from matplotlib.colors import LinearSegmentedColormap

# set datetime to default to UTC
from colormap import svs_tempo_cmap
from typing import Tuple
import io


import matplotlib.image as mimg
import tqdm.notebook as tqdm

import rasterio
from rasterio import Affine as A
from rasterio.warp import reproject, Resampling, calculate_default_transform

from pathlib import Path

from scipy import ndimage, signal

from PIL import Image

import json
import shapely
from shapely import Polygon
from shapely.ops import transform
from get_tempo_data_utils import run_command

from logger import setup_logging

logger = setup_logging(debug=True, name="process_funcs")



directory = ["./2023m1103", "./2023m1101", "./2024m0328"][2]
sample = False
quality_flag = "high"
input_files = glob.glob(f"{directory}/TEMPO_NO2_L3_V0*_S*.nc")


# sort input_files by datetime
# input_files.sort()


def quality_mask(
    geoloc: xr.Dataset, product: xr.Dataset, support: xr.Dataset, quality_flag
):
    logger.debug(f"Applying quality mask with flag: {quality_flag}")
    if quality_flag == "high":
        high_quality = (geoloc["solar_zenith_angle"] < 80) & (
            product["main_data_quality_flag"] == 0
        )
    elif quality_flag == "medium":
        high_quality = (geoloc["solar_zenith_angle"] < 80) & (
            product["main_data_quality_flag"] == 0
        )
    elif quality_flag == "low":
        high_quality = (geoloc["solar_zenith_angle"] < 80) & (
            product["main_data_quality_flag"] == 0
        )
    elif quality_flag == "svs":
        high_quality = (geoloc["solar_zenith_angle"] <= 80) & (
            product["main_data_quality_flag"] <= 1
        )
    elif quality_flag == "all":
        high_quality = product["main_data_quality_flag"] <= 1
    else:
        return None
    return high_quality


def cloud_cover_mask(quality_flag):
    logger.debug(f"Getting cloud threshold for quality flag: {quality_flag}")
    if quality_flag == "high":
        return 0.2
    elif quality_flag == "medium":
        return 0.4
    elif quality_flag == "svs":
        return 0.5
    else:
        return 0.0


def process_file(
    input_file: str, quality_flag: str = "svs"
) -> tuple[xr.Dataset, datetime | None, xr.Dataset, xr.Dataset]:
    logger.debug(f"Processing file: {input_file}")
    datetimestring = input_file.split("_")[-2]
    try:
        datetimes = datetime.strptime(datetimestring, "%Y%m%dT%H%M%SZ")
    except ValueError:
        datetimes = None
        
    if not Path(input_file).exists():
        logger.error(f"File {input_file} does not exist")
        raise FileNotFoundError(f"File {input_file} does not exist")
    coords = xr.open_dataset(input_file, engine="h5netcdf", chunks="auto")
    product = xr.open_dataset(
        input_file, engine="h5netcdf", chunks="auto", group="product"
    )
    geoloc = xr.open_dataset(
        input_file, engine="h5netcdf", chunks="auto", group="geolocation"
    )
    support = xr.open_dataset(
        input_file, engine="h5netcdf", chunks="auto", group="support_data"
    )
    product = product.assign_coords(coords.coords)

    mask = quality_mask(geoloc, product, support, quality_flag)
    masked_product = product.where(mask)

    # cloud_mask = cloud_quality_mask(geoloc, product, support, quality_flag)
    masked_support = support.where(mask)

    logger.debug(f"Processed file: {input_file}")
    return masked_product, datetimes, coords, masked_support


def get_field_of_regards(geospatial_bounds):
    logger.debug("Getting field of regards")
    shape = transform(lambda x, y, *args: (y, x), shapely.from_wkt(geospatial_bounds))
    json_spec = shapely.to_geojson(shape)
    return {"type": "GeometryCollection", "geometries": [json.loads(json_spec)]}


def get_bounds(chunk: xr.DataArray, pairs=False, bbox=False):
    logger.debug("Getting bounds of the data chunk")
    """
    Get the bounds of the data chunk
    
    returns: tuple of (lon_min, lon_max, lat_min, lat_max)
    if pairs True: returns a list of pairs [(lat_min, lon_min), (lat_max, lon_max)]
    if bbox True: returns a tuple of (left, bottom, right, top)
    """
    bounds = chunk.rio.bounds()
    left, bottom, right, top = bounds
    if pairs:
        return [(bottom, left), (top, right)]
    if bbox:
        return left, bottom, right, top
    return left, right, bottom, top

    # lon = chunk['longitude'].values
    # lat = chunk['latitude'].values
    # lat_min, lat_max = lat.min(), lat.max()
    # lon_min, lon_max = lon.min(), lon.max()
    # if pairs:
    #     return [(lat_min, lon_min), (lat_max, lon_max)]
    # if bbox: # left, bottom, right, top
    #     return lon.min(), lat.min(), lon.max(), lat.max()
    # return lon.min(), lon.max(), lat.min(), lat.max()


def project_array(
    array, bounds, refinement: float = 1, projection="EPSG:3857", method="nearest"
):
    logger.debug(f"Projecting array with method: {method}")
    """
    from Jonathan Foster
    Project a numpy array defined in WGS84 coordinates to Mercator Web coordinate system
    Web Mercator / Spherical Mercator / Pseudo-Mercator is the most common CRS for web maps.
    Web Mercator is EPSG:3857
    
    ipyleaflets use the Mercator Web coordinate system.
    :arg array: Data in 2D numpy array
    :arg bounds: Image latitude, longitude bounds, [(lat_min, lon_min), (lat_max, lon_max)]
    :kwarg int refinement: Scaling factor for output array resolution.
        refinement=1 implies that output array has the same size as the input.
    :method nearest, average, bilinear, cubic, med, sum: Resampling method
    """
    with rasterio.Env():

        (lat_min, lon_min), (lat_max, lon_max) = bounds
        nlat, nlon = array.shape
        dlat = (lat_max - lat_min) / nlat
        dlon = (lon_max - lon_min) / nlon
        src_transform = A.translation(lon_min, lat_min) * A.scale(dlon, dlat)
        src_crs = {"init": "EPSG:4326"}

        nlat2 = int(nlat * refinement)
        nlon2 = int(nlon * refinement)
        dst_shape = (nlat2, nlon2)
        dst_crs = {"init": projection}
        bbox = [lon_min, lat_min, lon_max, lat_max]
        dst_transform, width, height = calculate_default_transform(
            src_crs, dst_crs, nlon, nlat, *bbox, dst_width=nlon2, dst_height=nlat2
        )
        dst_shape = height, width
        destination = np.zeros(dst_shape) # type: ignore

        if method == "average":
            method = Resampling.average
        elif method == "nearest":
            method = Resampling.nearest
        elif method == "bilinear":
            method = Resampling.bilinear
        elif method == "cubic":
            method = Resampling.cubic
        elif method == "med":
            method = Resampling.med
        elif method == "sum":
            method = Resampling.sum
        else:
            method = Resampling.average

        reproject(
            array,
            destination,
            src_transform=src_transform,
            src_crs=src_crs,
            dst_transform=dst_transform,
            dst_crs=dst_crs,
            resampling=method,
        )
        logger.debug("Projection completed")
        return destination


def save_grayscale_with_transparency(data, filename, vmin=None, vmax=None):
    logger.debug(f"Saving grayscale image with transparency to: {filename}")
    # Create an alpha channel where NaNs will be 0 (transparent) and others will be 255 (opaque)
    alpha_channel = np.where(np.isnan(data), 0, 255).astype(np.uint8)

    # Clamp the data if vmin or vmax is specified
    if vmin is not None:
        data = np.maximum(data, vmin)
    if vmax is not None:
        data = np.minimum(data, vmax)

    # Normalize the data to 0-255 and convert to uint8, keeping NaNs intact
    data_min = np.nanmin(data)
    data_max = np.nanmax(data)
    data_normalized = np.nan_to_num(
        (255 * (data - data_min) / (data_max - data_max))
    ).astype(np.uint8)

    # Replace NaNs in data_normalized with 0 to avoid issues when converting to image
    data_normalized[alpha_channel == 0] = 0

    # Convert the data array to a Pillow image in grayscale mode ('L')
    grayscale_img = Image.fromarray(data_normalized, mode="L")

    # Convert to RGBA mode and apply transparency
    rgba_img = grayscale_img.convert("RGBA")
    rgba_img.putalpha(Image.fromarray(alpha_channel))

    # Save the RGBA image
    rgba_img.save(filename)
    logger.debug("Grayscale image saved")
    return rgba_img


def reproject_data(
    xarray: xr.DataArray, bounds, reproject=True, method="average"
) -> Tuple[np.ndarray, np.ndarray]:
    logger.debug("Reprojecting data")
    og_data = xarray.to_numpy()

    if reproject:
        projection = "EPSG:3857"  # Web Mercator
    else:
        projection = "EPSG:4326"  # WGS84 / Equirectangular

    # Always do both refinements
    full_res = project_array(
        og_data, bounds, refinement=1, projection=projection, method=method
    )
    half_res = project_array(
        og_data, bounds, refinement=0.5, projection=projection, method=method
    )

    logger.debug("Reprojection completed")
    return full_res, half_res


# def save_image(
#     projected_data: np.ndarray,
#     cmap: LinearSegmentedColormap | str,
#     vmin: float,
#     vmax: float,
#     filename: Path | str,
# ) -> None:
#     logger.debug(f"Saving image to: {filename}")
#     mimg.imsave(
#         fname=filename,
#         arr=projected_data,
#         cmap=cmap,
#         vmin=vmin,
#         vmax=vmax,
#         origin="upper",
#     )
#     logger.debug("Image saved")

def save_image(
    projected_data: np.ndarray,
    cmap: LinearSegmentedColormap | str,
    vmin: float,
    vmax: float,
    filename: Path | str,
) -> None:
    save_image_compressed_command(projected_data, cmap, vmin, vmax, filename)


def save_image_compressed_buffer(
    projected_data: np.ndarray,
    cmap: LinearSegmentedColormap | str,
    vmin: float,
    vmax: float,
    filename: Path | str,
) -> None:
    logger.debug(f"Saving image to: {filename}")
    buffer = io.BytesIO()
    mimg.imsave(
        fname=buffer,
        arr=projected_data,
        cmap=cmap,
        vmin=vmin,
        vmax=vmax,
        origin="upper",
        format="png"
    )
    buffer.seek(0)
    with Image.open(buffer) as img:
        compressed_buffer = io.BytesIO()
        img = img.convert("P", palette=Image.ADAPTIVE, colors=256)
        img.save(compressed_buffer, format="PNG", optimize=True)
        compressed_buffer.seek(0)
        with open(filename, "wb") as f:
            f.write(compressed_buffer.getvalue())
    
    logger.debug("Image saved")
    
def save_image_compressed_command(
    projected_data: np.ndarray,
    cmap: LinearSegmentedColormap | str,
    vmin: float,
    vmax: float,
    filename: Path | str,
    compression_filter: int = 4,
    compression_level: int = 9,
    compression_strategy: int = 1,
) -> None:
    logger.debug(f"Saving image to: {filename}")
    
    if Path(filename).exists():
        logger.debug(f"File {filename} already exists. Skipping creation.")
    else:
        mimg.imsave(
            fname=filename,
            arr=projected_data,
            cmap=cmap,
            vmin=vmin,
            vmax=vmax,
            origin="upper",
            format="png"
        )
    # use the imagemagick command line tool to compress the image
    # convert "$file" -define png:compression-filter=5 -define png:compression-level=1 -define png:compression-strategy=3 "$file"
    outfilename = str(filename)
    
    # if Path(outfilename).exists():
    #     logger.debug(f"Compressed file {outfilename} already exists. Skipping creation.")
    #     return
    
    run_command(
        [
            "convert", str(filename),
            "-define", f"png:compression-filter={compression_filter}",
            "-define", f"png:compression-level={compression_level}",
            "-define", f"png:compression-strategy={compression_strategy}",
            outfilename
        ],
        dry_run=False,
        background=True
    )
    
    logger.debug("Image saved")


# Modify the existing plot_image function if needed
def plot_image(
    projected_data: np.ndarray,
    cmap=None,
    vmin=0,
    vmax=1,
    filename: Path | str = "out.png",
    greyscale=False,
):
    logger.debug(f"Plotting image to: {filename}")
    if cmap is not None:
        save_image(projected_data, cmap, vmin, vmax, filename)
    else:
        if greyscale:
            save_grayscale_with_transparency(projected_data, filename, vmin, vmax)
        else:
            save_image(projected_data, "gray", vmin, vmax, filename)
    logger.debug("Image plotted")


# file name format is tempo_2024-03-28T12h24m.png
def chunk_time_to_fname(chunck: xr.DataArray, suffix="") -> str:
    logger.debug("Generating filename from chunk time")
    time = chunck.time.values
    time_str = time.astype("datetime64[s]").astype(datetime).strftime("%Y-%m-%dT%Hh%Mm")
    return f"tempo_{time_str}{suffix}.png"


def chunk_time_to_jstime(chunck: xr.DataArray) -> int:
    logger.debug("Converting chunk time to JS timestamp")
    time = chunck.time.values
    # get the number of seconds since the epoch
    time_str = time.astype("datetime64[s]").astype(datetime).strftime("%Y-%m-%dT%Hh%Mm")
    # convert the time string to datetime
    d = datetime.strptime(time_str, "%Y-%m-%dT%Hh%Mm")
    # need to convert timezone to utc https://www.phind.com/search?cache=bywp7qy3dytgxu48phlmlg7e
    d_utc = d.replace(tzinfo=timezone.utc)
    # get the number of milliseconds since the epoch
    time = int(d_utc.timestamp() * 1000)

    return time
