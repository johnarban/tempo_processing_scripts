"""
Adapted from code orignially by Jonathan Foster (@jfoster17 on github)
"""
from datetime import datetime, timezone
import numpy as np
import xarray as xr
import glob

import matplotlib.pyplot as plt
import matplotlib.colors as mc
# set datetime to default to UTC
from colormap import svs_tempo_cmap


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

directory = ['./2023m1103', './2023m1101', './2024m0328'][2]
sample = False
quality_flag = 'high'
input_files = glob.glob(f"{directory}/TEMPO_NO2_L3_V0*_S*.nc")



# sort input_files by datetime
#input_files.sort()

def quality_mask(geoloc: xr.Dataset, product: xr.Dataset, support: xr.Dataset, quality_flag):
    
    if quality_flag == 'high':
        high_quality = (geoloc['solar_zenith_angle'] < 80) & (product['main_data_quality_flag'] == 0) & (support['eff_cloud_fraction'] < 0.2)
        return high_quality
    elif quality_flag == 'medium':
        med_quality = (geoloc['solar_zenith_angle'] < 80) & (product['main_data_quality_flag'] == 0) & (support['eff_cloud_fraction'] < 0.4)
        return med_quality
    elif quality_flag == 'low':
        low_quality = (geoloc['solar_zenith_angle'] < 80) & (product['main_data_quality_flag'] == 0)
        return low_quality
    elif quality_flag == 'svs':
        svs_quality = (geoloc['solar_zenith_angle'] <= 80) & (product['main_data_quality_flag'] <= 1) & (support['eff_cloud_fraction'] <= 0.5)
        return svs_quality
    elif quality_flag == 'all':
        return product['main_data_quality_flag'] <= 1
    return None

def cloud_quality_mask(geoloc: xr.Dataset, product: xr.Dataset, support: xr.Dataset, quality_flag):
    if quality_flag == 'high':
        high_quality = (geoloc['solar_zenith_angle'] < 80) & (product['main_data_quality_flag'] == 0) & (support['eff_cloud_fraction'] >= 0.2)
        return high_quality
    elif quality_flag == 'medium':
        med_quality = (geoloc['solar_zenith_angle'] < 80) & (product['main_data_quality_flag'] == 0) & (support['eff_cloud_fraction'] >= 0.4)
        return med_quality
    elif quality_flag == 'low':
        low_quality = (geoloc['solar_zenith_angle'] < 80) & (product['main_data_quality_flag'] == 0)
        return low_quality
    elif quality_flag == 'svs':
        svs_quality = (geoloc['solar_zenith_angle'] <= 80) & (product['main_data_quality_flag'] <= 1) & (support['eff_cloud_fraction'] > 0.5)
        return svs_quality
    elif quality_flag == 'all':
        return product['main_data_quality_flag'] <= 1
    return None


def process_file(input_file: str, quality_flag: str = 'svs') -> tuple[xr.Dataset, datetime | None, xr.Dataset, xr.Dataset] :
    datetimestring = input_file.split('_')[-2]
    try:
        datetimes = datetime.strptime(datetimestring, '%Y%m%dT%H%M%SZ')
    except ValueError:
        datetimes = None
    coords = xr.open_dataset(input_file, engine='h5netcdf', chunks='auto')
    product = xr.open_dataset(input_file, engine='h5netcdf', chunks='auto', group='product')
    geoloc = xr.open_dataset(input_file, engine='h5netcdf', chunks='auto', group='geolocation')
    support = xr.open_dataset(input_file, engine='h5netcdf', chunks='auto', group='support_data')
    product = product.assign_coords(coords.coords)
    

    mask = quality_mask(geoloc, product, support, quality_flag)
    masked_product = product.where(mask)
    
    cloud_mask = cloud_quality_mask(geoloc, product, support, quality_flag)
    masked_support = support.where(cloud_mask)
    
    return masked_product, datetimes, coords, masked_support
    
def get_field_of_regards(geospatial_bounds):
    shape = transform(lambda x,y, *args: (y,x), shapely.from_wkt(geospatial_bounds))
    json_spec= shapely.to_geojson(shape)
    return {"type":"GeometryCollection","geometries":[json.loads(json_spec)]}
    
    

def get_bounds(chunk: xr.DataArray, pairs = False, bbox = False):
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

def project_array(array, bounds, refinement=1, projection = 'EPSG:3857', method = 'nearest'):
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
        dlat = (lat_max - lat_min)/nlat
        dlon = (lon_max - lon_min)/nlon
        src_transform = A.translation(lon_min, lat_min) * A.scale(dlon, dlat)
        src_crs = {'init': 'EPSG:4326'}

        nlat2 = int(nlat*refinement)
        nlon2 = int(nlon*refinement)
        dst_shape = (nlat2, nlon2)
        dst_crs = {'init': projection}
        bbox = [lon_min, lat_min, lon_max, lat_max]
        dst_transform, width, height = calculate_default_transform(
            src_crs, dst_crs, nlon, nlat, *bbox, dst_width=nlon2, dst_height=nlat2)
        dst_shape = height, width
        destination = np.zeros(dst_shape)
        
        if method == 'average':
            method = Resampling.average
        elif method == 'nearest':
            method = Resampling.nearest
        elif method == 'bilinear':
            method = Resampling.bilinear
        elif method == 'cubic':
            method = Resampling.cubic
        elif method == 'med':
            method = Resampling.med
        elif method == 'sum':
            method = Resampling.sum
        else:
            method = Resampling.average
        
        print('Projecting with method', method.name)

        reproject(
            array,
            destination,
            src_transform=src_transform,
            src_crs=src_crs,
            dst_transform=dst_transform,
            dst_crs=dst_crs,
            resampling=method)
        return destination
    
def save_grayscale_with_transparency(data, filename, vmin=None, vmax=None):
   
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
    data_normalized = np.nan_to_num((255 * (data - data_min) / (data_max - data_min))).astype(np.uint8)


    # Replace NaNs in data_normalized with 0 to avoid issues when converting to image
    data_normalized[alpha_channel == 0] = 0


    # Convert the data array to a Pillow image in grayscale mode ('L')
    grayscale_img = Image.fromarray(data_normalized, mode='L')

    # Convert to RGBA mode and apply transparency
    rgba_img = grayscale_img.convert("RGBA")
    rgba_img.putalpha(Image.fromarray(alpha_channel))

    # Save the RGBA image
    rgba_img.save(filename)

    return rgba_img

def reproject_array(array: xr.DataArray | np.ndarray, bounds, refinement = 1, projection = 'ESPG:4326', method = 'average'):
    if isinstance(array,xr.DataArray):
        array = array.to_numpy()
    
    return project_array(array, 
                         bounds, 
                         refinement=refinement, 
                         projection = projection,
                         method = method
                         )

def plot_image(xarray: xr.DataArray, cmap = None, vmin = None, vmax = None, filename: Path | str = 'out.png', reprojection=True, bounds = None, greyscale = False, method = 'average'):
    
    og_data = xarray.to_numpy()
    
    if reprojection:
        data = project_array(og_data, bounds, refinement=1, method = method)
    else:
        data = og_data[::-1]

    if cmap is not None:
        mimg.imsave(fname = filename, arr = data, cmap=cmap, vmin=vmin, vmax=vmax, origin='upper')
    else:
        if greyscale:
            save_grayscale_with_transparency(data, filename, vmin, vmax)
        else:
            mimg.imsave(fname = filename, arr = data, cmap='gray', vmin=vmin, vmax=vmax, origin='upper')
        


    # data_lowres = xarray.coarsen(latitude=2, longitude=2, boundary='exact').mean().values
    if reprojection:
        data_lowres = project_array(og_data, bounds, refinement=.5, projection = 'EPSG:3857', method = method)
    else:
        data_lowres = project_array(og_data, bounds, refinement=.5, projection = 'EPSG:4326', method = method)
        # data_lowres = xarray.coarsen(latitude=2, longitude=2, boundary='exact').mean().values[::-1]
    new_path = Path(filename)
    new_path = new_path.parent / 'resized_images' / new_path.name
    new_path.parent.mkdir(parents=True, exist_ok=True)
    # check the it is a directory
    if not new_path.is_dir():
        ValueError(f"Path {new_path} is not a directory")
    mimg.imsave(fname = new_path, arr = data_lowres, cmap=cmap, vmin=vmin, vmax=vmax, origin='upper')
    
    
# file name format is tempo_2024-03-28T12h24m.png
def chunk_time_to_fname(chunck: xr.DataArray, suffix = '') -> str:
    time = chunck.time.values
    time_str = time.astype('datetime64[s]').astype(datetime).strftime('%Y-%m-%dT%Hh%Mm')
    return f'tempo_{time_str}{suffix}.png'

def chunk_time_to_jstime(chunck: xr.DataArray) -> int:
    time = chunck.time.values
    #get the number of seconds since the epoch
    time_str = time.astype('datetime64[s]').astype(datetime).strftime('%Y-%m-%dT%Hh%Mm')
    # convert the time string to datetime
    d = datetime.strptime(time_str, '%Y-%m-%dT%Hh%Mm')
    # need to convert timezone to utc https://www.phind.com/search?cache=bywp7qy3dytgxu48phlmlg7e
    d_utc = d.replace(tzinfo=timezone.utc)
    # get the number of milliseconds since the epoch
    time = int(d_utc.timestamp()*1000)
    
    return time
