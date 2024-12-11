# create a conda environment for running the data pipeline 
conda create --name tempo -c conda-forge python pygeos geos shapely rioxarray h5netcdf rasterio netcdf4 matplotlib Pillow PyYAML scipy zarr xarray requests tqdm dask