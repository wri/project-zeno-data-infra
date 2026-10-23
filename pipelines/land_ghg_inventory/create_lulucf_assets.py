import dask
import time
import traceback
import xarray as xr
from typing import List
from odc.geo.cog import save_cog_with_dask


from pipelines.globals import (
    land_ghg_inventory_vegetation_zarr_uri as VEGETATION_ZARR_URI,
    land_ghg_inventory_soc_zarr_uri as SOC_ZARR_URI,
    land_ghg_inventory_organic_soil_zarr_uri as ORGANIC_ZARR_URI,
    land_ghg_inventory_rate_zarr_uri as RATE_ZARR_URI,
    land_ghg_inventory_rate_cog_tile_prefix as COG_TILE_PREFIX
)

# xarray variable names
RATE_VAR = "lulucf_net_flux_MgCO2e_ha_yr"
VEG_NET_FLUX_VAR = "net_flux__all_C_pools__all_gases__MgCO2e_ha_yr"
SOC_NET_FLUX_VAR = "SOC_net__mineral_soil_extent__0-30cm_MgCO2_ha_yr"
PEAT_VARS = ["burned_total_Mg_CO2e_ha_yr", "drained_total_Mg_CO2e_ha_yr"]

# indices
SOC_INTERVAL_INDEX = 3   # 2015-2020 change interval (index 4 distrusted by the model team)
PEAT_INDEX_2020 = 3      # block 2020 -> covers 2016-2020 (5 vegetation years)
PEAT_INDEX_2024 = 4      # block 2024 -> covers 2021-2024 (4 vegetation years)
PEAT_YEARS_2020 = 5
PEAT_YEARS_2024 = 4

# storage options
GFW = {"requester_pays": True}

# tiliing options
TILE_DEGREES = 40
TILE_BLOCKSIZE = 1024    # power of two: required for web-mercator tiling
TILE_BATCHING = 256      # odc-geo tiles per S3 multipart part
LAT_LIMIT = 90           # clip the top band to the zarr's edge

# --- helper functions to create COG tiles ---
def tile_name(min_lon, max_lat) -> str:
    """Name a tile after its north-west corner, e.g. 20S_100E."""
    return (
        f"{abs(max_lat):02.0f}{'S' if max_lat < 0 else 'N'}"
        f"_{abs(min_lon):03.0f}{'W' if min_lon < 0 else 'E'}"
    )

def tile_grid(tile_degrees=TILE_DEGREES, lon_bounds=(-180, 180), lat_bounds=(-60, 100)):
    """Uniform grid as {name: (minx, miny, maxx, maxy)}, clipped to LAT_LIMIT."""
    if (lon_bounds[1] - lon_bounds[0]) % tile_degrees:
        raise ValueError(f"{tile_degrees} deg does not divide the longitude range")
    grid = {}
    for min_lon in range(int(lon_bounds[0]), int(lon_bounds[1]), tile_degrees):
        for min_lat in range(int(lat_bounds[0]), int(lat_bounds[1]), tile_degrees):
            max_lat = min(min_lat + tile_degrees, LAT_LIMIT)
            grid[tile_name(min_lon, max_lat)] = (
                min_lon, min_lat, min_lon + tile_degrees, max_lat,
            )
    return grid

def clip_tile(layer, bbox):
    min_x, min_y, max_x, max_y = bbox
    return layer.sel(x=slice(min_x, max_x), y=slice(max_y, min_y))  # y descends

def tile_task(name, bbox, prefix=COG_TILE_PREFIX, compression="deflate"):
    """(uri, dask task) for one tile. No .chunk(): odc-geo rechunks to blocksize."""
    uri = f"{prefix}/{name}.tif"
    return uri, save_cog_with_dask(
        net_flux_tile(bbox), uri,
        compression=compression, predictor=3, stats=False,
        tile_batching=TILE_BATCHING, overview_resampling="average",
        blocksize=TILE_BLOCKSIZE,
    )

def create_rate_zarr(overwrite: bool = False) -> str:

    # vegetation: strict /9 average (missing year -> 0 via sum; min_count keeps ocean NaN)
    veg_nf = xr.open_zarr(VEGETATION_ZARR_URI, storage_options=GFW)[VEG_NET_FLUX_VAR]
    veg_nf = veg_nf.rio.write_crs("EPSG:4326")
    n_years = veg_nf.sizes["year"]
    veg_avg = veg_nf.sum("year", min_count=1) / n_years
    ref = veg_avg   # snap the soil layers onto this grid

    # mineral SOC: single 2015-2020 interval, used as the annual rate for all years
    soc = xr.open_zarr(SOC_ZARR_URI, storage_options=GFW)[SOC_NET_FLUX_VAR]
    soc_avg = soc.isel(year=SOC_INTERVAL_INDEX, drop=True).rio.write_crs("EPSG:4326")
    soc_avg = soc_avg.reindex_like(ref, method="nearest", tolerance=1e-4)

    # organic peat: annualized average of the two 5-year blocks
    org = xr.open_zarr(ORGANIC_ZARR_URI, storage_options=GFW)
    peat = (org[PEAT_VARS[0]] + org[PEAT_VARS[1]]).rio.write_crs("EPSG:4326")
    peat_avg = (
        PEAT_YEARS_2020 * peat.isel(year=PEAT_INDEX_2020, drop=True)
        + PEAT_YEARS_2024 * peat.isel(year=PEAT_INDEX_2024, drop=True)
    ) / n_years
    peat_avg = peat_avg.reindex_like(ref, method="nearest", tolerance=1e-4)

    # combine (sparse soil -> 0), then restore NaN outside the vegetation (land) extent
    rate = veg_avg.fillna(0) + soc_avg.fillna(0) + peat_avg.fillna(0)
    rate = rate.where(veg_avg.notnull()).astype("float32")
    rate = rate.rio.write_crs("EPSG:4326").rename(RATE_VAR)
    rate.encoding.clear()   # drop inherited chunks/preferred_chunks from the source zarr

    rate.to_dataset().to_zarr(RATE_ZARR_URI, mode="w")

    return RATE_ZARR_URI

def create_cogs(tile_names: List[str], tiles=None, prefix=COG_TILE_PREFIX):
    tiles = tile_grid()

    rate_global = xr.open_zarr(RATE_ZARR_URI)[RATE_VAR].rio.write_crs("EPSG:4326")

    written, failed = {}, {}
    started_all = time.time()
    for i in enumerate(tile_names, 1):
        started = time.time()
        try:
            uri, task = tile_task(tile_name, tiles[tile_name], prefix=prefix)
            dask.compute(task)
        except Exception as err:
            failed[tile_name] = traceback.format_exc()
            print(f"[{i:2d}/{len(tile_names)}] {tile_name:9s} FAILED after "
                  f"{time.time() - started:5.0f}s  {type(err).__name__}: {err}", flush=True)
            continue
        written[tile_name] = uri
        print(f"[{i:2d}/{len(tile_names)}] {tile_name:9s} {time.time() - started:5.0f}s  {uri}",
              flush=True)
    print(f"\n{len(written)} written, {len(failed)} failed in "
          f"{(time.time() - started_all) / 60:.0f} min")
    return written, failed