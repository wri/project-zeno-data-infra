import json
import dask
import time
import traceback
import xarray as xr
from typing import List, Optional
from cogeo_mosaic.backends import MosaicBackend
from cogeo_mosaic.mosaic import MosaicJSON
from odc.geo.cog import save_cog_with_dask
from shapely.geometry import Polygon, box


from pipelines.globals import (
    land_ghg_inventory_vegetation_zarr_uri as VEGETATION_ZARR_URI,
    land_ghg_inventory_soc_zarr_uri as SOC_ZARR_URI,
    land_ghg_inventory_organic_soil_zarr_uri as ORGANIC_ZARR_URI,
    land_ghg_inventory_net_flux_zarr_uri as NET_FLUX_ZARR_URI,
    land_ghg_inventory_net_flux_cog_tile_prefix as COG_TILE_PREFIX
)
from pipelines.prefect_flows.common_stages import clip_ds_to_bbox
from pipelines.utils import s3_uri_exists

# xarray variable names
NET_FLUX_VAR = "lulucf_net_flux_MgCO2e_ha_yr"
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

# mosaic options
MERCATOR_MAX_LAT = 85.0511287798066  # web-mercator latitude limit
MOSAIC_MIN_ZOOM = 2
MOSAIC_MAX_ZOOM = 12
MOSAIC_QUADKEY_ZOOM = 5   # index granularity: most lookups resolve to a single COG

# --- helper functions to create COG tiles ---
def tile_name(min_lon, max_lat) -> str:
    """Name a tile after its north-west corner, e.g. 20S_100E."""
    return (
        f"{abs(max_lat):02.0f}{'S' if max_lat < 0 else 'N'}"
        f"_{abs(min_lon):03.0f}{'W' if min_lon < 0 else 'E'}"
    )


def tile_grid(tile_degrees=TILE_DEGREES, lon_bounds=(-180, 180), lat_bounds=(-60, 100)):
    """Uniform grid as {name: shapely box}, clipped to LAT_LIMIT."""
    if (lon_bounds[1] - lon_bounds[0]) % tile_degrees:
        raise ValueError(f"{tile_degrees} deg does not divide the longitude range")
    grid = {}
    for min_lon in range(int(lon_bounds[0]), int(lon_bounds[1]), tile_degrees):
        for min_lat in range(int(lat_bounds[0]), int(lat_bounds[1]), tile_degrees):
            max_lat = min(min_lat + tile_degrees, LAT_LIMIT)
            grid[tile_name(min_lon, max_lat)] = box(
                min_lon, min_lat, min_lon + tile_degrees, max_lat,
            )
    return grid


def tiles_in_bbox(bbox: Optional[Polygon], tiles=None) -> dict:
    """Grid tiles whose extent intersects `bbox` (None -> the whole grid). Tiles
    stay on the global grid, so a partially covered tile is kept in full."""
    tiles = tile_grid() if tiles is None else tiles
    if bbox is None:
        return tiles
    return {name: poly for name, poly in tiles.items() if poly.intersects(bbox)}


def net_flux_tile(layer, bbox):
    """Per-hectare LULUCF net flux (MgCO2e/ha/yr) for one tile, full float32."""
    flux = clip_ds_to_bbox(layer, bbox)
    return flux.rio.write_crs("EPSG:4326").rio.write_nodata(float("nan"))


def tile_task(name, layer, bbox, prefix=COG_TILE_PREFIX, compression="deflate"):
    """(uri, dask task) for one tile. No .chunk(): odc-geo rechunks to blocksize."""
    uri = f"{prefix}/{name}.tif"
    return uri, save_cog_with_dask(
        net_flux_tile(layer, bbox), uri,
        compression=compression, predictor=3, stats=False,
        tile_batching=TILE_BATCHING, overview_resampling="average",
        blocksize=TILE_BLOCKSIZE,
    )

def create_net_flux_zarr(overwrite: bool = False) -> str:
    if not overwrite and s3_uri_exists(f"{NET_FLUX_ZARR_URI}/zarr.json"):
        return NET_FLUX_ZARR_URI

    # vegetation: strict /9 average (missing year -> 0 via sum; min_count keeps ocean NaN)
    veg_nf = xr.open_zarr(VEGETATION_ZARR_URI, storage_options=GFW)[VEG_NET_FLUX_VAR]
    veg_nf = veg_nf.rio.write_crs("EPSG:4326")
    n_years = veg_nf.sizes["year"]
    veg_avg = veg_nf.sum("year", min_count=1) / n_years
    ref = veg_avg   # snap the soil layers onto this grid

    # mineral SOC: single 2015-2020 interval, used as the annual net_flux for all years
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
    net_flux = veg_avg.fillna(0) + soc_avg.fillna(0) + peat_avg.fillna(0)
    net_flux = net_flux.where(veg_avg.notnull()).astype("float32")
    net_flux = net_flux.rio.write_crs("EPSG:4326").rename(NET_FLUX_VAR)
    net_flux.encoding.clear()   # drop inherited chunks/preferred_chunks from the source zarr

    net_flux.to_dataset().to_zarr(NET_FLUX_ZARR_URI, mode="w")

    return NET_FLUX_ZARR_URI


def create_cogs(
    bbox: Optional[Polygon] = None,
    names: Optional[List[str]] = None,
    prefix=COG_TILE_PREFIX,
    overwrite: bool = False,
    net_flux_zarr_uri: str = NET_FLUX_ZARR_URI,
):
    """Write a COG per 40-deg tile, one at a time. bbox option limits COGs to tiles that
    intersect with the bbox. Existing tiles are skipped unless overwrite is set."""
    tiles = tiles_in_bbox(bbox)
    if names is not None:
        tiles = {name: tiles[name] for name in names}

    net_flux_global = xr.open_zarr(net_flux_zarr_uri)[NET_FLUX_VAR].rio.write_crs("EPSG:4326")

    written, failed = {}, {}
    started_all = time.time()
    for i, name in enumerate(sorted(tiles), 1):
        started = time.time()
        uri = f"{prefix}/{name}.tif"
        if not overwrite and s3_uri_exists(uri):
            written[name] = uri
            print(f"[{i:2d}/{len(tiles)}] {name:9s} exists, skipped  {uri}", flush=True)
            continue
        try:
            uri, task = tile_task(name, net_flux_global, tiles[name], prefix=prefix)
            dask.compute(task)
        except Exception as err:
            failed[name] = traceback.format_exc()
            print(f"[{i:2d}/{len(tiles)}] {name:9s} FAILED after "
                  f"{time.time() - started:5.0f}s  {type(err).__name__}: {err}", flush=True)
            continue
        written[name] = uri
        print(f"[{i:2d}/{len(tiles)}] {name:9s} {time.time() - started:5.0f}s  {uri}",
              flush=True)
    print(f"\n{len(written)} written, {len(failed)} failed in "
          f"{(time.time() - started_all) / 60:.0f} min")
    return written, failed


def create_mosaic(tile_uris, prefix=COG_TILE_PREFIX, overwrite=False, clamp_global=True):
    """Build the mosaic.json index over the net-flux COG tiles at {prefix}/mosaic.json.
    clamp_global forces the global web-mercator extent (tiles carry data past the
    85.05 N/S limit); otherwise from_urls' own bounds are kept (e.g. a bbox subset)."""
    mosaic_uri = f"{prefix}/mosaic.json"
    if not overwrite and s3_uri_exists(mosaic_uri):
        return mosaic_uri

    mosaic = MosaicJSON.from_urls(
        sorted(tile_uris),
        minzoom=MOSAIC_MIN_ZOOM, maxzoom=MOSAIC_MAX_ZOOM, quadkey_zoom=MOSAIC_QUADKEY_ZOOM,
    )
    definition = json.loads(mosaic.model_dump_json(exclude_none=True))
    bounds = [round(v, 6) for v in definition["bounds"]]
    if clamp_global:
        bounds[0], bounds[2], bounds[3] = -180.0, 180.0, MERCATOR_MAX_LAT
    definition["bounds"] = bounds
    definition["center"] = [
        round((bounds[0] + bounds[2]) / 2, 6),
        round((bounds[1] + bounds[3]) / 2, 6),
        definition["minzoom"],
    ]

    with MosaicBackend(mosaic_uri, mosaic_def=MosaicJSON(**definition)) as backend:
        backend.write(overwrite=True)

    per_quadkey = [len(v) for v in definition["tiles"].values()]
    assets = len(set().union(*definition["tiles"].values())) if per_quadkey else 0
    print(f"wrote {mosaic_uri}: {len(per_quadkey)} quadkeys, {assets} assets, "
          f"{min(per_quadkey, default=0)}-{max(per_quadkey, default=0)} assets/quadkey, "
          f"bounds {definition['bounds']}", flush=True)
    return mosaic_uri