"""Builds the agriculture emissions zarr consumed by ``agriculture_stages``.

Livestock emissions are published as a global COG on their native grid
(~10km, WGS84), carrying a per-hectare rate in kg/ha. This resamples it onto
the vegetation zarr's 30m grid (the reference grid for the whole Land GHG
inventory) via ``odc.geo``'s dask-parallel nearest-neighbor reprojection,
multiplies by the UMD pixel-area zarr (already on the 30m grid, snapped on via
``common.align_to``) to recover an absolute per-pixel total, converts kg ->
Mg, and writes it into the agriculture zarr alongside cropland.

Cropland emissions use a different source and resampling strategy. Cornell's
per-hectare cropland rate (``mean_rate_physical_area``) is normalized by the
physical cropland area within each pixel (e.g. SPAM2020), not by the pixel's
full geographic area -- so multiplying that rate by the UMD pixel-area zarr
(full pixel area) overstates absolute totals wherever a pixel isn't 100%
cropland, which is almost everywhere (QC against an independent country-level
reference table showed pipeline totals ~5.3x too high). Cornell also
publishes the same data as an already-absolute ``total_amount`` COG (kg CO2e
per ~10km pixel, no area normalization). This is used instead: each parent
pixel's total is split evenly across however many 30m reference-grid pixels
fall inside it (a mass-conserving nearest-neighbor downscale, via
``_resample_total_uniformly``), so the sum over children exactly reproduces
the parent's original total rather than replicating it.
"""

import dask.array as da
import numpy as np
import rasterio
import rioxarray as rio
import xarray as xr
from odc.geo.xr import xr_reproject

from pipelines.globals import (
    land_ghg_inventory_agriculture_zarr_uri,
    land_ghg_inventory_vegetation_zarr_uri,
    pixel_area_zarr_uri,
)
from pipelines.land_ghg_inventory.agriculture_stages import (
    AGRICULTURE_SOURCE_VARS,
    AGRICULTURE_ZARR_GROUP,
)
from pipelines.land_ghg_inventory.common import align_to
from pipelines.utils import s3_uri_exists

# Any per-hectare flux variable in the vegetation zarr works as the reference grid --
# only its geobox (30m, EPSG:4326) is used, not its values.
REFERENCE_GRID_VAR = "gross_emissions__all_C_pools__all_gases__MgCO2e_ha_yr"

# Source COGs: static snapshots (single year, no versioning scheme), published
# by Cornell. Cropland is the absolute per-pixel total (kg CO2e); livestock is
# a per-hectare rate (kg/ha).
CROPLAND_COG_URI = (
    "s3://gfw2-data/climate/AFOLU_flux_model/cropland_emissions/"
    "raw__from_Cornell/20250828/year_2020/all_sources/"
    "Global_grid_cropland_emissions_total_amount_CO2eq_all_crops_"
    "without_peat_burn_kg_CO2__20260803.tif"
)
LIVESTOCK_COG_URI = (
    "s3://gfw2-data/climate/AFOLU_flux_model/livestock_emissions/"
    "raw__from_Cornell/20260731_emis_per_ha_only/Total_GHG_Emissions/"
    "Tot_CO2eq_kg_livestock_GHG_emissions_kgCO2e_ha.tif"
)
KG_PER_MG = 1_000


def _reference_geobox():
    """The vegetation zarr's 30m grid, which the agriculture raster resamples to."""
    ref = xr.open_zarr(
        land_ghg_inventory_vegetation_zarr_uri,
        storage_options={"requester_pays": True},
    )[REFERENCE_GRID_VAR]
    ref = ref.isel(year=0, drop=True)
    ref.rio.write_crs("EPSG:4326", inplace=True)
    return ref.odc.geobox


def _resample(cog_uri: str, geobox) -> xr.DataArray:
    """Reproject one source COG (kg/ha) onto ``geobox`` via nearest-neighbor."""
    with rasterio.Env(AWS_REQUEST_PAYER="requester"):
        src = rio.open_rasterio(cog_uri, chunks={"x": 10000, "y": 10000})
    reprojected = xr_reproject(
        src,
        geobox,
        resampling="nearest",
        dst_nodata=0,
        chunks=(10000, 10000),
        always_yx=True,
    )
    if "band" in reprojected.dims:
        reprojected = reprojected.isel(band=0, drop=True)
    return reprojected


def _resample_total_uniformly(cog_uri: str, geobox) -> xr.DataArray:
    """Downscale an absolute per-pixel total COG onto ``geobox``, splitting each
    source pixel's total evenly across however many destination pixels a
    nearest-neighbor reprojection maps onto it.

    Plain nearest-neighbor resampling (as ``_resample`` does) replicates a
    source pixel's value into every destination pixel that maps to it, which
    is only mass-conserving for rates (kg/ha), not for absolute totals (kg) --
    replicating an absolute total would multiply it by the number of
    destination pixels instead of splitting it among them.

    To get an exact per-source-pixel child count that's guaranteed to match
    what ``xr_reproject`` actually does (rather than reimplementing its
    nearest-neighbor + bounds-clipping rules independently, which is easy to
    get subtly wrong at edge/boundary pixels), this reprojects the source
    pixels' own linear index onto ``geobox`` with the same call used for the
    value, then counts how many destination pixels each source index actually
    landed on. Dividing the source total by that count before a second,
    ordinary nearest-neighbor reprojection makes the replication
    mass-conserving: summing a source pixel's children reproduces its
    original total.
    """
    with rasterio.Env(AWS_REQUEST_PAYER="requester"):
        src = rio.open_rasterio(cog_uri, chunks={"x": 10000, "y": 10000})
    if "band" in src.dims:
        src = src.isel(band=0, drop=True)

    # out-of-bounds destination pixels get index ``src.size`` (an extra,
    # discarded bin) instead of a sentinel like -1, so bincount/take need no
    # boolean masking -- awkward on dask arrays with unknown chunk sizes.
    src_index = xr.DataArray(
        np.arange(src.size, dtype="int64").reshape(src.shape),
        dims=src.dims,
        coords={"y": src["y"], "x": src["x"]},
    )
    src_index.rio.write_crs(src.rio.crs, inplace=True)
    reprojected_index = xr_reproject(
        src_index,
        geobox,
        resampling="nearest",
        dst_nodata=src.size,
        chunks=(10000, 10000),
        always_yx=True,
    )
    if "band" in reprojected_index.dims:
        reprojected_index = reprojected_index.isel(band=0, drop=True)

    flat_index = reprojected_index.data.ravel()
    child_counts = da.bincount(flat_index, minlength=src.size + 1)[:-1]
    per_dst_count = da.where(
        flat_index < src.size, child_counts[da.minimum(flat_index, src.size - 1)], 1
    ).reshape(reprojected_index.shape)

    reprojected_value = xr_reproject(
        src,
        geobox,
        resampling="nearest",
        dst_nodata=0,
        chunks=(10000, 10000),
        always_yx=True,
    )
    if "band" in reprojected_value.dims:
        reprojected_value = reprojected_value.isel(band=0, drop=True)

    return reprojected_value.copy(data=reprojected_value.data / per_dst_count)


def create_agriculture_zarr(overwrite: bool = False) -> str:
    """Resample cropland and livestock emissions onto the vegetation grid,
    convert to absolute per-pixel Mg totals, and write the zarr consumed by
    ``agriculture_stages.load_agriculture``."""
    marker_uri = (
        f"{land_ghg_inventory_agriculture_zarr_uri}/{AGRICULTURE_ZARR_GROUP}/zarr.json"
    )
    if not overwrite and s3_uri_exists(marker_uri):
        return land_ghg_inventory_agriculture_zarr_uri

    geobox = _reference_geobox()

    # cropland is already an absolute per-pixel total (kg); split each source
    # pixel's total evenly across its 30m children rather than area-weighting.
    cropland_kg = _resample_total_uniformly(CROPLAND_COG_URI, geobox)
    livestock_kg_ha = _resample(LIVESTOCK_COG_URI, geobox)
    pixel_area_ha = align_to(livestock_kg_ha, pixel_area_zarr_uri)

    cropland = cropland_kg / KG_PER_MG
    livestock = (livestock_kg_ha * pixel_area_ha) / KG_PER_MG

    combined = xr.Dataset(
        {
            AGRICULTURE_SOURCE_VARS["cropland"]: cropland,
            AGRICULTURE_SOURCE_VARS["livestock"]: livestock,
        }
    )
    combined.to_zarr(
        land_ghg_inventory_agriculture_zarr_uri, group=AGRICULTURE_ZARR_GROUP, mode="w"
    )

    return land_ghg_inventory_agriculture_zarr_uri
