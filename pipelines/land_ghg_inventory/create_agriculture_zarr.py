"""Builds the agriculture emissions zarr consumed by ``agriculture_stages``.

Cropland and livestock emissions are both published as absolute per-pixel
totals (kg CO2e) on their native ~10km grid, resampled onto the vegetation
zarr's 30m grid (the reference grid for the whole Land GHG inventory).
Livestock's source COG is Cornell's pre-aggregated cross-animal total.
"""

import rasterio
import rioxarray as rio
import xarray as xr
from odc.geo.xr import xr_reproject

from pipelines.globals import (
    land_ghg_inventory_agriculture_zarr_uri,
    land_ghg_inventory_vegetation_zarr_uri,
)
from pipelines.land_ghg_inventory.agriculture_stages import (
    AGRICULTURE_SOURCE_VARS,
    AGRICULTURE_ZARR_GROUP,
)
from pipelines.utils import s3_uri_exists

# Any per-hectare flux variable in the vegetation zarr works as the reference grid --
# only its geobox (30m, EPSG:4326) is used, not its values.
REFERENCE_GRID_VAR = "gross_emissions__all_C_pools__all_gases__MgCO2e_ha_yr"

# Source COGs: static snapshots (single year, no versioning scheme), both
# absolute per-pixel totals in kg CO2e (see module docstring).
CROPLAND_COG_URI = (
    "s3://gfw2-data/climate/AFOLU_flux_model/cropland_emissions/"
    "raw__from_Cornell/20250828/year_2020/all_sources/"
    "Global_grid_cropland_emissions_total_amount_CO2eq_all_crops_"
    "without_peat_burn_kg_CO2__20260803.tif"
)
LIVESTOCK_COG_URI = (
    "s3://gfw-data-lake/wri_land_ghg_monitoring_system/v1.0.3/raw_data/"
    "Total_GHG_kg_CO2e_yr_Livestock_ALL.tif"
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


def _resample_total_uniformly(cog_uri: str, geobox) -> xr.DataArray:
    """Downscale an absolute per-pixel total COG onto ``geobox`` by splitting
    each source pixel's total evenly across its destination children.

    Plain nearest-neighbor resampling replicates a source pixel's value into
    every destination pixel that maps to it, which is only mass-conserving
    for rates (kg/ha), not for absolute totals (kg) -- replicating an
    absolute total would multiply it by the number of destination pixels
    instead of splitting it among them.

    The true child count per source pixel alternates by +/-1 around
    ``(src_res / dst_res) ** 2`` (e.g. 333 or 334 here, since 0.08333.../0.00025
    isn't an integer ratio) depending on where a given source pixel happens to
    land relative to the destination grid. Rather than computing that exact,
    varying count, this divides by the unrounded ratio everywhere -- a single
    global constant. Any one source pixel's children then sum to only
    approximately (not exactly) its original total, off by however far its
    actual child count deviates from the mean ratio (~0.3% here in the
    worst case); summed over the whole raster this has no systematic
    direction, so the aggregate (e.g. a country total) is unaffected.
    """
    with rasterio.Env(AWS_REQUEST_PAYER="requester"):
        src = rio.open_rasterio(cog_uri, chunks={"x": 10000, "y": 10000})
    if "band" in src.dims:
        src = src.isel(band=0, drop=True)

    children_per_row = abs(src.rio.resolution()[1]) / abs(geobox.resolution.y)
    children_per_col = abs(src.rio.resolution()[0]) / abs(geobox.resolution.x)
    per_child_value = src / (children_per_row * children_per_col)

    reprojected = xr_reproject(
        per_child_value,
        geobox,
        resampling="nearest",
        dst_nodata=0,
        chunks=(10000, 10000),
        always_yx=True,
    )
    if "band" in reprojected.dims:
        reprojected = reprojected.isel(band=0, drop=True)
    return reprojected


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

    cropland_kg = _resample_total_uniformly(CROPLAND_COG_URI, geobox)
    livestock_kg = _resample_total_uniformly(LIVESTOCK_COG_URI, geobox)

    cropland = cropland_kg / KG_PER_MG
    livestock = livestock_kg / KG_PER_MG

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
