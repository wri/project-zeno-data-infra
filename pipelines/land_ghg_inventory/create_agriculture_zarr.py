"""Builds the agriculture emissions zarr consumed by ``agriculture_stages``.

Cropland emissions are published as a global COG on its own native grid (~10km,
WGS84), carrying a per-hectare rate in kg/ha. This resamples it onto the
vegetation zarr's 30m grid (the reference grid for the whole Land GHG inventory)
via ``odc.geo``'s dask-parallel nearest-neighbor reprojection, multiplies by the
UMD pixel-area zarr (already on the 30m grid, snapped on via
``common.align_to``) to recover an absolute per-pixel total, converts kg -> Mg,
and writes a single-variable zarr matching
``agriculture_stages.AGRICULTURE_SOURCE_VARS``.
"""

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

# Source COG: a static snapshot (single year, no versioning scheme), published by
# Cornell. Per-hectare rate in kg/ha.
CROPLAND_COG_URI = (
    "s3://gfw2-data/climate/AFOLU_flux_model/cropland_emissions/processed/"
    "Cornell_v20250828/year_2020/global_COG/all_sources/"
    "Global_grid_all_GHGs_cropland_mean_rate_physical_area_CO2eq_all_crops_"
    "NonPeatland_2019_kg_ha_CO2_COG.tif"
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


def create_agriculture_zarr(overwrite: bool = False) -> str:
    """Resample cropland emissions onto the vegetation grid, convert per-hectare
    rates to absolute per-pixel Mg totals, and write the zarr consumed by
    ``agriculture_stages.load_agriculture``."""
    marker_uri = (
        f"{land_ghg_inventory_agriculture_zarr_uri}/{AGRICULTURE_ZARR_GROUP}/zarr.json"
    )
    if not overwrite and s3_uri_exists(marker_uri):
        return land_ghg_inventory_agriculture_zarr_uri

    geobox = _reference_geobox()
    cropland_kg_ha = _resample(CROPLAND_COG_URI, geobox)
    pixel_area_ha = align_to(cropland_kg_ha, pixel_area_zarr_uri)
    cropland = (cropland_kg_ha * pixel_area_ha) / KG_PER_MG

    combined = xr.Dataset({AGRICULTURE_SOURCE_VARS["cropland"]: cropland})
    combined.to_zarr(
        land_ghg_inventory_agriculture_zarr_uri, group=AGRICULTURE_ZARR_GROUP, mode="w"
    )

    return land_ghg_inventory_agriculture_zarr_uri
