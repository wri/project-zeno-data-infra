"""Builds the agriculture emissions zarr consumed by ``agriculture_stages``.

Cropland and livestock emissions are published as global COGs on their own native
grids (~10km, WGS84), each carrying an absolute per-pixel total in kg. This
resamples both onto the vegetation zarr's 30m grid (the reference grid for the
whole Land GHG inventory) via ``odc.geo``'s dask-parallel reprojection, converts
kg -> Mg, and writes a single two-variable zarr matching
``agriculture_stages.AGRICULTURE_SOURCE_VARS``.
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

# Source COGs: static snapshots (single year, no versioning scheme), published by
# Cornell. Absolute per-pixel totals in kg.
CROPLAND_COG_URI = (
    "s3://gfw2-data/climate/AFOLU_flux_model/cropland_emissions/processed/"
    "Cornell_v20250828/year_2020/global_COG/all_sources/"
    "Global_grid_all_GHGs_cropland_total_amount_CO2eq_all_crops_NonPeatland_"
    "2019_kg_CO2_COG.tif"
)
LIVESTOCK_COG_URI = (
    "s3://gfw2-data/climate/AFOLU_flux_model/livestock_emissions/raw__from_Cornell/"
    "20251223/Total_GHG_Emissions/Tot_CO2eq_kg_livestock_GHG_emissions.tif"
)
KG_PER_MG = 1_000


def _reference_geobox():
    """The vegetation zarr's 30m grid, which the agriculture rasters resample to."""
    ref = xr.open_zarr(
        land_ghg_inventory_vegetation_zarr_uri,
        storage_options={"requester_pays": True},
    )[REFERENCE_GRID_VAR]
    ref = ref.isel(year=0, drop=True)
    ref.rio.write_crs("EPSG:4326", inplace=True)
    return ref.odc.geobox


def _resample(cog_uri: str, geobox) -> xr.DataArray:
    """Reproject one source COG (kg) onto ``geobox``, converting to Mg."""
    with rasterio.Env(AWS_REQUEST_PAYER="requester"):
        src = rio.open_rasterio(cog_uri, chunks={"x": 10000, "y": 10000})
    reprojected = xr_reproject(
        src,
        geobox,
        dst_nodata=0,
        chunks=(10000, 10000),
        always_yx=True,
    )
    if "band" in reprojected.dims:
        reprojected = reprojected.isel(band=0, drop=True)
    return reprojected / KG_PER_MG


def create_agriculture_zarr(overwrite: bool = False) -> str:
    """Resample cropland + livestock emissions onto the vegetation grid and write
    the combined zarr consumed by ``agriculture_stages.load_agriculture``."""
    marker_uri = (
        f"{land_ghg_inventory_agriculture_zarr_uri}/{AGRICULTURE_ZARR_GROUP}/zarr.json"
    )
    if not overwrite and s3_uri_exists(marker_uri):
        return land_ghg_inventory_agriculture_zarr_uri

    geobox = _reference_geobox()
    cropland = _resample(CROPLAND_COG_URI, geobox)
    livestock = _resample(LIVESTOCK_COG_URI, geobox)

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
