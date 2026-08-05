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
pixel's total is divided by its (unrounded) number of 30m reference-grid
children before an ordinary nearest-neighbor reprojection (``_resample_total_uniformly``),
so replication approximately splits rather than multiplies each parent's
total -- exact for no single pixel (the real child count alternates by +/-1
around the average), but unbiased in aggregate.
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
    """Downscale an absolute per-pixel total COG onto ``geobox`` by splitting
    each source pixel's total evenly across its destination children.

    Plain nearest-neighbor resampling (as ``_resample`` does) replicates a
    source pixel's value into every destination pixel that maps to it, which
    is only mass-conserving for rates (kg/ha), not for absolute totals (kg) --
    replicating an absolute total would multiply it by the number of
    destination pixels instead of splitting it among them.

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
