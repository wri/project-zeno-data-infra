"""Builds the agriculture emissions zarr consumed by ``agriculture_stages``.

Both cropland and livestock emissions are published by Cornell as
already-absolute ``total_amount`` COGs (kg CO2e per ~10km pixel, no area
normalization) on their native grid. Each parent pixel's total is divided by
its (unrounded) number of 30m reference-grid children before an ordinary
nearest-neighbor reprojection (``_resample_total_uniformly``), so replication
approximately splits rather than multiplies each parent's total -- exact for
no single pixel (the real child count alternates by +/-1 around the average),
but unbiased in aggregate. This avoids needing the UMD pixel-area zarr:
per-hectare rate COGs (e.g. Cornell's ``mean_rate_physical_area`` for
cropland) are normalized by the physical area actually occupied within each
pixel, not by the pixel's full geographic area, so multiplying by the
UMD pixel-area zarr (full pixel area) overstates absolute totals wherever a
pixel isn't 100% covered by the source class, which is almost everywhere (QC
against an independent country-level reference table showed pipeline
cropland totals ~5.3x too high with that approach).
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

# Source COGs: static snapshots (single year, no versioning scheme), published
# by Cornell as absolute per-pixel totals (kg CO2e).
CROPLAND_COG_URI = "https://gfw2-data.s3.amazonaws.com/climate/AFOLU_flux_model/cropland_emissions/raw__from_Cornell/20250828/year_2020/rice/Amount/Global_grid_rice_CH4_IPCC2019_CO2eq_amount_kg_CO2.tif"
LIVESTOCK_COG_URI = (
    "s3://gfw2-data/climate/AFOLU_flux_model/livestock_emissions/"
    "raw__from_Cornell/20251223/Total_GHG_Emissions/"
    "Tot_CO2eq_kg_livestock_GHG_emissions.tif"
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

    # cropland and livestock are both already absolute per-pixel totals (kg);
    # split each source pixel's total evenly across its 30m children rather
    # than area-weighting.
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
