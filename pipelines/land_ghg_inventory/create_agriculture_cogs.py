"""Writes per-hectare agriculture emission COGs for map visualization.

- Converts units from kgCO2e/ha to MgCO2e/ha for livestock
- Converts units from kgCO2e/px to MgCO2e/ha for livestock
- Unlike ``create_agriculture_zarr``, this keeps the 10km pixels

Unlike ``create_agriculture_zarr`` -- which resamples absolute per-pixel
totals onto the vegetation 30m grid for zonal stats -- these COGs stay on each
source's native grid and carry MgCO2e/ha values for display. Zeros become
nodata so empty ocean/desert renders as no-data rather than a true zero.
"""

import numpy as np
import rasterio
import rioxarray as rio  # noqa: F401  registers the .rio xarray accessor
import xarray as xr

from pipelines.land_ghg_inventory.create_agriculture_zarr import (
    CROPLAND_COG_URI,
    KG_PER_MG,
)
from pipelines.utils import s3_uri_exists

# note: this is different from LIVESTOCK_COG_URI in create_agriculture_zarr
LIVESTOCK_PER_HA_COG_URI = (
    "s3://gfw-data-lake/wri_land_ghg_monitoring_system/v1.0.3/raw_data/"
    "Total_GHG_kg_CO2e_ha_yr_AllAnimals.tif"
)
AGRICULTURE_PREFIX = "s3://lcl-cogs/lgms"
EARTH_RADIUS_M = 6_371_008.8

CROPLAND_COG_NAME = "cropland_per_ha.tif"
LIVESTOCK_COG_NAME = "livestock_per_ha.tif"


def _cell_area_ha(lats, dlat, dlon):
    """Area (ha) of each cell centred on ``lats``, for a spherical Earth.

    Exact enough for display (WGS84 differs by <0.6%); depends only on latitude.
    """
    north, south = np.radians(lats + dlat / 2), np.radians(lats - dlat / 2)
    area_m2 = EARTH_RADIUS_M**2 * np.radians(dlon) * (np.sin(north) - np.sin(south))
    return np.abs(area_m2) / 1e4


def _agriculture_cog(source_uri: str, out_name: str, per_hectare_source: bool) -> str:
    """Write one agriculture COG in MgCO2e/ha on the source's native grid.

    ``per_hectare_source=True`` for livestock (kg/ha), False for cropland
    (absolute kg, divided by per-cell area to get a rate). Zeros become nodata
    so empty ocean/desert renders as no-data rather than a true zero.
    """
    with rasterio.Env(AWS_REQUEST_PAYER="requester"):
        src = (
            rio.open_rasterio(source_uri, masked=True)
            .squeeze(drop=True) # type: ignore[union-attr]
            .astype("float64")
        )

    values = src / KG_PER_MG
    if not per_hectare_source:
        dlat = abs(float(src.y[1] - src.y[0]))
        dlon = abs(float(src.x[1] - src.x[0]))
        area = xr.DataArray(
            _cell_area_ha(src.y.values, dlat, dlon),
            dims="y",
            coords={"y": src.y},
        )
        values = values / area

    values = values.where(np.isfinite(values) & (values != 0)).astype("float32")
    values = values.rio.write_crs("EPSG:4326").rio.write_nodata(float("nan"))

    out = f"{AGRICULTURE_PREFIX}/{out_name}"
    values.rio.to_raster(
        out,
        driver="COG",
        compress="deflate",
        predictor=3,
        blocksize=512,
        overview_resampling="average",
    )
    valid = values.values[np.isfinite(values.values)]
    print(
        f"{out_name:28s} {values.shape} valid={valid.size / values.size:6.1%} "
        f"max={valid.max():.4g} sum={valid.sum():.4g} MgCO2e/ha -> {out}"
    )
    return out


def create_agriculture_cogs(overwrite: bool = False) -> tuple[str, str]:
    """Write the cropland and livestock per-hectare visualization COGs.

    Returns ``(cropland_uri, livestock_uri)``. Skips work when both already
    exist unless ``overwrite`` is set.
    """
    cropland_uri = f"{AGRICULTURE_PREFIX}/{CROPLAND_COG_NAME}"
    livestock_uri = f"{AGRICULTURE_PREFIX}/{LIVESTOCK_COG_NAME}"
    if not overwrite and s3_uri_exists(cropland_uri) and s3_uri_exists(livestock_uri):
        return cropland_uri, livestock_uri

    livestock_uri = _agriculture_cog(
        LIVESTOCK_PER_HA_COG_URI, LIVESTOCK_COG_NAME, per_hectare_source=True
    )
    cropland_uri = _agriculture_cog(
        CROPLAND_COG_URI, CROPLAND_COG_NAME, per_hectare_source=False
    )
    return cropland_uri, livestock_uri
