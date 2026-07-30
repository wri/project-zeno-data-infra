"""Zonal-statistics stages for Land GHG inventory mineral soil organic carbon (SOC).

Sums per-hectare SOC stock-change fluxes (converted to per-pixel totals by
multiplying by pixel area) grouped by admin unit only, then rolls up to aoi_id.
The reduce and GADM roll-up are reused from ``pipelines.prefect_flows.common_stages``.

Only the 2015-2020 change interval (zarr ``year`` index 3) is used -- see
``pipelines.globals.land_ghg_inventory_soc_zarr_uri`` -- so there is no year axis;
this is a single static snapshot, applied uniformly across all vegetation years.

Output parquet schema (one row per aoi_id)::

    aoi_id                     str    admin unit, e.g. "BRA", "BRA.1", "BRA.1.1"
    aoi_type                   str    always "admin"
    gross_emissions_MgCO2e     float  summed gross emissions (SOC loss)
    gross_removals_MgCO2       float  summed gross removals (SOC gain)
    net_flux_MgCO2e            float  summed net flux (emissions - removals)
    area_ha                    float  summed area, hectares
"""

from typing import Dict, Optional, Tuple

import pandas as pd
import xarray as xr
from shapely.geometry import Polygon

from pipelines.land_ghg_inventory.common import align_to, clip
from pipelines.prefect_flows.common_stages import (
    _load_zarr,
)
from pipelines.prefect_flows.common_stages import (
    create_result_dataframe as common_create_result_dataframe,
)
from pipelines.prefect_flows.common_stages import (
    rollup_by_gadm_and_convert_to_aoi,
)

MEASURES = ["gross_emissions_MgCO2e", "gross_removals_MgCO2", "net_flux_MgCO2e"]
AREA_LAYER = "area_ha"
CHANGE_INTERVAL_INDEX = 3  # the 2015-2020 change interval; index 4 is distrusted

# canonical measure -> per-hectare source variable in the SOC zarr
SOC_SOURCE_VARS = {
    "gross_emissions_MgCO2e": "SOC_loss__mineral_soil_extent__0-30cm_MgCO2_ha_yr",
    "gross_removals_MgCO2": "SOC_gain__mineral_soil_extent__0-30cm_MgCO2_ha_yr",
    "net_flux_MgCO2e": "SOC_net__mineral_soil_extent__0-30cm_MgCO2_ha_yr",
}


def setup_compute(
    measures: Dict[str, xr.DataArray],
    pixel_area: xr.DataArray,
    country: xr.DataArray,
    region: xr.DataArray,
    subregion: xr.DataArray,
    expected_groups: Tuple,
) -> Tuple:
    """Build the per-pixel flux cube + group-by layers for the reduce.

    The zarr fluxes are stored per-hectare, so ``measures`` (canonical name ->
    per-hectare DataArray) are each multiplied by ``pixel_area`` (hectares) to get
    per-pixel totals and stacked with an ``area_ha`` layer along ``analysis_layer``.
    Grouping is admin-only (no year, no categorical axis).
    """
    pixel_area = pixel_area.fillna(0)
    layers = [
        (measures[name].fillna(0) * pixel_area).astype("float64").rename(name)
        for name in measures
    ]
    layers.append(pixel_area.astype("float64").rename(AREA_LAYER))
    cube = xr.concat(layers, dim="analysis_layer").assign_coords(
        analysis_layer=list(measures) + [AREA_LAYER]
    )
    groupbys = (
        country.rename("country"),
        region.rename("region"),
        subregion.rename("subregion"),
    )
    return (cube, groupbys, expected_groups)


def create_result_dataframe(reduced: xr.DataArray) -> pd.DataFrame:
    """Reshape the sparse reduce into tidy aoi_id rows (no year/category axis)."""
    df = common_create_result_dataframe(reduced)
    df = df.pivot_table(
        index=["country", "region", "subregion"],
        columns="analysis_layer",
        values="value",
        aggfunc="sum",
    ).reset_index()
    df.columns.name = None

    result = rollup_by_gadm_and_convert_to_aoi(df, [])
    # a structurally-zero measure is absent from the sparse reduce and pivots to
    # NaN; emit dense 0.0 so consumers aren't surprised.
    value_columns = MEASURES + [AREA_LAYER]
    result[value_columns] = result.reindex(columns=value_columns).fillna(0.0)
    return result


def load_data(
    soc_uri: str,
    pixel_area_uri: str,
    country_uri: str,
    region_uri: str,
    subregion_uri: str,
    bbox: Optional[Polygon] = None,
) -> Tuple[xr.Dataset, xr.DataArray, xr.DataArray, xr.DataArray, xr.DataArray]:
    """Load the SOC fluxes at the 2015-2020 change interval, pixel area, and GADM
    layers, aligned to the SOC grid (native 30m, so alignment is 1:1)."""
    soc = _load_zarr(soc_uri)[list(SOC_SOURCE_VARS.values())]
    soc = soc.isel(year=CHANGE_INTERVAL_INDEX, drop=True)
    soc = clip(soc, bbox)
    soc = soc.rename({source: name for name, source in SOC_SOURCE_VARS.items()})
    return (
        soc,
        align_to(soc, pixel_area_uri),
        align_to(soc, country_uri),
        align_to(soc, region_uri),
        align_to(soc, subregion_uri),
    )


def setup_mineral_soil_compute(datasets: Tuple, expected_groups: Tuple) -> Tuple:
    soc, pixel_area, country, region, subregion = datasets
    return setup_compute(
        {name: soc[name] for name in MEASURES},
        pixel_area,
        country,
        region,
        subregion,
        expected_groups,
    )


def mineral_soil_result_dataframe(reduced: xr.DataArray) -> pd.DataFrame:
    return create_result_dataframe(reduced)
