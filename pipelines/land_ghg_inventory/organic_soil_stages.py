"""Zonal-statistics stages for Land GHG inventory organic (peat) soil emissions.

Sums per-hectare burned + drained fluxes (converted to per-pixel totals by
multiplying by pixel area) grouped by admin unit x interval_end_year, then rolls
up to aoi_id. The reduce and GADM roll-up are reused from
``pipelines.prefect_flows.common_stages``.

``area_ha`` is restricted to organic-soil-extent pixels (the zarr's
``organic_soil`` mask), not every pixel in the admin unit -- burned/drained are
already zero outside that extent, so this only affects the area total, not the
emissions total.

Only the last two 5-year blocks (zarr ``year`` values 2020 and 2024) are used --
see ``pipelines.globals.land_ghg_inventory_organic_soil_zarr_uri``. These are
persisted at their native block resolution, not broadcast to vegetation's 9
annual years: ``interval_end_year=2020`` covers the 2016-2020 vegetation period,
``interval_end_year=2024`` covers the 2021-2024 vegetation period. A consumer
needing annual figures broadcasts each block's value across its corresponding
vegetation year range themselves.

Output parquet schema (one row per aoi_id x interval_end_year)::

    aoi_id                     str    admin unit, e.g. "BRA", "BRA.1", "BRA.1.1"
    aoi_type                   str    always "admin"
    interval_end_year          int    2020 (covers 2016-2020) | 2024 (covers 2021-2024)
    gross_emissions_MgCO2e     float  summed burned + drained emissions
    area_ha                    float  summed area, hectares
"""

from typing import Optional, Tuple

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

MEASURES = ["gross_emissions_MgCO2e"]
AREA_LAYER = "area_ha"
BLOCK_YEAR_INDICES = [3, 4]  # zarr year values 2020 (2016-2020), 2024 (2021-2024)

# per-hectare source variables summed together into the single emissions measure
ORGANIC_SOIL_SOURCE_VARS = ["burned_total_Mg_CO2e_ha_yr", "drained_total_Mg_CO2e_ha_yr"]
ORGANIC_SOIL_MASK_VAR = "organic_soil"


def setup_compute(
    emissions: xr.DataArray,
    organic_soil_mask: xr.DataArray,
    pixel_area: xr.DataArray,
    country: xr.DataArray,
    region: xr.DataArray,
    subregion: xr.DataArray,
    interval_end_year: xr.DataArray,
    expected_groups: Tuple,
) -> Tuple:
    """Build the per-pixel emissions cube + group-by layers for the reduce.

    ``emissions`` (burned + drained, per-hectare) is multiplied by ``pixel_area``
    (hectares) to get per-pixel totals and stacked with an ``area_ha`` layer along
    ``analysis_layer``. The ``area_ha`` layer is restricted to organic-soil-extent
    pixels via ``organic_soil_mask``. Grouping is admin x interval_end_year (the
    zarr's native 2020/2024 block labels, not vegetation calendar years).
    """
    pixel_area = pixel_area.fillna(0)
    layers = [
        (emissions.fillna(0) * pixel_area).astype("float64").rename(MEASURES[0]),
        (pixel_area * organic_soil_mask.fillna(0)).astype("float64").rename(AREA_LAYER),
    ]
    cube = xr.concat(layers, dim="analysis_layer").assign_coords(
        analysis_layer=MEASURES + [AREA_LAYER]
    )
    groupbys = (
        country.rename("country"),
        region.rename("region"),
        subregion.rename("subregion"),
        interval_end_year.rename("interval_end_year"),
    )
    return (cube, groupbys, expected_groups)


def create_result_dataframe(reduced: xr.DataArray) -> pd.DataFrame:
    """Reshape the sparse reduce into tidy aoi_id x interval_end_year rows."""
    df = common_create_result_dataframe(reduced)
    df = df.pivot_table(
        index=["country", "region", "subregion", "interval_end_year"],
        columns="analysis_layer",
        values="value",
        aggfunc="sum",
    ).reset_index()
    df.columns.name = None
    df["interval_end_year"] = df["interval_end_year"].astype(int)

    result = rollup_by_gadm_and_convert_to_aoi(df, ["interval_end_year"])
    # a structurally-zero measure is absent from the sparse reduce and pivots to
    # NaN; emit dense 0.0 so consumers aren't surprised.
    value_columns = MEASURES + [AREA_LAYER]
    result[value_columns] = result.reindex(columns=value_columns).fillna(0.0)
    return result


def load_data(
    organic_soil_uri: str,
    pixel_area_uri: str,
    country_uri: str,
    region_uri: str,
    subregion_uri: str,
    bbox: Optional[Polygon] = None,
) -> Tuple[xr.Dataset, xr.DataArray, xr.DataArray, xr.DataArray, xr.DataArray]:
    """Load the organic soil fluxes and extent mask at the 2020/2024 blocks, pixel
    area, and GADM layers, aligned to the organic soil grid (native 30m, so
    alignment is 1:1)."""
    org = _load_zarr(organic_soil_uri)[
        ORGANIC_SOIL_SOURCE_VARS + [ORGANIC_SOIL_MASK_VAR]
    ]
    org = org.isel(year=BLOCK_YEAR_INDICES)
    org = clip(org, bbox)
    return (
        org,
        align_to(org, pixel_area_uri),
        align_to(org, country_uri),
        align_to(org, region_uri),
        align_to(org, subregion_uri),
    )


def setup_organic_soil_compute(datasets: Tuple, expected_groups: Tuple) -> Tuple:
    org, pixel_area, country, region, subregion = datasets
    emissions = sum(org[var] for var in ORGANIC_SOIL_SOURCE_VARS)
    return setup_compute(
        emissions,
        org[ORGANIC_SOIL_MASK_VAR],
        pixel_area,
        country,
        region,
        subregion,
        org["year"].rename("interval_end_year"),
        expected_groups,
    )


def organic_soil_result_dataframe(reduced: xr.DataArray) -> pd.DataFrame:
    return create_result_dataframe(reduced)
