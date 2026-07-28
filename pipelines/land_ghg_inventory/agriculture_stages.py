"""Zonal-statistics stages for Land GHG inventory agriculture emissions.

Sums already-absolute per-pixel cropland + livestock emissions grouped by admin
unit x category (cropland / livestock) only (no land_state_class, no year — a
single static snapshot), then rolls up to aoi_id. The reduce and GADM roll-up are
reused from ``pipelines.prefect_flows.common_stages``.

Output parquet schema (one row per aoi_id x category)::

    aoi_id                    str    admin unit, e.g. "BRA", "BRA.1", "BRA.1.1"
    aoi_type                  str    always "admin"
    category                  str    "cropland" | "livestock"
    gross_emissions_MgCO2e    float  summed emissions for aoi_id x category
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

# canonical category -> source variable in the agriculture zarr
AGRICULTURE_SOURCE_VARS = {
    "cropland": "cropland_emissions",
    "livestock": "livestock_emissions",
}
AGRICULTURE_ZARR_GROUP = "pipeline"


def load_agriculture(
    agriculture_uri: str,
    country_uri: str,
    region_uri: str,
    subregion_uri: str,
    bbox: Optional[Polygon] = None,
) -> Tuple[xr.Dataset, xr.DataArray, xr.DataArray, xr.DataArray]:
    """Load the agriculture emissions (already per-pixel absolute totals, no year
    axis) and GADM layers, aligned to the agriculture grid."""
    ag = _load_zarr(agriculture_uri, group=AGRICULTURE_ZARR_GROUP)[
        list(AGRICULTURE_SOURCE_VARS.values())
    ]
    if "band" in ag.dims:
        ag = ag.isel(band=0, drop=True)
    ag = clip(ag, bbox)
    ag = ag.rename({source: name for name, source in AGRICULTURE_SOURCE_VARS.items()})
    return (
        ag,
        align_to(ag, country_uri),
        align_to(ag, region_uri),
        align_to(ag, subregion_uri),
    )


def setup_agriculture_compute(datasets: Tuple, expected_groups: Tuple) -> Tuple:
    """Build the agriculture measure cube + admin group-by layers for the reduce.

    Stacks the per-pixel absolute cropland/livestock totals into a ``category``
    dim; grouping is admin-only (country x region x subregion).
    """
    ag, country, region, subregion = datasets
    cube = ag.fillna(0).astype("float64").to_dataarray(dim="category")
    groupbys = (
        country.rename("country"),
        region.rename("region"),
        subregion.rename("subregion"),
    )
    return (cube, groupbys, expected_groups)


def agriculture_result_dataframe(reduced: xr.DataArray) -> pd.DataFrame:
    """Reshape the sparse agriculture reduce into tidy aoi_id x category rows.

    Cropland and livestock emissions stay as separate rows (one per admin unit
    x category) rather than columns, so consumers group/filter by ``category``.
    """
    df = common_create_result_dataframe(reduced)
    df = df.rename(columns={"value": "gross_emissions_MgCO2e"})

    result = rollup_by_gadm_and_convert_to_aoi(df, ["category"])
    value_columns = ["gross_emissions_MgCO2e"]
    result[value_columns] = result.reindex(columns=value_columns).fillna(0.0)
    return result
