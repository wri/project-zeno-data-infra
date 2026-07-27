"""Zonal-statistics stages for Land GHG inventory agriculture emissions.

Sums already-absolute per-pixel cropland + livestock emissions grouped by admin
unit only (no land_state_class, no year — a single static snapshot), then rolls up
to aoi_id. The reduce and GADM roll-up are reused from
``pipelines.prefect_flows.common_stages``.
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

# canonical measure -> source variable in the agriculture zarr
AGRICULTURE_SOURCE_VARS = {
    "cropland_emissions": "cropland_emissions",
    "livestock_emissions": "livestock_emissions",
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

    Unlike vegetation, agriculture values are already per-pixel absolute totals (no
    pixel-area multiplication), and there is no land_state_class or year axis —
    grouping is admin-only (country x region x subregion).
    """
    ag, country, region, subregion = datasets
    layers = [
        ag[name].fillna(0).astype("float64").rename(name)
        for name in AGRICULTURE_SOURCE_VARS
    ]
    cube = xr.concat(layers, dim="analysis_layer").assign_coords(
        analysis_layer=list(AGRICULTURE_SOURCE_VARS)
    )
    groupbys = (
        country.rename("country"),
        region.rename("region"),
        subregion.rename("subregion"),
    )
    return (cube, groupbys, expected_groups)


def agriculture_result_dataframe(reduced: xr.DataArray) -> pd.DataFrame:
    """Reshape the sparse agriculture reduce into tidy aoi_id rows.

    Mirrors vegetation's ``create_result_dataframe`` and carbon_flux's pivot (see
    ``pipelines/carbon_flux/stages.py``): ``common_create_result_dataframe`` only
    supports a single ``DataArray`` with the measures stacked on ``analysis_layer``
    (not a multi-variable ``Dataset``), so they're pivoted back into columns here.
    """
    df = common_create_result_dataframe(reduced)
    df = df.pivot_table(
        index=["country", "region", "subregion"],
        columns="analysis_layer",
        values="value",
        aggfunc="sum",
    ).reset_index()
    df.columns.name = None

    result = rollup_by_gadm_and_convert_to_aoi(df, [])
    value_columns = list(AGRICULTURE_SOURCE_VARS)
    result[value_columns] = result.reindex(columns=value_columns).fillna(0.0)
    return result
