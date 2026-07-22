"""Zonal-statistics stages for Land GHG inventory vegetation flux.

Sums per-hectare fluxes (converted to per-pixel totals by multiplying by pixel area)
grouped by admin unit x land_state x year, then rolls up to aoi_id. Output keeps the
raw land_state (all 95 codes); consolidation into broader classes is done at query
time. The reduce and GADM roll-up are reused from
``pipelines.prefect_flows.common_stages``. Soil is a separate pipeline.
"""

from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd
import xarray as xr
from shapely.geometry import Polygon

from pipelines.land_ghg_inventory.land_state_categories import LAND_STATE_ATTRIBUTES
from pipelines.prefect_flows.common_stages import (
    _load_zarr,
)
from pipelines.prefect_flows.common_stages import (
    create_result_dataframe as common_create_result_dataframe,
)
from pipelines.prefect_flows.common_stages import (
    rollup_by_gadm_and_convert_to_aoi,
)

# Canonical measures shared by every component.
MEASURES = ["gross_emissions_MgCO2e", "gross_removals_MgCO2", "net_flux_MgCO2e"]
AREA_LAYER = "area_ha"
YEAR_BASE = 2016  # annual year index 0..8 -> 2016..2024

# Vegetation: canonical measure -> per-hectare source variable in the veg zarr.
VEGETATION_SOURCE_VARS = {
    "gross_emissions_MgCO2e": "gross_emissions__all_C_pools__all_gases__MgCO2e_ha_yr",
    "gross_removals_MgCO2": "gross_removals__all_C_pools__MgCO2_ha_yr",
    "net_flux_MgCO2e": "net_flux__all_C_pools__all_gases__MgCO2e_ha_yr",
}
LAND_STATE_VAR = "land_state_node"


# --------------------------------------------------------------------------- #
# Generic core (used by every component)
# --------------------------------------------------------------------------- #
def setup_compute(
    measures: Dict[str, xr.DataArray],
    pixel_area: xr.DataArray,
    land_state: xr.DataArray,
    country: xr.DataArray,
    region: xr.DataArray,
    subregion: xr.DataArray,
    year: xr.DataArray,
    expected_groups: Tuple,
) -> Tuple:
    """Build the per-pixel flux cube + group-by layers for the reduce.

    The zarr fluxes are stored per-hectare, so ``measures`` (canonical name ->
    per-hectare DataArray) are each multiplied by ``pixel_area`` (hectares) to get
    per-pixel totals and stacked with an ``area_ha`` layer along ``analysis_layer``.
    Grouping is admin x land_state x year.
    """
    pixel_area = pixel_area.fillna(0)
    layers = [
        (measures[name].fillna(0) * pixel_area).astype("float64").rename(name)
        for name in measures
    ]
    layers.append(
        (pixel_area * xr.ones_like(measures[MEASURES[0]]))
        .astype("float64")
        .rename(AREA_LAYER)
    )
    cube = xr.concat(layers, dim="analysis_layer").assign_coords(
        analysis_layer=list(measures) + [AREA_LAYER]
    )
    groupbys = (
        country.rename("country"),
        region.rename("region"),
        subregion.rename("subregion"),
        land_state.rename("land_state"),
        year,
    )
    return (cube, groupbys, expected_groups)


def create_result_dataframe(reduced: xr.DataArray) -> pd.DataFrame:
    """Reshape the sparse reduce into tidy aoi_id rows keyed by raw land_state.

    All land_state codes are kept (consolidation is a query-time concern); the
    reduced year index is a calendar offset from ``YEAR_BASE``. The lookup's
    descriptive columns are joined on afterward.
    """
    df = common_create_result_dataframe(reduced)
    df = df.pivot_table(
        index=["country", "region", "subregion", "land_state", "year"],
        columns="analysis_layer",
        values="value",
        aggfunc="sum",
    ).reset_index()
    df.columns.name = None

    df["year"] = df["year"].astype(int) + YEAR_BASE
    df = rollup_by_gadm_and_convert_to_aoi(df, ["land_state", "year"])
    return _join_land_state_attributes(df)


def _join_land_state_attributes(df: pd.DataFrame) -> pd.DataFrame:
    """Attach each land_state's meaning/broad/detailed/tall-veg-type from the lookup."""
    attributes = pd.DataFrame(
        [(code, *attrs) for code, attrs in LAND_STATE_ATTRIBUTES.items()],
        columns=[
            "land_state",
            "land_state_meaning",
            "land_state_broad_class",
            "land_state_detailed_class",
            "tall_veg_type",
        ],
    )
    return df.merge(attributes, on="land_state", how="left")


def _align_to(reference: xr.Dataset, uri: str) -> xr.DataArray:
    """Load a contextual band_data zarr and snap it to the reference grid."""
    layer = _load_zarr(uri).band_data
    if "band" in layer.dims:
        layer = layer.isel(band=0, drop=True)
    layer = layer.reindex_like(reference, method="nearest", tolerance=1e-4)
    return xr.align(reference, layer, join="left")[1]


def _clip(dataset: xr.Dataset, bbox: Optional[Polygon]) -> xr.Dataset:
    if bbox is None:
        return dataset
    min_x, min_y, max_x, max_y = bbox.bounds
    return dataset.sel(x=slice(min_x, max_x), y=slice(max_y, min_y))


# --------------------------------------------------------------------------- #
# Vegetation component
# --------------------------------------------------------------------------- #
def _clean_land_state(land_state: xr.DataArray) -> xr.DataArray:
    """Cast the raw land_state_node to a clean integer grouper. Off-extent NaN
    becomes 0, which is absent from ``expected_groups`` and so dropped by the reduce.
    """

    def to_int(block):
        return np.nan_to_num(block, nan=0).astype("int64")

    return xr.apply_ufunc(
        to_int, land_state, dask="parallelized", output_dtypes=["int64"]
    )


def load_data(
    vegetation_uri: str,
    pixel_area_uri: str,
    country_uri: str,
    region_uri: str,
    subregion_uri: str,
    bbox: Optional[Polygon] = None,
) -> Tuple[xr.Dataset, xr.DataArray, xr.DataArray, xr.DataArray, xr.DataArray]:
    """Load the veg fluxes + land_state, pixel area, and GADM layers, aligned to
    the veg grid (all native 30m, so alignment is 1:1)."""
    veg = _load_zarr(vegetation_uri)[
        list(VEGETATION_SOURCE_VARS.values()) + [LAND_STATE_VAR]
    ]
    veg = _clip(veg, bbox)
    veg = veg.rename({source: name for name, source in VEGETATION_SOURCE_VARS.items()})
    return (
        veg,
        _align_to(veg, pixel_area_uri),
        _align_to(veg, country_uri),
        _align_to(veg, region_uri),
        _align_to(veg, subregion_uri),
    )


def setup_vegetation_compute(datasets: Tuple, expected_groups: Tuple) -> Tuple:
    veg, pixel_area, country, region, subregion = datasets
    return setup_compute(
        {name: veg[name] for name in MEASURES},
        pixel_area,
        _clean_land_state(veg[LAND_STATE_VAR]),
        country,
        region,
        subregion,
        veg["year"],
        expected_groups,
    )


def vegetation_result_dataframe(reduced: xr.DataArray) -> pd.DataFrame:
    return create_result_dataframe(reduced)
