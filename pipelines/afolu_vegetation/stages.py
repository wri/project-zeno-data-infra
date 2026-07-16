"""Zonal-statistics stages for AFOLU vegetation flux.

Sums per-hectare vegetation fluxes (gross emissions / gross removals / net flux),
converted to per-pixel totals, grouped by admin unit x vegetation category x year,
then rolls up to aoi_id. Mirrors the integrated-alerts pipeline; the flox reduce and
GADM roll-up are reused from pipelines.prefect_flows.common_stages.
"""

from typing import Optional, Tuple

import numpy as np
import xarray as xr
from shapely.geometry import Polygon

from pipelines.afolu_vegetation.land_state_categories import (
    LAND_STATE_TO_CATEGORY,
    VEGETATION_CATEGORIES,
)
from pipelines.prefect_flows.common_stages import (
    _load_zarr,
)
from pipelines.prefect_flows.common_stages import (
    create_result_dataframe as common_create_result_dataframe,
)
from pipelines.prefect_flows.common_stages import (
    rollup_by_gadm_and_convert_to_aoi,
)

# Canonical measure name -> per-hectare source variable in the veg mega-zarr.
FLUX_SOURCE_VARS = {
    "gross_emissions_MgCO2e": "gross_emissions__all_C_pools__all_gases__MgCO2e_ha_yr",
    "gross_removals_MgCO2": "gross_removals__all_C_pools__MgCO2_ha_yr",
    "net_flux_MgCO2e": "net_flux__all_C_pools__all_gases__MgCO2e_ha_yr",
}
MEASURES = list(FLUX_SOURCE_VARS)
AREA_LAYER = "area_ha"
LAND_STATE_VAR = "land_state_node"
YEAR_BASE = 2016  # veg-zarr year index 0..8 -> 2016..2024


def collapse_land_state(land_state: xr.DataArray) -> xr.DataArray:
    """Relabel land_state_node codes to vegetation category codes (0-4).

    Codes absent from the mapping collapse to 0 (excluded). Vectorised per dask
    block via a sorted-code lookup.
    """
    codes = np.array(sorted(LAND_STATE_TO_CATEGORY), dtype="int64")
    categories = np.array([LAND_STATE_TO_CATEGORY[c] for c in codes], dtype="uint8")

    def relabel(block):
        block = np.nan_to_num(block, nan=0).astype("int64")
        pos = np.clip(np.searchsorted(codes, block), 0, len(codes) - 1)
        return np.where(codes[pos] == block, categories[pos], 0).astype("uint8")

    return xr.apply_ufunc(
        relabel, land_state, dask="parallelized", output_dtypes=["uint8"]
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
    veg = _load_zarr(vegetation_uri)[list(FLUX_SOURCE_VARS.values()) + [LAND_STATE_VAR]]
    if bbox is not None:
        min_x, min_y, max_x, max_y = bbox.bounds
        veg = veg.sel(x=slice(min_x, max_x), y=slice(max_y, min_y))
    veg = veg.rename({source: name for name, source in FLUX_SOURCE_VARS.items()})

    def align(uri: str) -> xr.DataArray:
        layer = _load_zarr(uri).band_data
        if "band" in layer.dims:
            layer = layer.isel(band=0, drop=True)
        layer = layer.reindex_like(veg, method="nearest", tolerance=1e-4)
        return xr.align(veg, layer, join="left")[1]

    return (
        veg,
        align(pixel_area_uri),
        align(country_uri),
        align(region_uri),
        align(subregion_uri),
    )


def setup_compute(
    datasets: Tuple[xr.Dataset, xr.DataArray, xr.DataArray, xr.DataArray, xr.DataArray],
    expected_groups: Tuple,
) -> Tuple:
    """Build the per-pixel flux cube and the group-by layers for the reduce.

    The cube stacks the measures + area along an ``analysis_layer`` dim and keeps
    ``year``; grouping by admin + veg_category + year yields per-year results.
    """
    veg, pixel_area, country, region, subregion = datasets
    pixel_area = pixel_area.fillna(0)
    veg_category = collapse_land_state(veg[LAND_STATE_VAR])

    layers = [
        (veg[measure].fillna(0) * pixel_area).astype("float64").rename(measure)
        for measure in MEASURES
    ]
    layers.append(
        (pixel_area * xr.ones_like(veg[MEASURES[0]]))
        .astype("float64")
        .rename(AREA_LAYER)
    )
    cube = xr.concat(layers, dim="analysis_layer").assign_coords(
        analysis_layer=MEASURES + [AREA_LAYER]
    )

    groupbys = (
        country.rename("country"),
        region.rename("region"),
        subregion.rename("subregion"),
        veg_category.rename("veg_category"),
        veg["year"],
    )
    return (cube, groupbys, expected_groups)


def create_result_dataframe(reduced: xr.DataArray):
    """Reshape the sparse reduce into tidy aoi_id rows.

    Reuses the common sparse-extraction + GADM roll-up; pivots the analysis layers
    into columns and maps category codes / year indices to their labels.
    """
    df = common_create_result_dataframe(reduced)
    df = df.pivot_table(
        index=["country", "region", "subregion", "veg_category", "year"],
        columns="analysis_layer",
        values="value",
        aggfunc="sum",
    ).reset_index()
    df.columns.name = None

    df["veg_category"] = df["veg_category"].map(VEGETATION_CATEGORIES)
    df = df[df["veg_category"] != "excluded"]
    df["year"] = df["year"].astype(int) + YEAR_BASE

    return rollup_by_gadm_and_convert_to_aoi(df, ["veg_category", "year"])
