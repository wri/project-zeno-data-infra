"""Zonal-statistics stages for AFOLU flux (vegetation + soil).

Each component sums per-hectare fluxes (converted to per-pixel totals) grouped by
admin unit x category x year, then rolls up to aoi_id. The reduce and GADM roll-up
are reused from ``pipelines.prefect_flows.common_stages``; ``setup_compute`` and
``create_result_dataframe`` here are component-agnostic — vegetation and soil differ
only in the measures, category layer, and year handling passed in.
"""

from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import xarray as xr
from shapely.geometry import Polygon

from pipelines.afolu.land_state_categories import (
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
    flux_class: xr.DataArray,
    country: xr.DataArray,
    region: xr.DataArray,
    subregion: xr.DataArray,
    year: xr.DataArray,
    expected_groups: Tuple,
) -> Tuple:
    """Build the per-pixel flux cube + group-by layers for one carbon pool.

    ``measures`` maps canonical names to per-hectare DataArrays; each is converted
    to per-pixel totals and stacked with an ``area_ha`` layer along ``analysis_layer``.
    Grouping is admin x flux_class x year.
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
        flux_class.rename("flux_class"),
        year,
    )
    return (cube, groupbys, expected_groups)


def create_result_dataframe(
    reduced: xr.DataArray,
    class_names: Dict[int, str],
    carbon_pool: str,
    year_expansion: Optional[Dict[int, List[int]]] = None,
) -> pd.DataFrame:
    """Reshape the sparse reduce into tidy aoi_id rows for one carbon pool.

    ``class_names`` maps flux-class codes to labels (``"excluded"`` rows dropped).
    ``year_expansion`` maps each reduced year index to the calendar year(s) it
    represents; when ``None`` the index is a calendar offset from ``YEAR_BASE``
    (vegetation's annual axis). Soil passes an expansion that broadcasts interval
    rates onto the annual years.
    """
    df = common_create_result_dataframe(reduced)
    df = df.pivot_table(
        index=["country", "region", "subregion", "flux_class", "year"],
        columns="analysis_layer",
        values="value",
        aggfunc="sum",
    ).reset_index()
    df.columns.name = None

    df["flux_class"] = df["flux_class"].map(class_names)
    df = df[df["flux_class"] != "excluded"]
    df = _to_calendar_years(df, year_expansion)

    df = rollup_by_gadm_and_convert_to_aoi(df, ["flux_class", "year"])
    df["carbon_pool"] = carbon_pool
    return df


def _to_calendar_years(
    df: pd.DataFrame, year_expansion: Optional[Dict[int, List[int]]]
) -> pd.DataFrame:
    if year_expansion is None:
        df["year"] = df["year"].astype(int) + YEAR_BASE
        return df
    parts = []
    for period, calendar_years in year_expansion.items():
        period_rows = df[df["year"] == period]
        for calendar_year in calendar_years:
            rows = period_rows.copy()
            rows["year"] = calendar_year
            parts.append(rows)
    return pd.concat(parts, ignore_index=True)


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
        collapse_land_state(veg[LAND_STATE_VAR]),
        country,
        region,
        subregion,
        veg["year"],
        expected_groups,
    )


def vegetation_result_dataframe(reduced: xr.DataArray) -> pd.DataFrame:
    return create_result_dataframe(reduced, VEGETATION_CATEGORIES, "vegetation")


# --------------------------------------------------------------------------- #
# Soil component — mineral SOC (organic peat soil is a follow-up)
# --------------------------------------------------------------------------- #
SOIL_MINERAL_SOURCE_VARS = {
    "gross_emissions_MgCO2e": "SOC_loss__mineral_soil_extent__0-30cm_MgCO2_ha_yr",
    "gross_removals_MgCO2": "SOC_gain__mineral_soil_extent__0-30cm_MgCO2_ha_yr",
    "net_flux_MgCO2e": "SOC_net__mineral_soil_extent__0-30cm_MgCO2_ha_yr",
}
MINERAL_CATEGORY_CODE = 1
SOIL_CATEGORY_NAMES = {MINERAL_CATEGORY_CODE: "mineral"}
# Mineral SOC is a 5-year stock-change rate on an interval axis (index 0..4). The
# reference broadcasts SOC period index 3 (the 2020->2022 change rate) across every
# annual year, so we do the same. Validated against the reference (Brunei: exact).
MINERAL_YEAR_EXPANSION = {3: list(range(2016, 2025))}


def _constant_category(reference: xr.DataArray, code: int) -> xr.DataArray:
    """A category layer with a single code everywhere (soil has no per-pixel
    category split, unlike vegetation's land_state)."""
    return (xr.zeros_like(reference, dtype="uint8") + np.uint8(code)).rename("category")


def load_soil_mineral(
    soc_uri: str,
    pixel_area_uri: str,
    country_uri: str,
    region_uri: str,
    subregion_uri: str,
    bbox: Optional[Polygon] = None,
) -> Tuple[xr.Dataset, xr.DataArray, xr.DataArray, xr.DataArray, xr.DataArray]:
    """Load mineral SOC loss/gain/net + pixel area + GADM, aligned to the SOC grid."""
    soc = _load_zarr(soc_uri)[list(SOIL_MINERAL_SOURCE_VARS.values())]
    soc = _clip(soc, bbox)
    soc = soc.rename(
        {source: name for name, source in SOIL_MINERAL_SOURCE_VARS.items()}
    )
    return (
        soc,
        _align_to(soc, pixel_area_uri),
        _align_to(soc, country_uri),
        _align_to(soc, region_uri),
        _align_to(soc, subregion_uri),
    )


def setup_soil_mineral_compute(datasets: Tuple, expected_groups: Tuple) -> Tuple:
    soc, pixel_area, country, region, subregion = datasets
    return setup_compute(
        {name: soc[name] for name in MEASURES},
        pixel_area,
        _constant_category(pixel_area, MINERAL_CATEGORY_CODE),
        country,
        region,
        subregion,
        soc["year"],
        expected_groups,
    )


def soil_mineral_result_dataframe(reduced: xr.DataArray) -> pd.DataFrame:
    return create_result_dataframe(
        reduced, SOIL_CATEGORY_NAMES, "soil", MINERAL_YEAR_EXPANSION
    )
