import functools
from datetime import date, timedelta
from typing import Dict, Optional, Tuple

import dask
import pandas as pd
import xarray as xr

from pipelines.globals import (
    country_zarr_uri,
    dist_driver_zarr_uri,
    grasslands_zarr_uri,
    land_cover_zarr_uri,
    pixel_area_zarr_uri,
    region_zarr_uri,
    sbtn_natural_lands_zarr_uri,
    subregion_zarr_uri,
)
from pipelines.prefect_flows.common_stages import (
    create_result_dataframe as common_create_result_dataframe,
)

ExpectedGroupsType = Tuple

alerts_confidence = {2: "low", 3: "high"}

# contextual column name -> (zarr uri, year to select or None)
CONTEXTUAL_LAYERS = {
    "natural_land_class": (sbtn_natural_lands_zarr_uri, None),
    "driver": (dist_driver_zarr_uri, None),
    "grasslands": (grasslands_zarr_uri, 2022),
    "land_cover": (land_cover_zarr_uri, 2024),
}


def load_data(dist_zarr_uri: str) -> Tuple[xr.DataArray, ...]:
    """Load the DIST alert zarr and the GADM and pixel area zarrs aligned to it."""
    dist_alerts = _load_zarr(dist_zarr_uri)
    return (
        dist_alerts,
        _align_to(dist_alerts, country_zarr_uri),
        _align_to(dist_alerts, region_zarr_uri),
        _align_to(dist_alerts, subregion_zarr_uri),
        _align_to(dist_alerts, pixel_area_zarr_uri),
    )


@functools.cache
def load_alert_pixels(dist_zarr_uri: str) -> Dict[str, xr.DataArray]:
    """Every DIST input at alert pixels only, as 1-D arrays persisted on the cluster.

    Pixels without an alert have no expected alert_date group, so they never
    contribute to a result. Dropping them up front reads and masks the grid once
    for all DIST outputs, instead of reducing the full grid once per output.
    """
    dist_alerts, country, region, subregion, pixel_area = load_data(dist_zarr_uri)
    layers = {
        "alert_date": dist_alerts.alert_date,
        "confidence": dist_alerts.confidence,
        "country": country,
        "region": region,
        "subregion": subregion,
        "pixel_area": pixel_area,
    }
    for name, (uri, year) in CONTEXTUAL_LAYERS.items():
        layer = _align_to(dist_alerts, uri)
        layers[name] = layer if year is None else layer.sel(year=year)

    grid = _as_grid(dist_alerts.alert_date)
    has_alert = (grid > 0).data
    pixels = {
        name: _as_grid(layer, like=grid).data[has_alert]
        for name, layer in layers.items()
    }
    (pixels,) = dask.persist(pixels)
    # Masking leaves chunk lengths unknown; flox needs them.
    return {
        name: xr.DataArray(arr.compute_chunk_sizes(), dims="pixel", name=name)
        for name, arr in pixels.items()
    }


def alert_pixel_inputs(
    dist_zarr_uri: str, contextual_name: Optional[str] = None
) -> Tuple:
    """load_data's tuple plus a contextual layer, over alert pixels only."""
    pixels = load_alert_pixels(dist_zarr_uri)
    dist_alerts = xr.Dataset(
        {"alert_date": pixels["alert_date"], "confidence": pixels["confidence"]}
    )
    return (
        dist_alerts,
        pixels["country"],
        pixels["region"],
        pixels["subregion"],
        pixels["pixel_area"],
        pixels[contextual_name] if contextual_name else None,
    )


def release_alert_pixels() -> None:
    load_alert_pixels.cache_clear()


def _align_to(dist_alerts: xr.Dataset, zarr_uri: str) -> xr.DataArray:
    # reindex to dist alerts to avoid floating point precision issues
    # when aligning the datasets
    # https://github.com/pydata/xarray/issues/2217
    layer = _load_zarr(zarr_uri).reindex_like(
        dist_alerts, method="nearest", tolerance=1e-5
    )
    return xr.align(dist_alerts, layer, join="left")[1].band_data


def _as_grid(layer: xr.DataArray, like: Optional[xr.DataArray] = None) -> xr.DataArray:
    """A 2-D (y, x) view; chunked like `like` so one mask selects the same pixels."""
    if "band" in layer.dims:
        layer = layer.squeeze("band", drop=True)
    layer = layer.transpose("y", "x")
    if like is None:
        return layer
    return layer.chunk({"y": like.chunks[0], "x": like.chunks[1]})


def setup_compute(
    datasets: Tuple[xr.DataArray, ...],
    expected_groups: Optional[ExpectedGroupsType],
    contextual_column_name: Optional[str] = None,
) -> Tuple:
    """Setup the arguments for the xarray reduce on dist alerts"""
    dist_alerts, country, region, subregion, pixel_area, contextual_layer = datasets

    base_layer = pixel_area
    groupbys: Tuple[xr.DataArray, ...] = (
        country.rename("country"),
        region.rename("region"),
        subregion.rename("subregion"),
        dist_alerts.alert_date,
        dist_alerts.confidence,
    )
    if contextual_layer is not None:
        groupbys = (
            groupbys[:3]
            + (contextual_layer.rename(contextual_column_name),)
            + groupbys[3:]
        )

    return (base_layer, groupbys, expected_groups)


def create_result_dataframe(alerts_area: xr.DataArray) -> pd.DataFrame:
    df = common_create_result_dataframe(alerts_area)
    df.rename(columns={"value": "area_ha"}, inplace=True)
    df.rename(columns={"confidence": "dist_alert_confidence"}, inplace=True)
    df.rename(columns={"alert_date": "dist_alert_date"}, inplace=True)
    # Convert distinct values once; row-wise .apply is very slow at this row count.
    day_to_date = {
        d: date(2020, 12, 31) + timedelta(days=int(d))
        for d in df["dist_alert_date"].unique()
    }
    df["dist_alert_date"] = df["dist_alert_date"].map(day_to_date)
    confidence_labels = {
        c: alerts_confidence[c] for c in df["dist_alert_confidence"].unique()
    }
    df["dist_alert_confidence"] = df["dist_alert_confidence"].map(confidence_labels)
    return df


def _load_zarr(zarr_uri):
    return xr.open_zarr(zarr_uri)
