from datetime import date
from typing import Optional, Tuple

import numpy as np
import pandas as pd
import xarray as xr
from dateutil.relativedelta import relativedelta
from shapely.geometry import Polygon

from pipelines.globals import (
    country_10m_zarr_uri,
    pixel_area_10m_zarr_uri,
    region_10m_zarr_uri,
    subregion_10m_zarr_uri,
)
from pipelines.prefect_flows.common_stages import (
    clip_ds_to_bbox,
)
from pipelines.prefect_flows.common_stages import (
    create_result_dataframe as common_create_result_dataframe,
)
from pipelines.prefect_flows.common_stages import (
    rollup_by_gadm_and_convert_to_aoi,
)

ExpectedGroupsType = Tuple

alerts_confidence = {2: "low", 3: "high", 4: "highest"}


def load_data(
    zarr_uri: str,
    natural_lands_uri: str,
    bbox: Optional[Polygon] = None,
) -> Tuple[xr.DataArray, ...]:
    """Load in the alert Zarr, the GADM zarrs, and the natural lands zarr. If bbox
    is given, everything is clipped to it, since all other layers are reindexed
    to the (clipped) alerts."""

    alerts = clip_ds_to_bbox(_load_zarr(zarr_uri), bbox)

    # reindex to alerts to avoid floating point precision issues
    # when aligning the datasets
    # https://github.com/pydata/xarray/issues/2217.
    country = _load_zarr(country_10m_zarr_uri).reindex_like(
        alerts, method="nearest", tolerance=1e-5
    )
    country_aligned = xr.align(alerts, country, join="left")[1].band_data
    region = _load_zarr(region_10m_zarr_uri).reindex_like(
        alerts, method="nearest", tolerance=1e-5
    )
    region_aligned = xr.align(alerts, region, join="left")[1].band_data
    subregion = _load_zarr(subregion_10m_zarr_uri).reindex_like(
        alerts, method="nearest", tolerance=1e-5
    )
    subregion_aligned = xr.align(alerts, subregion, join="left")[1].band_data
    pixel_area = (
        _load_zarr(pixel_area_10m_zarr_uri)
        .reindex_like(alerts, method="nearest", tolerance=1e-5)
        .astype(np.float64)
    )
    pixel_area_aligned = xr.align(alerts, pixel_area, join="left")[1].band_data / 10000

    # The natural lands zarr is 30m, so resample it to the 10m alerts grid. All
    # its SBTN classes are kept, so the API can filter on any group of them.
    natural_lands_aligned = resample_to_alerts_grid(
        _load_zarr(natural_lands_uri).band_data, alerts
    )

    return (
        alerts,
        country_aligned,
        region_aligned,
        subregion_aligned,
        pixel_area_aligned,
        natural_lands_aligned,
    )


def resample_to_alerts_grid(layer: xr.DataArray, alerts) -> xr.DataArray:
    """Nearest-neighbor resample a coarser layer (e.g. 30m) to the 10m alerts grid.

    Each alerts pixel takes the value of the layer pixel its center falls in. The
    tolerance is half a layer pixel, since 10m pixel centers never coincide with
    30m pixel centers (so a tiny tolerance would match nothing). Alerts pixels
    outside the layer's extent are set to 0. The output takes on the alerts
    coords and, with dask, the layer's chunk size, so a 10000-chunk layer lines
    up with the 10000-chunk alerts zarr.

    Matching is done on the x/y coordinates in degrees, so both must be in
    EPSG:4326 (as all our zarrs are), and the layer's pixels must be square,
    since the tolerance is taken from its x step and applied to both axes.
    """
    check_square_pixels(layer, "layer")
    layer_resolution = abs(float(layer.x[1] - layer.x[0]))
    return layer.reindex_like(
        alerts, method="nearest", tolerance=layer_resolution / 2, fill_value=0
    ).astype(layer.dtype)


def check_square_pixels(grid, name: str) -> None:
    """Raise if grid's x and y pixel steps differ, since resampling takes its
    tolerance from the x step and applies it to both axes."""
    x_step = abs(float(grid.x[1] - grid.x[0]))
    y_step = abs(float(grid.y[1] - grid.y[0]))
    if not np.isclose(x_step, y_step, rtol=1e-6):
        raise ValueError(
            f"{name} must have square pixels, but its x step is {x_step} and "
            f"its y step is {y_step}"
        )


def setup_compute(
    datasets: Tuple[xr.DataArray, ...],
    expected_groups: Optional[ExpectedGroupsType],
) -> Tuple:
    """Setup the arguments for the xarray reduce on alerts"""
    (alerts, country, region, subregion, pixel_area, natural_lands) = datasets

    base_layer = pixel_area
    groupbys: Tuple[xr.DataArray, ...] = (
        country.rename("country"),
        region.rename("region"),
        subregion.rename("subregion"),
        natural_lands.rename("natural_lands_class"),
        alerts.alert_date,
        alerts.confidence,
    )

    return (base_layer, groupbys, expected_groups)


def create_result_dataframe(alerts_area: xr.DataArray) -> pd.DataFrame:
    df = common_create_result_dataframe(alerts_area)
    df.rename(columns={"value": "area_ha"}, inplace=True)
    df.rename(columns={"confidence": "alert_confidence"}, inplace=True)
    df.rename(columns={"alert_date": "alert_date"}, inplace=True)
    df["alert_date"] = df.sort_values(by="alert_date").alert_date.apply(
        lambda x: date(2014, 12, 31) + relativedelta(days=x)
    )
    df["alert_confidence"] = df.alert_confidence.apply(lambda x: alerts_confidence[x])
    # Keep the raw SBTN class codes (0-21). The API maps them to labels and to
    # groups like natural lands (classes 2-11).
    df["natural_lands_class"] = df.natural_lands_class.astype(np.uint8)
    df = rollup_by_gadm_and_convert_to_aoi(
        df, ["natural_lands_class", "alert_date", "alert_confidence"]
    )
    # Sort by aoi_id first, since API admin queries always filter by aoi_id, so
    # DuckDB can skip most of the parquet's row groups using their aoi_id min/max
    # statistics, and reads far less from S3.
    return df.sort_values(
        ["aoi_id", "natural_lands_class", "alert_confidence", "alert_date"],
        ignore_index=True,
    )


def _load_zarr(zarr_uri):
    return xr.open_zarr(zarr_uri, storage_options={"requester_pays": True})
