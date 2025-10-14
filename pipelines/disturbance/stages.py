from datetime import date
from typing import Optional, Tuple

import pandas as pd
import xarray as xr
from dateutil.relativedelta import relativedelta

from pipelines.globals import (
    country_zarr_uri,
    pixel_area_uri,
    region_zarr_uri,
    subregion_zarr_uri,
)
from pipelines.prefect_flows.common_stages import (
    create_result_dataframe as common_create_result_dataframe,
)

ExpectedGroupsType = Tuple

alerts_confidence = {2: "low", 3: "high"}


def load_data(
    dist_zarr_uri: str,
    contextual_uri: Optional[str] = None,
) -> Tuple[xr.DataArray, ...]:
    """Load in the Dist alert Zarr, the GADM zarrs, and possibly a contextual layer zarr"""

    dist_alerts = _load_zarr(dist_zarr_uri)

    # reindex to dist alerts to avoid floating point precision issues
    # when aligning the datasets
    # https://github.com/pydata/xarray/issues/2217
    country = _load_zarr(country_zarr_uri).reindex_like(
        dist_alerts, method="nearest", tolerance=1e-5
    )
    country_aligned = xr.align(dist_alerts, country, join="left")[1].band_data
    region = _load_zarr(region_zarr_uri).reindex_like(
        dist_alerts, method="nearest", tolerance=1e-5
    )
    region_aligned = xr.align(dist_alerts, region, join="left")[1].band_data
    subregion = _load_zarr(subregion_zarr_uri).reindex_like(
        dist_alerts, method="nearest", tolerance=1e-5
    )
    subregion_aligned = xr.align(dist_alerts, subregion, join="left")[1].band_data
    pixel_area = _load_zarr(pixel_area_uri).reindex_like(
        dist_alerts, method="nearest", tolerance=1e-5
    )
    pixel_area_aligned = xr.align(dist_alerts, pixel_area, join="left")[1].band_data

    if contextual_uri is not None:
        contextual_layer = _load_zarr(contextual_uri).reindex_like(
            dist_alerts, method="nearest", tolerance=1e-5
        )
        contextual_layer_aligned = xr.align(dist_alerts, contextual_layer, join="left")[
            1
        ].band_data
    else:
        contextual_layer_aligned = None

    return (
        dist_alerts,
        country_aligned,
        region_aligned,
        subregion_aligned,
        pixel_area_aligned,
        contextual_layer_aligned,
    )


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
    return postprocess_result_dataframe(df)


def postprocess_result_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df.rename(columns={"value": "area_ha"}, inplace=True)
    df.rename(columns={"confidence": "dist_alert_confidence"}, inplace=True)
    df.rename(columns={"alert_date": "dist_alert_date"}, inplace=True)
    df["dist_alert_date"] = df.sort_values(by="dist_alert_date").dist_alert_date.apply(
        lambda x: date(2020, 12, 31) + relativedelta(days=x)
    )
    df["dist_alert_confidence"] = df.dist_alert_confidence.apply(
        lambda x: alerts_confidence[x]
    )

    # convert country/region/subregion to just AOI ID
    adm1_df = (
        df.drop(columns=["subregion"])
        .groupby(list(set(df.columns) - {"subregion", "area_ha"}))
        .sum()
        .reset_index()
    )
    iso_df = (
        adm1_df.drop(columns=["region"])
        .groupby(list(set(adm1_df.columns) - {"region", "area_ha"}))
        .sum()
        .reset_index()
    )

    # need to make copy to make these types of changes without warnings
    df = df.copy()
    adm1_df = adm1_df.copy()
    iso_df = iso_df.copy()

    df["aoi_id"] = (
        df[["country", "region", "subregion"]].astype(str).agg(".".join, axis=1)
    )
    adm1_df["aoi_id"] = adm1_df[["country", "region"]].astype(str).agg(".".join, axis=1)
    iso_df["aoi_id"] = iso_df["country"]

    adm2_df = df.drop(columns=["country", "region", "subregion"])
    adm1_df = adm1_df.drop(columns=["country", "region"])
    iso_df = iso_df.drop(columns=["country"])

    df = pd.concat([iso_df, adm1_df, adm2_df])
    return df


def _load_zarr(zarr_uri):
    return xr.open_zarr(zarr_uri)
