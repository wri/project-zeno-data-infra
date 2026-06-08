from datetime import date
from typing import Optional, Tuple
from shapely.geometry import mapping, shape
from shapely import wkb

import pandas as pd
import xarray as xr
from dateutil.relativedelta import relativedelta

from pipelines.globals import (
    country_10m_zarr_uri,
    pixel_area_10m_zarr_uri,
    region_10m_zarr_uri,
    subregion_10m_zarr_uri,
)
from pipelines.prefect_flows.common_stages import (
    create_result_dataframe as common_create_result_dataframe,
)

ExpectedGroupsType = Tuple

alerts_confidence = {2: "low", 3: "high"}


def wkb_to_geojson_feature(wkbstr: str) -> dict:
    geom = wkb.loads(bytes.fromhex(wkbstr))
    subobj = mapping(geom)
    return subobj


def clip_ds_to_geojson(ds, geojson: dict):
    geom = shape(geojson)
    sliced = ds.sel(
        x=slice(geom.bounds[0], geom.bounds[2]),
        y=slice(geom.bounds[3], geom.bounds[1]),
    ).squeeze("band")
    clipped = sliced.rio.clip([geom])
    return clipped


def load_data(
    zarr_uri: str,
    contextual_uri: Optional[str] = None,
) -> Tuple[xr.DataArray, ...]:
    """Load in the alert Zarr, the GADM zarrs, and possibly a contextual layer zarr"""

    wkbstr = "01030000000100000005000000e7aed1e4156e5840d583f1ef0d6706405123d124216e5840bb5dcfdae43506403dbcd0243d70584057ee7202b43806406a50d1e404705840f75140ece2700640e7aed1e4156e5840d583f1ef0d670640"
    # wkbstr = "01030000000100000005000000fa7637c15bb94ec0e082b62ea03811c0fa7637c15bb94ec010b38ccd0d7a19c070b278d981f94cc010b38ccd0d7a19c070b278d981f94cc0e082b62ea03811c0fa7637c15bb94ec0e082b62ea03811c0"
    geojson = wkb_to_geojson_feature(wkbstr)

    alerts = _load_zarr(zarr_uri)
    spatial_chunks = {"x": alerts.chunksizes["x"], "y": alerts.chunksizes["y"]}

    # reindex to alerts to avoid floating point precision issues
    # when aligning the datasets
    # https://github.com/pydata/xarray/issues/2217.  This also converts the 30m GADM zarrs to 10m.
    country = _load_zarr(country_10m_zarr_uri).reindex_like(
        alerts, method="nearest", tolerance=1e-5
    ).chunk(spatial_chunks)
    country_aligned = xr.align(alerts, country, join="left")[1].band_data
    region = _load_zarr(region_10m_zarr_uri).reindex_like(
        alerts, method="nearest", tolerance=1e-5
    ).chunk(spatial_chunks)
    region_aligned = xr.align(alerts, region, join="left")[1].band_data
    subregion = _load_zarr(subregion_10m_zarr_uri).reindex_like(
        alerts, method="nearest", tolerance=1e-5
    ).chunk(spatial_chunks)
    subregion_aligned = xr.align(alerts, subregion, join="left")[1].band_data
    pixel_area = _load_zarr(pixel_area_10m_zarr_uri).reindex_like(
        alerts, method="nearest", tolerance=1e-5
    ).chunk(spatial_chunks)
    pixel_area = pixel_area.rio.write_crs("EPSG:4326", inplace=True)
    pixel_area_aligned = xr.align(alerts, pixel_area, join="left")[1].band_data
    pixel_area_aligned = pixel_area_aligned / 9.0

    alerts = clip_ds_to_geojson(alerts, geojson)
    country_aligned = clip_ds_to_geojson(country_aligned, geojson)
    region_aligned = clip_ds_to_geojson(region_aligned, geojson)
    subregion_aligned = clip_ds_to_geojson(subregion_aligned, geojson)
    pixel_area_aligned = clip_ds_to_geojson(pixel_area_aligned, geojson)

    if contextual_uri is not None:
        contextual_layer = _load_zarr(contextual_uri).reindex_like(
            alerts, method="nearest", tolerance=1e-5
        )
        contextual_layer_aligned = xr.align(alerts, contextual_layer, join="left")[
            1
        ].band_data
    else:
        contextual_layer_aligned = None

    return (
        alerts,
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
    """Setup the arguments for the xarray reduce on alerts"""
    (alerts, country,
     region, subregion,
     pixel_area, contextual_layer) = datasets

    base_layer = pixel_area
    groupbys: Tuple[xr.DataArray, ...] = (
        country.rename("country"),
        region.rename("region"),
        subregion.rename("subregion"),
        alerts.alert_date,
        alerts.confidence,
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
    df.rename(columns={"confidence": "intdist_alert_confidence"}, inplace=True)
    df.rename(columns={"alert_date": "intdist_alert_date"}, inplace=True)
    df["intdist_alert_date"] = df.sort_values(by="intdist_alert_date").intdist_alert_date.apply(
        lambda x: date(2020, 12, 31) + relativedelta(days=x)
    )
    df["intdist_alert_confidence"] = df.intdist_alert_confidence.apply(
        lambda x: alerts_confidence[x]
    )
    return df


def _load_zarr(zarr_uri):
    return xr.open_zarr(zarr_uri, storage_options={"requester_pays": True})
