from typing import Dict

import dask.dataframe as dd
import numpy as np
from flox.xarray import xarray_reduce

from app.analysis.common.analysis import (
    JULIAN_DATE_2021,
    read_zarr_clipped_to_geojson,
)
from app.domain.models.environment import Environment

ALERTS_CONFIDENCE = {2: "low", 3: "high"}

# We want to move this into configuration, but will tolerate it being here for now.
# Note that it actually gets passed in to the constructor for easy moving later.
# Please DO NOT directly reference in constructor.
INPUT_URIS = {
    Environment.staging: {},
    Environment.production: {
        "integrated_alerts_zarr_uri": (
            "s3://lcl-analytics/zarr/gfw_integrated_dist_alerts/v20260601/"
            "date_conf.zarr"
        ),
        "pixel_area_zarr_uri": (
            "s3://gfw-data-lake/umd_area_2013/v1.10/raster/epsg-4326/zarr/"
            "area_m_10m_f32"
        ),
        "admin_results_table_uri": (
            "s3://lcl-analytics/zonal-statistics/intdist-alerts/v20260601/"
            "admin-intdist-alerts.parquet"
        ),
    },
}


def extract_params(integrated_alerts_in) -> Dict[str, str]:
    return {
        "start_date": full_date(integrated_alerts_in.start_date),
        "end_date": full_date(integrated_alerts_in.end_date, end=True),
    }


def build_admin_query(integrated_alerts_in) -> str:
    params = extract_params(integrated_alerts_in)
    # The precomputed parquet is keyed by country/region/subregion, so each
    # GADM id is aggregated to its admin level and labelled with its aoi_id.
    subqueries = [
        gadm_subquery(aoi_id, params["start_date"], params["end_date"])
        for aoi_id in integrated_alerts_in.aoi.ids
    ]
    return (
        " UNION ALL ".join(subqueries)
        + " ORDER BY aoi_id, alert_date, alert_confidence"
    )


def integrated_alerts_area(
    input_uris: Dict[str, str], aoi, geojson, start_date, end_date
) -> dd.DataFrame:
    # Sadly, this must be a module function (not a bound method) because Dask
    # can't serialize the compute_engine (a live Dask Task).
    alerts = read_zarr_clipped_to_geojson(
        input_uris["integrated_alerts_zarr_uri"], geojson
    )

    pixel_area = read_zarr_clipped_to_geojson(
        input_uris["pixel_area_zarr_uri"], geojson
    ).band_data.reindex_like(alerts, method="nearest", tolerance=1e-5)

    groupby_layers = [alerts.alert_date, alerts.confidence]
    expected_groups = [
        np.arange(5000),  # days since 2020-12-31 (epoch matches the GADM parquet)
        [2, 3],  # confidence: 2=low, 3=high
    ]

    alerts_area = xarray_reduce(
        pixel_area,
        *tuple(groupby_layers),
        func="sum",
        expected_groups=tuple(expected_groups),
    )

    df = (
        alerts_area.to_dask_dataframe()
        .drop("band", axis=1)
        .drop("spatial_ref", axis=1)
        .reset_index(drop=True)
        .rename(columns={"band_data": "area_ha", "confidence": "alert_confidence"})
    )

    df.alert_confidence = df.alert_confidence.map(
        ALERTS_CONFIDENCE, meta=("alert_confidence", "object")
    )
    df.alert_date = dd.to_datetime(
        df.alert_date + JULIAN_DATE_2021, origin="julian", unit="D"
    ).dt.strftime("%Y-%m-%d")

    df["aoi_type"] = aoi["type"].lower()
    df["aoi_id"] = aoi["id"] if "id" in aoi else aoi["properties"]["id"]

    df = df[
        (df.area_ha > 0) & (df.alert_date >= start_date) & (df.alert_date <= end_date)
    ]
    return df


def full_date(value: str, end: bool = False) -> str:
    """Normalise a year-only value (YYYY) to a full YYYY-MM-DD date."""
    if len(value) == 4:
        return f"{value}-12-31" if end else f"{value}-01-01"
    return value


def gadm_subquery(aoi_id: str, start_date: str, end_date: str) -> str:
    parts = aoi_id.split(".")
    filters = [f"country = '{parts[0]}'"]
    if len(parts) > 1:
        filters.append(f"region = {parts[1]}")
    if len(parts) > 2:
        filters.append(f"subregion = {parts[2]}")
    filters.append(
        f"intdist_alert_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
    )
    where_clause = " AND ".join(filters)
    return (
        f"SELECT '{aoi_id}' AS aoi_id, "
        "STRFTIME(intdist_alert_date, '%Y-%m-%d') AS alert_date, "
        "intdist_alert_confidence AS alert_confidence, "
        "SUM(area_ha)::FLOAT AS area_ha "
        "FROM data_source "
        f"WHERE {where_clause} "
        "GROUP BY intdist_alert_date, intdist_alert_confidence"
    )
