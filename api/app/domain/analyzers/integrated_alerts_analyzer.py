from functools import partial
from typing import Any, Dict, List

import dask.dataframe as dd
import newrelic.agent as nr_agent
import numpy as np
from flox.xarray import xarray_reduce

from app.analysis.common.analysis import (
    JULIAN_DATE_2021,
    get_geojson,
    read_zarr_clipped_to_geojson,
)
from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis
from app.domain.models.environment import Environment
from app.models.land_change.integrated_alerts import IntegratedAlertsAnalyticsIn

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


class IntegratedAlertsAnalyzer(Analyzer):
    """Get integrated disturbance alert areas by date and confidence for the AOIs"""

    def __init__(
        self,
        compute_engine=None,
        duckdb_query_service=None,
        input_uris: Dict[str, str] | None = None,
    ):
        self.compute_engine = compute_engine  # Dask Client, or not?
        self.duckdb_query_service = duckdb_query_service
        self.input_uris = input_uris

    @nr_agent.function_trace(name="IntegratedAlertsAnalyzer.analyze")
    async def analyze(self, analysis: Analysis) -> None:
        if self.input_uris is None:
            raise Exception("Input URIs must be provided for actual analysis")

        integrated_alerts_in = IntegratedAlertsAnalyticsIn(**analysis.metadata)
        start_date = full_date(integrated_alerts_in.start_date)
        end_date = full_date(integrated_alerts_in.end_date, end=True)

        if integrated_alerts_in.aoi.type == "admin":
            gadm_ids: List = integrated_alerts_in.aoi.ids
            results: Dict = await self.analyze_admin_areas(
                gadm_ids, start_date, end_date
            )
        else:
            aois = integrated_alerts_in.aoi.model_dump()
            geojsons = await get_geojson(aois)
            if aois["type"] != "feature_collection":
                aoi_list = sorted(
                    [{"type": aois["type"], "id": id} for id in aois["ids"]],
                    key=lambda aoi: aoi["id"],
                )
            else:
                aoi_list = aois["feature_collection"]["features"]
                geojsons = [geojson["geometry"] for geojson in geojsons]

            analysis_partial = partial(
                self.analyze_area,
                self.input_uris,
                start_date=start_date,
                end_date=end_date,
            )
            dd_df_futures = await self.compute_engine.gather(
                self.compute_engine.map(analysis_partial, aoi_list, geojsons)
            )
            dfs = await self.compute_engine.gather(dd_df_futures)
            combined_results_df = await self.compute_engine.compute(dd.concat(dfs))
            results = combined_results_df.to_dict(orient="list")

        analysis.result = results

    async def analyze_admin_areas(
        self, gadm_ids, start_date, end_date
    ) -> Dict[str, Any]:
        # The precomputed parquet is keyed by country/region/subregion, so each
        # GADM id is aggregated to its admin level and labelled with its aoi_id.
        subqueries = [
            gadm_subquery(aoi_id, start_date, end_date) for aoi_id in gadm_ids
        ]
        query = (
            " UNION ALL ".join(subqueries)
            + " ORDER BY aoi_id, alert_date, alert_confidence"
        )
        data: Dict = await self.duckdb_query_service.execute(query)
        data["aoi_type"] = ["admin"] * len(data["aoi_id"])

        return data

    @staticmethod
    def analyze_area(
        input_uris: Dict[str, str], aoi, geojson, start_date, end_date
    ) -> dd.DataFrame:
        # Sadly, this method must be static because Dask can't serialize compute_engine
        # (a live Dask Task) in self
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
            (df.area_ha > 0)
            & (df.alert_date >= start_date)
            & (df.alert_date <= end_date)
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
