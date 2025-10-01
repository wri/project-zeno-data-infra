from typing import Any, Dict

import dask.dataframe as dd
import newrelic.agent as nr_agent
import numpy as np
from app.analysis.common.analysis import (
    JULIAN_DATE_2021,
)
from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis
from app.domain.models.dataset import Dataset
from app.domain.repositories.data_api_aoi_geometry_repository import (
    DataApiAoiGeometryRepository,
)
from app.domain.repositories.zarr_dataset_repository import ZarrDatasetRepository
from app.models.land_change.integrated_alerts import IntegratedAlertsAnalyticsIn
from flox.xarray import xarray_reduce


class IntegratedAlertsAnalyzer(Analyzer):
    """Get count of integrated alerts in an AOI."""

    def __init__(
        self,
        dataset_repository=ZarrDatasetRepository(),
        aoi_geometry_repository=DataApiAoiGeometryRepository(),
        dask_client=None,
        dask_map_service=None,
        query_service=None,
    ):
        self.dataset_repository = dataset_repository
        self.aoi_geometry_repository = aoi_geometry_repository
        self.dask_client = dask_client
        self.dask_map_service = (
            dask_map_service,
        )  # DaskAoiMapService(self.dask_client, self.aoi_geometry_repository, self.dataset_repository)
        self.query_service = query_service

    @nr_agent.function_trace(name="IntegratedAlertsAnalyzer.analyze")
    async def analyze(self, analysis: Analysis):
        integrated_alerts_analytics_in = IntegratedAlertsAnalyticsIn(
            **analysis.metadata
        )
        if integrated_alerts_analytics_in.aoi.type == "admin":
            gadm_ids = integrated_alerts_analytics_in.aoi.ids
            results = await self.analyze_admin_areas(gadm_ids)
        else:
            results = await self.dask_map_service.map(
                integrated_alerts_analytics_in.aoi.ids,
                integrated_alerts_analytics_in.aoi.type,
                self.analyze_aoi,
            )
        return results

    async def analyze_admin_areas(self, gadm_ids) -> Dict[str, Any]:
        id_str = (", ").join([f"'{aoi_id}'" for aoi_id in gadm_ids])
        query = f"select aoi_id, integrated_alerts_date, integrated_alerts_confidence, integrated_alerts_count from data_source where aoi_id in ({id_str})"
        df = await self.query_service.execute(query)
        df["aoi_type"] = ["admin"] * len(df["aoi_id"])

        return df

    @staticmethod
    def analyze_aoi(aoi, geojson, dataset_repository):
        aoi_id, aoi_geometry = aoi
        integrated_alerts = dataset_repository.load(
            Dataset.integrated_alerts, geometry=aoi_geometry
        )

        groupby_layers = [integrated_alerts.alert_date, integrated_alerts.confidence]
        expected_groups = [np.arange(731, 2000), [1, 2, 3]]

        counts = xarray_reduce(
            integrated_alerts.alert_date,
            *tuple(groupby_layers),
            func="count",
            expected_groups=tuple(expected_groups),
        )

        alerts_df = (
            counts.to_dask_dataframe()
            .drop("band", axis=1)
            .drop("spatial_ref", axis=1)
            .reset_index(drop=True)
        )
        alerts_df = alerts_df.rename(
            columns={
                "confidence": "integrated_alert_confidence",
                "alert_date": "integrated_alert_date",
            }
        )
        alerts_df.dist_alert_confidence = alerts_df.dist_alert_confidence.map(
            {2: "low", 3: "high"}, meta=("integrated_alert_confidence", "uint8")
        )
        alerts_df.dist_alert_date = dd.to_datetime(
            alerts_df.dist_alert_date + JULIAN_DATE_2021, origin="julian", unit="D"
        ).dt.strftime("%Y-%m-%d")

        alerts_df = alerts_df[alerts_df.integrated_alert_count > 0]
        alerts_df["aoi_id"] = aoi_id
        return alerts_df
