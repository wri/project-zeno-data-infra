from typing import Dict

from app.analysis.integrated_alerts.analysis import (
    get_precomputed_statistics,
    zonal_statistics_on_aois,
)
from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis
from app.domain.models.environment import Environment
from app.models.land_change.integrated_alerts import IntegratedAlertsAnalyticsIn

INPUT_URIS = {
    Environment.staging: {},
    Environment.production: {
        "pixel_area_zarr_uri": (
            "s3://gfw-data-lake/umd_area_2013/v1.10/raster/epsg-4326/zarr/"
            "area_m_10m_f32"
        ),
    },
}


class IntegratedAlertsAnalyzer(Analyzer):
    def __init__(
        self,
        compute_engine=None,
        input_uris: Dict[str, str] | None = None,
    ):
        self.compute_engine = compute_engine
        self.input_uris = input_uris

    async def analyze(self, analysis: Analysis) -> None:
        if self.input_uris is None:
            raise RuntimeError("Input URIs must be provided for actual analysis")

        analytics_in = IntegratedAlertsAnalyticsIn(**analysis.metadata)

        aoi_dict = analytics_in.model_dump()["aoi"]
        version = analysis.metadata["_version"]
        if analytics_in.aoi.type == "admin":
            alerts_df = await get_precomputed_statistics(
                aoi_dict,
                self.compute_engine,
                version,
            )
        else:
            alerts_df = await zonal_statistics_on_aois(
                self.input_uris, aoi_dict, self.compute_engine, version
            )

        if analytics_in.start_date is not None:
            alerts_df = alerts_df[alerts_df.alert_date >= analytics_in.start_date]
        if analytics_in.end_date is not None:
            alerts_df = alerts_df[alerts_df.alert_date <= analytics_in.end_date]

        analysis.result = alerts_df.to_dict(orient="list")
