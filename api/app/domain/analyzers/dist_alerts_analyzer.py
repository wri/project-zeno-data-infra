from app.analysis.dist_alerts.analysis import (
    get_precomputed_statistics,
    zonal_statistics_on_aois,
)
from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis
from app.models.land_change.dist_alerts import DistAlertsAnalyticsIn


class DistAlertsAnalyzer(Analyzer):
    def __init__(
        self,
        analysis_repository=None,
        compute_engine=None,
        dataset_repository=None,
    ):
        self.analysis_repository = analysis_repository
        self.compute_engine = compute_engine  # Dask Client, or not?
        self.dataset_repository = dataset_repository  # AWS-S3 for zarrs, etc.

    async def analyze(self, analysis: Analysis):
        dist_analytics_in = DistAlertsAnalyticsIn(**analysis.metadata)

        # for now we only support one intersection, as enforced by the route
        if dist_analytics_in.intersections:
            intersection = dist_analytics_in.intersections[0]
        else:
            intersection = None

        aoi_dict = dist_analytics_in.model_dump()["aoi"]
        if dist_analytics_in.aoi.type == "admin":
            alerts_df = await get_precomputed_statistics(
                aoi_dict, intersection, self.compute_engine
            )
        else:
            alerts_df = await zonal_statistics_on_aois(
                aoi_dict, self.compute_engine, intersection
            )

        if dist_analytics_in.start_date is not None:
            alerts_df = alerts_df[
                alerts_df.dist_alert_date >= dist_analytics_in.start_date
            ]
        if dist_analytics_in.end_date is not None:
            alerts_df = alerts_df[
                alerts_df.dist_alert_date <= dist_analytics_in.end_date
            ]
        alerts_dict = alerts_df.to_dict(orient="list")

        return alerts_dict
