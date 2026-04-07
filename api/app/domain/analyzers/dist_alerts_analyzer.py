from typing import Dict

from app.analysis.dist_alerts.analysis import (
    get_precomputed_statistics,
    zonal_statistics_on_aois,
)
from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis
from app.domain.models.environment import Environment
from app.models.land_change.dist_alerts import DistAlertsAnalyticsIn

INPUT_URIS = {
    Environment.production: {
        "dist_drivers_zarr_uri": (
            "s3://gfw-data-lake/umd_glad_dist_alerts_driver/zarr/umd_dist_alerts_drivers.zarr/"
        ),
        "land_cover_zarr_uri": (
            "s3://gfw-data-lake/umd_lcl_land_cover/v2/raster/epsg-4326/zarr/umd_lcl_land_cover_2015-2024.zarr/"
        ),
        "natural_grasslands_zarr_uri": (
            "s3://gfw-data-lake/gfw_grasslands/v1/zarr/natural_grasslands_4kchunk.zarr/"
        ),
        "natural_lands_zarr_uri": (
            "s3://gfw-data-lake/sbtn_natural_lands/zarr/sbtn_natural_lands_all_classes_clipped_to_dist.zarr/"
        ),
        "pixel_area_uri": "s3://gfw-data-lake/umd_area_2013/v1.10/raster/epsg-4326/zarr/pixel_area_ha.zarr/",
    }
}


class DistAlertsAnalyzer(Analyzer):
    def __init__(
        self,
        compute_engine=None,
        input_uris: Dict[str, str] | None = None,
    ):
        self.compute_engine = compute_engine  # Dask Client, or not?
        self.input_uris = input_uris

    async def analyze(self, analysis: Analysis) -> None:
        if self.input_uris is None:
            raise RuntimeError("Input URIs must be provided for actual analysis")

        dist_analytics_in = DistAlertsAnalyticsIn(**analysis.metadata)
        if analysis.metadata.get("_input_uris") is not None:
            dist_analytics_in._input_uris = analysis.metadata["_input_uris"]

        # for now we only support one intersection, as enforced by the route
        if dist_analytics_in.intersections:
            intersection = dist_analytics_in.intersections[0]
        else:
            intersection = None

        aoi_dict = dist_analytics_in.model_dump()["aoi"]
        version = analysis.metadata["_version"]
        if dist_analytics_in.aoi.type == "admin":
            alerts_df = await get_precomputed_statistics(
                aoi_dict,
                intersection,
                self.compute_engine,
                version,
            )
        else:
            alerts_df = await zonal_statistics_on_aois(
                self.input_uris, aoi_dict, self.compute_engine, version, intersection
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

        analysis.result = alerts_dict
