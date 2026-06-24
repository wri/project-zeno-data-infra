from functools import partial
from typing import Dict

import dask.dataframe as dd
import numpy as np
from flox.xarray import xarray_reduce

from app.analysis.common.analysis import JULIAN_DATE_2021, read_zarr_clipped_to_geojson
from app.domain.analyzers.zonal_statistics_analyzer import ZonalStatisticsAnalyzer
from app.domain.models.dataset import Dataset
from app.domain.models.environment import Environment
from app.domain.repositories.zarr_dataset_repository import ZarrDatasetRepository
from app.models.land_change.integrated_alerts import IntegratedAlertsAnalyticsIn

ALERTS_CONFIDENCE = {2: "low", 3: "high", 4: "highest"}

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
        str(Dataset.pixel_area_m2_10m): ZarrDatasetRepository.resolve_zarr_uri(
            Dataset.pixel_area_m2_10m, Environment.production
        ),
        "admin_results_table_uri": (
            "s3://lcl-analytics/zonal-statistics/intdist-alerts/v20260601/"
            "admin-intdist-alerts.aoi.parquet"
        ),
    },
}


class IntegratedAlertsAnalyzer(ZonalStatisticsAnalyzer):
    """Get integrated disturbance alert areas by date and confidence for the AOIs"""

    model = IntegratedAlertsAnalyticsIn

    def build_admin_query(self, analytics_in) -> str:
        # The precomputed parquet is keyed by aoi_id and preaggregated to each
        # GADM level, so a single scan filtered by aoi_id serves every request.
        start_date = analytics_in.start_date
        end_date = analytics_in.end_date
        id_list = ", ".join(f"'{aoi_id}'" for aoi_id in analytics_in.aoi.ids)
        return (
            "SELECT aoi_id, "
            "STRFTIME(intdist_alert_date, '%Y-%m-%d') AS alert_date, "
            "intdist_alert_confidence AS alert_confidence, "
            "area_ha "
            "FROM data_source "
            f"WHERE aoi_id IN ({id_list}) "
            f"AND intdist_alert_date BETWEEN DATE '{start_date}' AND DATE '{end_date}' "
            "ORDER BY aoi_id, alert_date, alert_confidence"
        )

    def build_area_task(self, analytics_in):
        return partial(
            self.analyze_area,
            self.input_uris,
            start_date=analytics_in.start_date,
            end_date=analytics_in.end_date,
        )

    @staticmethod
    def analyze_area(
        input_uris: Dict[str, str], aoi, geojson, start_date, end_date
    ) -> dd.DataFrame:
        # Sadly, this method must be static because Dask can't serialize compute_engine
        # (a live Dask Task) in self
        alerts = read_zarr_clipped_to_geojson(
            input_uris["integrated_alerts_zarr_uri"], geojson
        )

        # The 10m pixel-area raster is in m²; cast to float64 and convert to
        # hectares so it matches the 10m alerts grid and the admin path's units.
        pixel_area = (
            read_zarr_clipped_to_geojson(
                input_uris[str(Dataset.pixel_area_m2_10m)], geojson
            )
            .band_data.astype(np.float64)
            .reindex_like(alerts, method="nearest", tolerance=1e-5)
            / 10000
        )

        groupby_layers = [alerts.alert_date, alerts.confidence]
        expected_groups = [
            np.arange(5000),  # days since 2020-12-31 (epoch matches the GADM parquet)
            [2, 3, 4],  # confidence: 2=low, 3=high, 4=highest
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
