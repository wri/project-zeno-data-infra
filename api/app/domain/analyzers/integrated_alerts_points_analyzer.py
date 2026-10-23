import asyncio
from typing import Dict, List, Tuple

import newrelic.agent as nr_agent
import numpy as np
from shapely.geometry.base import BaseGeometry

from app.analysis.common.analysis import LAND_FILTER_CLASSES
from app.domain.analyzers.analyzer import Analyzer
from app.domain.analyzers.integrated_alerts_analyzer import ALERTS_CONFIDENCE
from app.domain.models.analysis import Analysis
from app.domain.repositories.gadm_aoi_geometry_repository import (
    GadmAoiGeometryRepository,
)
from app.infrastructure.external_services.alert_points_table import (
    AlertPointsTable,
    date_to_day,
    day_to_date,
    load_alert_points_table,
    newest_points,
)
from app.models.land_change.integrated_alerts import (
    IntegratedAlertsPointsAnalyticsIn,
    PointsCursor,
    decode_cursor,
    encode_cursor,
)

CONFIDENCE_CODES = {label: code for code, label in ALERTS_CONFIDENCE.items()}


def build_points_input_uris(version: str) -> Dict[str, str]:
    # First iteration: every request reads the 30-day v20261018 table from the
    # benchmark, whatever the version, until the pipeline publishes one per
    # version at f"integrated-alerts/{version}/alert-points/by-date.parquet".
    return {
        "alert_points_table_uri": (
            "s3://lcl-analytics/zonal-statistics/integrated-alerts/test/"
            "alert-points-index/v20261018/global-30d/by-date.parquet"
        ),
    }


class IntegratedAlertsPointsAnalyzer(Analyzer):
    """List the newest integrated alert pixels in a GADM area (one per request),
    with their pixel center, date, confidence and SBTN natural lands class, from
    the alert points table of one alerts version."""

    def __init__(
        self,
        input_uris: Dict[str, str] | None = None,
        geometry_repository=None,
    ):
        self.input_uris = input_uris
        self.geometry_repository = geometry_repository or GadmAoiGeometryRepository()

    @nr_agent.function_trace(name="IntegratedAlertsPointsAnalyzer.analyze")
    async def analyze(self, analysis: Analysis) -> None:
        if self.input_uris is None:
            raise Exception("Input URIs must be provided for actual analysis")

        analytics_in = IntegratedAlertsPointsAnalyticsIn(**analysis.metadata)
        aoi_ids, geometries = await self._load_geometries(analytics_in)
        table = await asyncio.to_thread(
            load_alert_points_table, self.input_uris["alert_points_table_uri"]
        )
        start_day, end_day = served_days(table, analytics_in)
        after = None
        if analytics_in.cursor is not None:
            cursor = decode_cursor(analytics_in.cursor)
            # The router serves a cursor's own version; a mismatch means the
            # position would point into a different file.
            if cursor.version != table.version:
                raise ValueError(
                    f"The cursor is for alerts version {cursor.version}, but the "
                    f"table loaded is for {table.version}."
                )
            after = (cursor.group, cursor.offset)

        result, truncated, next_cursor = await asyncio.to_thread(
            self._list_points,
            table,
            analytics_in,
            aoi_ids,
            geometries,
            start_day,
            end_day,
            after,
        )
        analysis.result = result
        analysis.metadata = {
            **analysis.metadata,
            "alerts": {
                "version": table.version,
                "latest_alert_date": day_to_date(table.latest_day).isoformat(),
                "stored_start_date": day_to_date(table.first_day).isoformat(),
                "stored_end_date": day_to_date(table.latest_day).isoformat(),
                "start_date": (
                    day_to_date(start_day).isoformat() if start_day <= end_day else None
                ),
                "end_date": (
                    day_to_date(end_day).isoformat() if start_day <= end_day else None
                ),
            },
            "truncated": truncated,
            # Pass back as `cursor` for the next page; None when the list is done.
            "next_cursor": next_cursor,
        }

    async def _load_geometries(
        self, analytics_in
    ) -> Tuple[List[str], List[BaseGeometry]]:
        aoi_ids = sorted(set(analytics_in.aoi.ids))
        geometries, _ = await self.geometry_repository.load("admin", aoi_ids)
        if len(geometries) != len(aoi_ids):
            raise ValueError(
                f"Expected {len(aoi_ids)} geometries for admin AOIs, "
                f"got {len(geometries)}."
            )
        return aoi_ids, list(geometries)

    @staticmethod
    def _list_points(
        table: AlertPointsTable,
        analytics_in,
        aoi_ids: List[str],
        geometries: List[BaseGeometry],
        start_day: int,
        end_day: int,
        after: Tuple[int, int] | None = None,
    ):
        confidences = (
            list(ALERTS_CONFIDENCE)
            if analytics_in.alert_confidence is None
            else [CONFIDENCE_CODES[c] for c in analytics_in.alert_confidence]
        )
        classes = (
            None
            if analytics_in.land_filter is None
            else LAND_FILTER_CLASSES[analytics_in.land_filter]
        )
        result: Dict[str, list] = {
            "lat": [],
            "lon": [],
            "alert_date": [],
            "alert_confidence": [],
            "natural_lands_class": [],
        }
        truncated: Dict[str, bool] = {}
        next_cursor = None
        # One AOI per request (validated), so one list and at most one cursor.
        for aoi_id, geometry in zip(aoi_ids, geometries):
            if start_day > end_day:
                truncated[aoi_id] = False
                continue
            points, truncated[aoi_id], last = newest_points(
                table,
                geometry,
                start_day,
                end_day,
                confidences,
                classes,
                analytics_in.limit,
                after,
            )
            if truncated[aoi_id] and last is not None:
                next_cursor = encode_cursor(
                    PointsCursor(table.version, *last, analytics_in.list_id())
                )
            result["lat"] += table.lat(points["row"].to_numpy()).tolist()
            result["lon"] += table.lon(points["col"].to_numpy()).tolist()
            days = points["alert_date"].to_numpy()
            dates = {d: day_to_date(d).isoformat() for d in np.unique(days)}
            result["alert_date"] += [dates[d] for d in days]
            result["alert_confidence"] += [
                ALERTS_CONFIDENCE[c] for c in points["confidence"].tolist()
            ]
            result["natural_lands_class"] += points["natural_lands_class"].tolist()
        return result, truncated, next_cursor


def served_days(table: AlertPointsTable, analytics_in) -> Tuple[int, int]:
    """The request's date range clipped to the dates stored in the table, as days
    since the alerts epoch. start > end when they don't overlap."""
    start_day = (
        table.first_day
        if analytics_in.start_date is None
        else max(date_to_day(analytics_in.start_date), table.first_day)
    )
    end_day = (
        table.latest_day
        if analytics_in.end_date is None
        else min(date_to_day(analytics_in.end_date), table.latest_day)
    )
    return start_day, end_day
