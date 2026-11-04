from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

import newrelic.agent as nr_agent
from shapely.geometry.base import BaseGeometry

from app.analysis.common.analysis import LAND_FILTER_CLASSES
from app.domain.analyzers.analyzer import Analyzer
from app.domain.analyzers.integrated_alerts_analyzer import ALERTS_CONFIDENCE
from app.domain.models.analysis import Analysis
from app.domain.repositories.alert_points_repository import (
    AlertPointsQuery,
    AlertPointsRepository,
    AlertPointsSource,
)
from app.domain.repositories.gadm_aoi_geometry_repository import (
    GadmAoiGeometryRepository,
)
from app.models.land_change.integrated_alerts import (
    IntegratedAlertsPointsAnalyticsIn,
    PointsCursor,
    decode_cursor,
    encode_cursor,
)

CONFIDENCE_CODES = {label: code for code, label in ALERTS_CONFIDENCE.items()}


class IntegratedAlertsPointsAnalyzer(Analyzer):
    """List the newest integrated alert pixels in a GADM area (one per request),
    with their pixel center, date, confidence and SBTN natural lands class, from
    an alert points repository serving one alerts version."""

    def __init__(
        self,
        points_repository: AlertPointsRepository,
        input_uris: Dict[str, str] | None = None,
        geometry_repository=None,
    ):
        self.points_repository = points_repository
        self.input_uris = input_uris
        self.geometry_repository = geometry_repository or GadmAoiGeometryRepository()

    @nr_agent.function_trace(name="IntegratedAlertsPointsAnalyzer.analyze")
    async def analyze(self, analysis: Analysis) -> None:
        analytics_in = IntegratedAlertsPointsAnalyticsIn(**analysis.metadata)
        aoi_ids, geometries = await self._load_geometries(analytics_in)
        source = await self.points_repository.source()
        start_date, end_date = served_dates(source, analytics_in)
        after = None
        if analytics_in.cursor is not None:
            cursor = decode_cursor(analytics_in.cursor)
            # The router serves a cursor's own version; a mismatch means the
            # position would point into a different version's points.
            if cursor.version != source.version:
                raise ValueError(
                    f"The cursor is for alerts version {cursor.version}, but the "
                    f"points served are for {source.version}."
                )
            after = cursor.position

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
            if start_date > end_date:
                truncated[aoi_id] = False
                continue
            page = await self.points_repository.read_most_recent_points(
                AlertPointsQuery(
                    geometry=geometry,
                    start_date=start_date,
                    end_date=end_date,
                    confidences=_confidence_codes(analytics_in),
                    natural_lands_classes=(
                        None
                        if analytics_in.land_filter is None
                        else LAND_FILTER_CLASSES[analytics_in.land_filter]
                    ),
                    limit=analytics_in.limit,
                    after=after,
                )
            )
            truncated[aoi_id] = page.more
            if page.more and page.last_position is not None:
                next_cursor = encode_cursor(
                    PointsCursor(
                        source.version, page.last_position, analytics_in.list_id()
                    )
                )
            points = page.points
            result["lat"] += points["lat"].tolist()
            result["lon"] += points["lon"].tolist()
            result["alert_date"] += [d.isoformat() for d in points["alert_date"]]
            result["alert_confidence"] += [
                ALERTS_CONFIDENCE[c] for c in points["confidence"].tolist()
            ]
            result["natural_lands_class"] += points["natural_lands_class"].tolist()

        analysis.result = result
        analysis.metadata = {
            **analysis.metadata,
            "alerts": {
                "version": source.version,
                "latest_alert_date": source.latest_date.isoformat(),
                "stored_start_date": source.first_date.isoformat(),
                "stored_end_date": source.latest_date.isoformat(),
                "start_date": (
                    start_date.isoformat() if start_date <= end_date else None
                ),
                "end_date": end_date.isoformat() if start_date <= end_date else None,
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


def _confidence_codes(analytics_in) -> List[int]:
    if analytics_in.alert_confidence is None:
        return list(ALERTS_CONFIDENCE)
    return [CONFIDENCE_CODES[c] for c in analytics_in.alert_confidence]


def _parse_date(value: Optional[str]) -> Optional[date]:
    if value is None:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def served_dates(source: AlertPointsSource, analytics_in) -> Tuple[date, date]:
    """The request's date range clipped to the dates the source stores. start >
    end when they don't overlap."""
    start_date = _parse_date(analytics_in.start_date)
    end_date = _parse_date(analytics_in.end_date)
    return (
        source.first_date if start_date is None else max(start_date, source.first_date),
        source.latest_date if end_date is None else min(end_date, source.latest_date),
    )
