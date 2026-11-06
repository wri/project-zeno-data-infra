from typing import Dict

import newrelic.agent as nr_agent

from app.analysis.common.analysis import LAND_FILTER_CLASSES
from app.domain.analyzers.analyzer import Analyzer
from app.domain.analyzers.integrated_alerts_points_analyzer_helpers import (
    POINTS_RESULT_COLUMNS,
    confidence_codes,
    format_points,
    read_aoi_geometry,
    served_alerts_metadata,
    served_dates,
)
from app.domain.models.analysis import Analysis
from app.domain.repositories.alert_points_repository import (
    AlertPointsQuery,
    AlertPointsRepository,
)
from app.domain.repositories.aoi_geometry_repository import AoiGeometryRepository
from app.models.land_change.integrated_alerts import (
    IntegratedAlertsPointsAnalyticsIn,
    PointsCursor,
    decode_cursor,
    encode_cursor,
)


class IntegratedAlertsPointsAnalyzer(Analyzer):
    """List the newest integrated alert pixels in an admin or saved custom area
    (one per request), with their pixel center, date, confidence and SBTN natural
    lands class, from an alert points repository serving one alerts version."""

    def __init__(
        self,
        points_repository: AlertPointsRepository,
        geometry_repository: AoiGeometryRepository,
        input_uris: Dict[str, str] | None = None,
    ):
        self.points_repository = points_repository
        self.geometry_repository = geometry_repository
        self.input_uris = input_uris

    @nr_agent.function_trace(name="IntegratedAlertsPointsAnalyzer.analyze")
    async def analyze(self, analysis: Analysis) -> None:
        analytics_in = IntegratedAlertsPointsAnalyticsIn(**analysis.metadata)
        aoi_id, geometry = await read_aoi_geometry(
            self.geometry_repository, analytics_in
        )
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

        next_cursor = None
        # The requested dates lie outside the stored window, so nothing is served.
        if start_date > end_date:
            result: Dict[str, list] = {column: [] for column in POINTS_RESULT_COLUMNS}
            more = False
        else:
            page = await self.points_repository.read_most_recent_points(
                AlertPointsQuery(
                    geometry=geometry,
                    start_date=start_date,
                    end_date=end_date,
                    confidences=confidence_codes(analytics_in),
                    natural_lands_classes=(
                        None
                        if analytics_in.land_filter is None
                        else LAND_FILTER_CLASSES[analytics_in.land_filter]
                    ),
                    limit=analytics_in.limit,
                    after=after,
                )
            )
            result, more = format_points(page.points), page.more
            if page.more and page.last_position is not None:
                next_cursor = encode_cursor(
                    PointsCursor(
                        source.version, page.last_position, analytics_in.list_id()
                    )
                )

        analysis.result = result
        analysis.metadata = {
            **analysis.metadata,
            "alerts": served_alerts_metadata(source, start_date, end_date),
            "truncated": {aoi_id: more},
            # Pass back as `cursor` for the next page; None when the list is done.
            "next_cursor": next_cursor,
        }
