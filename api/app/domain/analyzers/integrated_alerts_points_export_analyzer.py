import asyncio
import tempfile
from dataclasses import replace
from pathlib import Path
from typing import Dict, Tuple, Type

import newrelic.agent as nr_agent
import pandas as pd

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
from app.domain.repositories.alert_points_file import AlertPointsFile
from app.domain.repositories.alert_points_repository import (
    AlertPointsQuery,
    AlertPointsRepository,
)
from app.domain.repositories.aoi_geometry_repository import AoiGeometryRepository
from app.models.land_change.integrated_alerts import (
    POINTS_EXPORT_ANALYTICS_NAME,
    POINTS_EXPORT_MAX_POINTS,
    IntegratedAlertsPointsExportIn,
)


class IntegratedAlertsPointsExportAnalyzer(Analyzer):
    """Write the newest integrated alert points in an admin or saved custom area
    (one per request), up to the export cap, to a file in the requested format in
    an export file store.

    Points are read and written a page at a time, so memory stays bounded by the
    page size rather than the cap.
    """

    def __init__(
        self,
        points_repository: AlertPointsRepository,
        file_store,
        geometry_repository: AoiGeometryRepository,
        file_types: Dict[str, Type[AlertPointsFile]],
        page_size: int,
        input_uris: Dict[str, str] | None = None,
    ):
        self.points_repository = points_repository
        self.file_store = file_store
        self.geometry_repository = geometry_repository
        self.file_types = file_types
        self.page_size = page_size
        self.input_uris = input_uris

    @nr_agent.function_trace(name="IntegratedAlertsPointsExportAnalyzer.analyze")
    async def analyze(self, analysis: Analysis) -> None:
        analytics_in = IntegratedAlertsPointsExportIn(**analysis.metadata)
        aoi_id, geometry = await read_aoi_geometry(
            self.geometry_repository, analytics_in
        )
        source = await self.points_repository.source()
        start_date, end_date = served_dates(source, analytics_in)

        file_type = self.file_types[analytics_in.format]
        alerts = served_alerts_metadata(source, start_date, end_date)
        name = (
            f"integrated_alerts_{aoi_id}_"
            f"{alerts['start_date'] or 'none'}_{alerts['end_date'] or 'none'}"
        )
        # The request thumbprint leaves out the alerts version (it isn't kept in
        # the metadata), so the version keeps each version's file apart.
        file_key = (
            f"{POINTS_EXPORT_ANALYTICS_NAME}/{source.version}/"
            f"{analytics_in.thumbprint()}.{file_type.extension}"
        )
        with tempfile.TemporaryDirectory() as export_dir:
            path = Path(export_dir) / f"{name}.{file_type.extension}"
            export_file = file_type(path)
            # The requested dates lie outside the stored window, so nothing is served.
            if start_date > end_date:
                empty = pd.DataFrame(columns=POINTS_RESULT_COLUMNS)
                await asyncio.to_thread(export_file.append, empty)
                point_count, more = 0, False
            else:
                query = AlertPointsQuery(
                    geometry=geometry,
                    start_date=start_date,
                    end_date=end_date,
                    confidences=confidence_codes(analytics_in),
                    natural_lands_classes=(
                        None
                        if analytics_in.land_filter is None
                        else LAND_FILTER_CLASSES[analytics_in.land_filter]
                    ),
                    limit=self.page_size,
                    after=None,
                )
                point_count, more = await self.write_points(export_file, query)
            await asyncio.to_thread(export_file.close)
            await self.file_store.write_file(file_key, path, file_type.content_type)

        analysis.result = {
            "file_key": file_key,
            "filename": f"{name}.{file_type.extension}",
            "format": analytics_in.format,
            "point_count": point_count,
        }
        analysis.metadata = {
            **analysis.metadata,
            "alerts": alerts,
            "truncated": {aoi_id: more},
        }

    async def write_points(
        self, export_file: AlertPointsFile, query: AlertPointsQuery
    ) -> Tuple[int, bool]:
        """Write the query's newest points to the file a page at a time, up to the
        export cap. Returns how many were written and whether more matched."""
        point_count, more, after = 0, True, None
        while more and point_count < POINTS_EXPORT_MAX_POINTS:
            limit = min(self.page_size, POINTS_EXPORT_MAX_POINTS - point_count)
            page = await self.points_repository.read_most_recent_points(
                replace(query, limit=limit, after=after)
            )
            points = await asyncio.to_thread(format_points, page.points)
            await asyncio.to_thread(export_file.append, points)
            point_count += len(points)
            more, after = page.more, page.last_position
        return point_count, more
