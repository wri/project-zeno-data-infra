import asyncio
import tempfile
from pathlib import Path
from typing import Dict

import newrelic.agent as nr_agent
import pandas as pd

from app.analysis.common.analysis import LAND_FILTER_CLASSES
from app.domain.analyzers.analyzer import Analyzer
from app.domain.analyzers.integrated_alerts_points_analyzer_helpers import (
    POINTS_RESULT_COLUMNS,
    confidence_codes,
    format_points,
    read_admin_geometries,
    served_alerts_metadata,
    served_dates,
)
from app.domain.models.analysis import Analysis
from app.domain.repositories.alert_points_repository import (
    AlertPointsQuery,
    AlertPointsRepository,
)
from app.domain.repositories.gadm_aoi_geometry_repository import (
    GadmAoiGeometryRepository,
)
from app.infrastructure.export.alert_points_files import (
    write_alert_points_shapefile_zip,
)
from app.models.land_change.integrated_alerts import (
    POINTS_DOWNLOAD_ANALYTICS_NAME,
    POINTS_DOWNLOAD_MAX_POINTS,
    IntegratedAlertsPointsDownloadIn,
)

# File extension and content type of each download format
DOWNLOAD_FILE_TYPES = {
    "csv": ("csv", "text/csv"),
    "shp": ("zip", "application/zip"),
}


class IntegratedAlertsPointsDownloadAnalyzer(Analyzer):
    """Write the newest integrated alert points in a GADM area (one per request),
    up to POINTS_DOWNLOAD_MAX_POINTS, to a CSV or zipped shapefile in a download
    file store."""

    def __init__(
        self,
        points_repository: AlertPointsRepository,
        file_store,
        input_uris: Dict[str, str] | None = None,
        geometry_repository=None,
    ):
        self.points_repository = points_repository
        self.file_store = file_store
        self.input_uris = input_uris
        self.geometry_repository = geometry_repository or GadmAoiGeometryRepository()

    @nr_agent.function_trace(name="IntegratedAlertsPointsDownloadAnalyzer.analyze")
    async def analyze(self, analysis: Analysis) -> None:
        analytics_in = IntegratedAlertsPointsDownloadIn(**analysis.metadata)
        # One AOI per request (validated).
        (aoi_id,), (geometry,) = await read_admin_geometries(
            self.geometry_repository, analytics_in
        )
        source = await self.points_repository.source()
        start_date, end_date = served_dates(source, analytics_in)

        # The requested dates lie outside the stored window, so nothing is served.
        if start_date > end_date:
            points: Dict[str, list] = {column: [] for column in POINTS_RESULT_COLUMNS}
            more = False
        else:
            # One read: the repository stops near the limit however many points
            # match, and reports whether more did.
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
                    limit=POINTS_DOWNLOAD_MAX_POINTS,
                    after=None,
                )
            )
            points, more = format_points(page.points), page.more

        extension, content_type = DOWNLOAD_FILE_TYPES[analytics_in.format]
        alerts = served_alerts_metadata(source, start_date, end_date)
        name = (
            f"integrated_alerts_{aoi_id}_"
            f"{alerts['start_date'] or 'none'}_{alerts['end_date'] or 'none'}"
        )
        # The request thumbprint leaves out the alerts version (it isn't kept in
        # the metadata), so the version keeps each version's file apart.
        file_key = (
            f"{POINTS_DOWNLOAD_ANALYTICS_NAME}/{source.version}/"
            f"{analytics_in.thumbprint()}.{extension}"
        )
        with tempfile.TemporaryDirectory() as download_dir:
            path = Path(download_dir) / f"{name}.{extension}"
            # Writing up to a million rows blocks, so it runs in a thread.
            if analytics_in.format == "csv":
                await asyncio.to_thread(pd.DataFrame(points).to_csv, path, index=False)
            else:
                await asyncio.to_thread(
                    write_alert_points_shapefile_zip, points, path, name
                )
            await self.file_store.write_file(file_key, path, content_type)

        analysis.result = {
            "file_key": file_key,
            "filename": f"{name}.{extension}",
            "format": analytics_in.format,
            "point_count": len(points["lat"]),
        }
        analysis.metadata = {
            **analysis.metadata,
            "alerts": alerts,
            "truncated": {aoi_id: more},
        }
