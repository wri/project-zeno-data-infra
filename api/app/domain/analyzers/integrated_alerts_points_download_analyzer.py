import asyncio
import tempfile
from pathlib import Path
from typing import Dict

import newrelic.agent as nr_agent

from app.domain.analyzers.analyzer import Analyzer
from app.domain.analyzers.integrated_alerts_points_analyzer import (
    POINTS_RESULT_COLUMNS,
    alert_points_query,
    format_points,
    read_admin_geometries,
    served_alerts_metadata,
    served_dates,
)
from app.domain.models.analysis import Analysis
from app.domain.repositories.alert_points_repository import AlertPointsRepository
from app.domain.repositories.gadm_aoi_geometry_repository import (
    GadmAoiGeometryRepository,
)
from app.infrastructure.export.alert_points_files import (
    write_alert_points_csv,
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
        aoi_ids, geometries = await read_admin_geometries(
            self.geometry_repository, analytics_in
        )
        source = await self.points_repository.source()
        start_date, end_date = served_dates(source, analytics_in)

        points: Dict[str, list] = {column: [] for column in POINTS_RESULT_COLUMNS}
        truncated: Dict[str, bool] = {}
        for aoi_id, geometry in zip(aoi_ids, geometries):
            if start_date > end_date:
                truncated[aoi_id] = False
                continue
            # One read: the repository stops near the limit however many points
            # match, and reports whether more did.
            page = await self.points_repository.read_most_recent_points(
                alert_points_query(
                    analytics_in,
                    geometry,
                    start_date,
                    end_date,
                    limit=POINTS_DOWNLOAD_MAX_POINTS,
                    after=None,
                )
            )
            truncated[aoi_id] = page.more
            for column, values in format_points(page.points).items():
                points[column] += values

        extension, content_type = DOWNLOAD_FILE_TYPES[analytics_in.format]
        alerts = served_alerts_metadata(source, start_date, end_date)
        name = (
            f"integrated_alerts_{'_'.join(aoi_ids)}_"
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
                await asyncio.to_thread(write_alert_points_csv, points, path)
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
            "truncated": truncated,
        }
