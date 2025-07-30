import json
from pathlib import Path
from typing import List

from app.models.common.analysis import AnalysisStatus

from api.app.models.land_change.land_cover import (
    LandCoverChangeAnalytics,
    LandCoverChangeAnalyticsIn,
)

LAND_COVER_CLASSES = [
    "Bare and sparse vegetation",
    "Short vegetation",
    "Tree cover",
    "Wetland-short vegetation",
    "Water",
    "Snow/ice",
    "Cropland",
    "Built-up",
    "Cultivated grasslands",
]


PAYLOAD_STORE_DIR = Path("/tmp/land_cover_change_analytics_payloads")
PAYLOAD_STORE_DIR.mkdir(parents=True, exist_ok=True)


class LandCoverChangeService:
    def __init__(self, background_tasks):
        self.background_tasks = background_tasks

    def do(self, land_cover_change_analytics: LandCoverChangeAnalyticsIn):
        payload_dict = land_cover_change_analytics.model_dump()
        payload_json = json.dumps(payload_dict, sort_keys=True)

        payload_dir = PAYLOAD_STORE_DIR / land_cover_change_analytics.thumbprint()
        metadata_data = payload_dir / "metadata.json"

        payload_dir.mkdir(parents=True, exist_ok=True)
        metadata_data.write_text(payload_json)

        self.background_tasks.add_task(self.compute, land_cover_change_analytics)

    async def get_resource(self, resource_id: str):
        # Construct file path
        file_path = PAYLOAD_STORE_DIR / resource_id
        analytics_metadata = file_path / "metadata.json"
        analytics_data = file_path / "data.json"
        metadata_content = None
        alerts_dict = None

        if analytics_metadata.exists() and analytics_data.exists():
            # load resource from filesystem
            alerts_dict = json.loads(analytics_data.read_text())
            metadata_content = json.loads(analytics_metadata.read_text())

            return LandCoverChangeAnalytics(
                result=alerts_dict,
                metadata=metadata_content,
                status=AnalysisStatus.saved,
            )

        if analytics_metadata.exists():
            metadata_content = json.loads(analytics_metadata.read_text())

            return LandCoverChangeAnalytics(
                result=None,
                metadata=metadata_content,
                status=AnalysisStatus.pending,
            )

        raise ValueError("Resource not found")

    async def compute(self, land_cover_change_analytics: LandCoverChangeAnalyticsIn):
        aoi_ids: List[str] = []
        land_cover_start: List[str] = []
        land_cover_end: List[str] = []
        area: List[float] = []

        for aoi_id in land_cover_change_analytics.aoi.ids:
            aoi_ids += [aoi_id] * len(LAND_COVER_CLASSES)
            land_cover_start += LAND_COVER_CLASSES
            land_cover_end += reversed(LAND_COVER_CLASSES)
            area += range(1, len(LAND_COVER_CLASSES) + 1)

        results = {
            "id": aoi_ids,
            "land_cover_class_start": land_cover_start,
            "land_cover_class_end": land_cover_end,
            "area_ha": area,
        }

        data = PAYLOAD_STORE_DIR / "data.json"
        data.write_text(json.dumps(results))

    def get_status(self):
        return AnalysisStatus.saved
