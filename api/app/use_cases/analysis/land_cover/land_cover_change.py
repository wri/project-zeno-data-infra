from typing import List

from app.models.common.analysis import AnalysisStatus

from api.app.models.land_change.land_cover import LandCoverChangeAnalyticsIn

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


class LandCoverChangeService:
    def __init__(self, mock=True, utils=None):
        self.mock = mock

    def do(self, land_cover_change_analytics: LandCoverChangeAnalyticsIn):
        pass

    async def get_results(
        self, land_cover_change_analytics: LandCoverChangeAnalyticsIn
    ):
        if self.mock:
            return self._get_mock_results(land_cover_change_analytics)
        else:
            raise ValueError(
                "Mocking is enabled, but no real data processing is available currently."
            )

    def _get_mock_results(
        self, land_cover_change_analytics: LandCoverChangeAnalyticsIn
    ):
        aoi_ids: List[str] = []
        land_cover_start: List[str] = []
        land_cover_end: List[str] = []
        area: List[float] = []

        for aoi_id in land_cover_change_analytics.aoi.ids:
            aoi_ids += [aoi_id] * len(LAND_COVER_CLASSES)
            land_cover_start += LAND_COVER_CLASSES
            land_cover_end += reversed(LAND_COVER_CLASSES)
            area += range(1, len(LAND_COVER_CLASSES) + 1)

        return {
            "id": aoi_ids,
            "land_cover_class_start": land_cover_start,
            "land_cover_class_end": land_cover_end,
            "area_ha": area,
        }

    def get_status(self):
        return AnalysisStatus.saved
