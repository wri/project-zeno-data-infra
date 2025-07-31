from typing import List

from app.models.common.analysis import AnalysisStatus
from app.models.land_change.land_cover import (
    LandCoverChangeAnalytics,
    LandCoverChangeAnalyticsIn,
    LandCoverChangeResult,
)
from app.use_cases.analysis.land_cover.file_resource import (
    load_resource,
    write_resource,
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


class LandCoverChangeService:
    def __init__(self):
        pass

    async def do(
        self,
        land_cover_change_analytics: LandCoverChangeAnalyticsIn,
        write_resource=write_resource,
        load_resource=load_resource,
    ):
        resource = await load_resource(land_cover_change_analytics.thumbprint())
        if resource.metadata is None:
            await write_resource(
                land_cover_change_analytics.thumbprint(),
                LandCoverChangeAnalytics(
                    metadata=land_cover_change_analytics, status=AnalysisStatus.pending
                ),
            )

        if resource.result is None:
            resource = await self.compute(land_cover_change_analytics)
            await write_resource(land_cover_change_analytics.thumbprint(), resource)

    async def get(
        self, resource_id: str, load_resource=load_resource
    ) -> LandCoverChangeAnalytics:
        """
        Retrieve the analytics result for a given resource ID.
        """
        return await load_resource(resource_id)

    async def compute(
        self, land_cover_change_analytics: LandCoverChangeAnalyticsIn
    ) -> LandCoverChangeAnalytics:
        aoi_ids: List[str] = []
        land_cover_start: List[str] = []
        land_cover_end: List[str] = []
        area: List[float] = []

        for aoi_id in land_cover_change_analytics.aoi.ids:
            aoi_ids += [aoi_id] * len(LAND_COVER_CLASSES)
            land_cover_start += LAND_COVER_CLASSES
            land_cover_end += reversed(LAND_COVER_CLASSES)
            area += range(1, len(LAND_COVER_CLASSES) + 1)

        results = LandCoverChangeResult(
            id=aoi_ids,
            land_cover_class_start=land_cover_start,
            land_cover_class_end=land_cover_end,
            area_ha=area,
        )

        return LandCoverChangeAnalytics(
            result=results,
            metadata=land_cover_change_analytics,
            status=AnalysisStatus.saved,
        )
