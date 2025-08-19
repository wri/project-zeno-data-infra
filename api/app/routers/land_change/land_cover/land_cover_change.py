from typing import List

from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis
from app.domain.repositories.analysis_repository import AnalysisRepository
from app.infrastructure.persistence.file_system_analysis_repository import (
    FileSystemAnalysisRepository,
)
from app.models.common.analysis import AnalyticsOut
from app.models.common.base import DataMartResourceLinkResponse
from app.models.land_change.land_cover import (
    LandCoverChangeAnalytics,
    LandCoverChangeAnalyticsIn,
    LandCoverChangeAnalyticsResponse,
    LandCoverChangeResult,
)
from app.routers.common_analytics import create_analysis, get_analysis
from app.use_cases.analysis.analysis_service import AnalysisService
from fastapi import APIRouter, BackgroundTasks, Depends, Request
from fastapi import Response as FastAPIResponse
from fastapi.responses import ORJSONResponse
from pydantic import UUID5

ANALYTICS_NAME = "land_cover_change"
router = APIRouter(prefix=f"/{ANALYTICS_NAME}")


def get_analysis_repository() -> AnalysisRepository:
    return FileSystemAnalysisRepository(ANALYTICS_NAME)


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


def create_analysis_service() -> AnalysisService:
    class DummyLandCoverChangeAnalyzer(Analyzer):
        def __init__(self, analysis_repository: AnalysisRepository):
            self.analysis_repository = analysis_repository

        async def analyze(self, analysis: Analysis):
            land_cover_change_analytics_in = LandCoverChangeAnalyticsIn(
                **analysis.metadata
            )
            aoi_ids: List[str] = []
            land_cover_start: List[str] = []
            land_cover_end: List[str] = []
            area: List[float] = []

            for aoi_id in analysis.metadata["aoi"]["ids"]:
                aoi_ids += [aoi_id] * len(LAND_COVER_CLASSES)
                land_cover_start += LAND_COVER_CLASSES
                land_cover_end += reversed(LAND_COVER_CLASSES)
                area += range(1, len(LAND_COVER_CLASSES) + 1)

            results = LandCoverChangeResult(
                id=aoi_ids,
                land_cover_class_start=land_cover_start,
                land_cover_class_end=land_cover_end,
                area_ha=area,
            ).model_dump()

            await self.analysis_repository.store_analysis(
                resource_id=land_cover_change_analytics_in.thumbprint(),
                analytics=Analysis(
                    metadata=analysis.metadata,
                    result=results,
                    status=analysis.status,
                ),
            )

    analysis_repository = FileSystemAnalysisRepository(ANALYTICS_NAME)
    return AnalysisService(
        analysis_repository=analysis_repository,
        analyzer=DummyLandCoverChangeAnalyzer(
            analysis_repository=analysis_repository,
        ),
        event=ANALYTICS_NAME,
    )


@router.post(
    "/analytics",
    response_class=ORJSONResponse,
    response_model=DataMartResourceLinkResponse,
    status_code=202,
)
async def create(
    *,
    data: LandCoverChangeAnalyticsIn,
    request: Request,
    background_tasks: BackgroundTasks,
    service: AnalysisService = Depends(create_analysis_service),
):
    return await create_analysis(
        data=data,
        service=service,
        request=request,
        background_tasks=background_tasks,
        resource_link_callback=_datamart_resource_link_response,
    )


def _datamart_resource_link_response(request, service) -> str:
    return str(
        request.url_for(
            "get_land_cover_change_analytics_result",
            resource_id=service.resource_thumbprint(),
        )
    )


@router.get(
    "/analytics/{resource_id}",
    response_class=ORJSONResponse,
    response_model=LandCoverChangeAnalyticsResponse,
    status_code=200,
)
async def get_land_cover_change_analytics_result(
    resource_id: UUID5,
    response: FastAPIResponse,
    analysis_repository: AnalysisRepository = Depends(get_analysis_repository),
):
    analytics_out: AnalyticsOut = await get_analysis(
        resource_id=resource_id,
        analysis_repository=analysis_repository,
        response=response,
    )

    return LandCoverChangeAnalyticsResponse(
        data=LandCoverChangeAnalytics(**analytics_out.model_dump()), status="success"
    )
