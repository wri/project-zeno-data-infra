from fastapi import APIRouter, BackgroundTasks, Depends, Request
from fastapi import Response as FastAPIResponse
from fastapi.responses import ORJSONResponse
from pydantic import UUID5

from app.dependencies import get_environment
from app.domain.analyzers.land_ghg_inventory_analyzer import (
    INPUT_URIS,
    LandGhgInventoryAnalyzer,
)
from app.domain.models.environment import Environment, resolve_uris
from app.domain.repositories.analysis_repository import AnalysisRepository
from app.infrastructure.persistence.aws_dynamodb_s3_analysis_repository import (
    AwsDynamoDbS3AnalysisRepository,
)
from app.models.common.analysis import AnalyticsOut
from app.models.common.base import DataMartResourceLinkResponse
from app.models.land_change.land_ghg_inventory import (
    ANALYTICS_NAME,
    LandGhgInventoryAnalytics,
    LandGhgInventoryAnalyticsIn,
    LandGhgInventoryAnalyticsResponse,
)
from app.routers.common_analytics import create_analysis, get_analysis
from app.use_cases.analysis.analysis_service import AnalysisService

router = APIRouter(prefix=f"/{ANALYTICS_NAME}")


def get_analysis_repository(request: Request) -> AnalysisRepository:
    return AwsDynamoDbS3AnalysisRepository(
        ANALYTICS_NAME, request.app.state.dynamodb_table, request.app.state.s3_client
    )


def create_analysis_service(
    analysis_repository: AnalysisRepository = Depends(get_analysis_repository),
    environment: Environment = Depends(get_environment),
) -> AnalysisService:
    return AnalysisService(
        analysis_repository=analysis_repository,
        analyzer=LandGhgInventoryAnalyzer(
            input_uris=resolve_uris(INPUT_URIS, environment),
        ),
        event=ANALYTICS_NAME,
    )


@router.post(
    "/analytics",
    response_class=ORJSONResponse,
    response_model=DataMartResourceLinkResponse,
    status_code=202,
    summary="Create Land GHG Inventory Analysis Task",
)
async def create(
    *,
    data: LandGhgInventoryAnalyticsIn,
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
            "get_land_ghg_inventory_analytics_result",
            resource_id=service.resource_thumbprint(),
        )
    )


@router.get(
    "/analytics/{resource_id}",
    response_class=ORJSONResponse,
    response_model=LandGhgInventoryAnalyticsResponse,
    status_code=200,
)
async def get_land_ghg_inventory_analytics_result(
    resource_id: UUID5,
    response: FastAPIResponse,
    analysis_repository: AnalysisRepository = Depends(get_analysis_repository),
):
    analytics_out: AnalyticsOut = await get_analysis(
        resource_id=resource_id,
        analysis_repository=analysis_repository,
        response=response,
    )

    return LandGhgInventoryAnalyticsResponse(
        data=LandGhgInventoryAnalytics(**analytics_out.model_dump()), status="success"
    )
