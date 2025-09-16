from app.domain.analyzers.land_cover_change_analyzer import LandCoverChangeAnalyzer
from app.domain.repositories.analysis_repository import AnalysisRepository
from app.infrastructure.external_services.duck_db_query_service import (
    DuckDbPrecalcQueryService,
)
from app.infrastructure.persistence.aws_dynamodb_s3_analysis_repository import (
    AwsDynamoDbS3AnalysisRepository,
)
from app.models.common.analysis import AnalyticsOut
from app.models.common.base import DataMartResourceLinkResponse
from app.models.land_change.land_cover_change import (
    ANALYTICS_NAME,
    LandCoverChangeAnalytics,
    LandCoverChangeAnalyticsIn,
    LandCoverChangeAnalyticsResponse,
)
from app.routers.common_analytics import create_analysis, get_analysis
from app.use_cases.analysis.analysis_service import AnalysisService
from fastapi import APIRouter, BackgroundTasks, Depends, Request
from fastapi import Response as FastAPIResponse
from fastapi.responses import ORJSONResponse
from pydantic import UUID5

router = APIRouter(prefix=f"/{ANALYTICS_NAME}")


def get_analysis_repository(request: Request) -> AnalysisRepository:
    return AwsDynamoDbS3AnalysisRepository(
        ANALYTICS_NAME, request.app.state.dynamodb_table, request.app.state.s3_client
    )


def create_analysis_service(
    request: Request,
    analysis_repository: AnalysisRepository = Depends(get_analysis_repository),
) -> AnalysisService:
    return AnalysisService(
        analysis_repository=analysis_repository,
        analyzer=LandCoverChangeAnalyzer(
            analysis_repository=analysis_repository,
            compute_engine=request.app.state.dask_client,
            query_service=DuckDbPrecalcQueryService(
                table_uri="s3://lcl-analytics/zonal-statistics/admin-land-cover-change.parquet"
            ),
        ),
        event=ANALYTICS_NAME,
    )


@router.post(
    "/analytics",
    response_class=ORJSONResponse,
    response_model=DataMartResourceLinkResponse,
    status_code=202,
    summary="Create Land Cover Change Analysis Task",
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
