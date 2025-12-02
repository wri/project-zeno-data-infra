from fastapi import APIRouter, BackgroundTasks, Depends, Request
from fastapi import Response as FastAPIResponse
from fastapi.responses import ORJSONResponse
from pydantic import UUID5

from app.domain.analyzers.dist_alerts_analyzer import DistAlertsAnalyzer
from app.domain.repositories.analysis_repository import AnalysisRepository
from app.infrastructure.persistence.aws_dynamodb_s3_analysis_repository import (
    AwsDynamoDbS3AnalysisRepository,
)
from app.models.common.analysis import AnalyticsOut
from app.models.common.base import (
    DataMartResourceLinkResponse,
)
from app.models.land_change.dist_alerts import (
    ANALYTICS_NAME,
    DistAlertsAnalytics,
    DistAlertsAnalyticsIn,
    DistAlertsAnalyticsResponse,
)
from app.routers.common_analytics import create_analysis, get_analysis
from app.use_cases.analysis.analysis_service import AnalysisService

router = APIRouter(prefix=f"/{ANALYTICS_NAME}")


def get_analysis_repository(request: Request) -> AnalysisRepository:
    return AwsDynamoDbS3AnalysisRepository(
        ANALYTICS_NAME, request.app.state.dynamodb_table, request.app.state.s3_client
    )


def create_analysis_service(
    request: Request, analysis_repository=Depends(get_analysis_repository)
) -> AnalysisService:
    return AnalysisService(
        analysis_repository=analysis_repository,
        analyzer=DistAlertsAnalyzer(
            analysis_repository=analysis_repository,
            compute_engine=getattr(request.app.state, "dask_client", None),
        ),
        event=ANALYTICS_NAME,
    )


async def get_latest_dist_version(request: Request) -> str:
    try:
        response = await request.app.state.s3_client.get_object(
            Bucket="lcl-analytics", Key="zonal-statistics/dist-alerts/latest"
        )
        async with response["Body"] as stream:
            content = await stream.read()
        return content.decode("utf-8").strip()
    except request.app.state.s3_client.exceptions.NoSuchKey:
        return "v20251004"


def _datamart_resource_link_response(request, service) -> str:
    return str(
        request.url_for(
            "get_dist_alerts_analytics_result",
            resource_id=service.resource_thumbprint(),
        )
    )


@router.post(
    "/analytics",
    response_class=ORJSONResponse,
    response_model=DataMartResourceLinkResponse,
    status_code=202,
    summary="Create Disturbance Alerts Analysis Task",
)
async def create(
    *,
    data: DistAlertsAnalyticsIn,
    request: Request,
    background_tasks: BackgroundTasks,
    service: AnalysisService = Depends(create_analysis_service),
    latest_version: str = Depends(get_latest_dist_version),
):

    created_data = DistAlertsAnalyticsIn(**data.model_dump())
    created_data._version = latest_version  # Direct assignment to PrivateAttr
    return await create_analysis(
        data=created_data,
        service=service,
        request=request,
        background_tasks=background_tasks,
        resource_link_callback=_datamart_resource_link_response,
    )


@router.get(
    "/analytics/{resource_id}",
    response_class=ORJSONResponse,
    response_model=DistAlertsAnalyticsResponse,
    status_code=200,
)
async def get_dist_alerts_analytics_result(
    resource_id: UUID5,
    response: FastAPIResponse,
    analysis_repository: AnalysisRepository = Depends(get_analysis_repository),
):
    analytics_out: AnalyticsOut = await get_analysis(
        resource_id=resource_id,
        analysis_repository=analysis_repository,
        response=response,
    )

    return DistAlertsAnalyticsResponse(
        data=DistAlertsAnalytics(**analytics_out.model_dump()), status="success"
    )
