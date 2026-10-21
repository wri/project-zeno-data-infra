import os

import boto3
from async_lru import alru_cache
from cachetools.func import ttl_cache
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request
from fastapi import Response as FastAPIResponse
from fastapi.responses import ORJSONResponse
from pydantic import UUID5

from app.dependencies import get_environment
from app.domain.analyzers.integrated_alerts_analyzer import (
    IntegratedAlertsAnalyzer,
    build_input_uris,
)
from app.domain.analyzers.integrated_alerts_points_analyzer import (
    IntegratedAlertsPointsAnalyzer,
    build_points_input_uris,
)
from app.domain.models.environment import Environment
from app.domain.repositories.analysis_repository import AnalysisRepository
from app.infrastructure.external_services.duck_db_query_service import (
    DuckDbPrecalcQueryService,
)
from app.infrastructure.persistence.aws_dynamodb_s3_analysis_repository import (
    AwsDynamoDbS3AnalysisRepository,
)
from app.models.common.analysis import AnalyticsOut
from app.models.common.base import DataMartResourceLinkResponse
from app.models.land_change.integrated_alerts import (
    ANALYTICS_NAME,
    POINTS_ANALYTICS_NAME,
    IntegratedAlertsAnalytics,
    IntegratedAlertsAnalyticsIn,
    IntegratedAlertsAnalyticsResponse,
    IntegratedAlertsLatest,
    IntegratedAlertsLatestResponse,
    IntegratedAlertsPoints,
    IntegratedAlertsPointsAnalyticsIn,
    IntegratedAlertsPointsResponse,
    decode_cursor,
)
from app.routers.common_analytics import create_analysis, get_analysis
from app.use_cases.analysis.analysis_service import AnalysisService
from app.use_cases.integrated_alerts.latest_alert_date import (
    IntegratedAlertsLatestDate,
    LatestAlertDateNotFound,
)

router = APIRouter(prefix=f"/{ANALYTICS_NAME}")


def get_analysis_repository(request: Request) -> AnalysisRepository:
    return AwsDynamoDbS3AnalysisRepository(
        ANALYTICS_NAME, request.app.state.dynamodb_table, request.app.state.s3_client
    )


@ttl_cache(maxsize=1, ttl=3600)
def get_latest_integrated_alerts_version() -> str:
    client = boto3.client("s3")
    try:
        response = client.get_object(
            Bucket="lcl-analytics", Key="zonal-statistics/integrated-alerts/latest"
        )
    except client.exceptions.NoSuchKey as error:
        raise HTTPException(
            status_code=503,
            detail="Integrated alerts 'latest' version marker not found in S3.",
        ) from error
    return response["Body"].read().decode("utf-8").strip()


def create_analysis_service(
    request: Request,
    analysis_repository: AnalysisRepository = Depends(get_analysis_repository),
    environment: Environment = Depends(get_environment),
    version: str = Depends(get_latest_integrated_alerts_version),
) -> AnalysisService:
    input_uris = build_input_uris(version, environment)
    return AnalysisService(
        analysis_repository=analysis_repository,
        analyzer=IntegratedAlertsAnalyzer(
            compute_engine=request.app.state.dask_client,
            duckdb_query_service=DuckDbPrecalcQueryService(
                table_uri=input_uris["admin_results_table_uri"]
            ),
            input_uris=input_uris,
            otf_timeout_seconds=float(os.environ.get("OTF_TIMEOUT_SECONDS", 600)),
        ),
        event=ANALYTICS_NAME,
    )


@router.post(
    "/analytics",
    response_class=ORJSONResponse,
    response_model=DataMartResourceLinkResponse,
    status_code=202,
    summary="Create Integrated Alerts Analysis Task",
)
async def create(
    *,
    data: IntegratedAlertsAnalyticsIn,
    request: Request,
    background_tasks: BackgroundTasks,
    service: AnalysisService = Depends(create_analysis_service),
    version: str = Depends(get_latest_integrated_alerts_version),
):
    data._version = version
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
            "get_integrated_alerts_analytics_result",
            resource_id=service.resource_thumbprint(),
        )
    )


@router.get(
    "/analytics/{resource_id}",
    response_class=ORJSONResponse,
    response_model=IntegratedAlertsAnalyticsResponse,
    status_code=200,
)
async def get_integrated_alerts_analytics_result(
    resource_id: UUID5,
    response: FastAPIResponse,
    analysis_repository: AnalysisRepository = Depends(get_analysis_repository),
):
    analytics_out: AnalyticsOut = await get_analysis(
        resource_id=resource_id,
        analysis_repository=analysis_repository,
        response=response,
    )

    return IntegratedAlertsAnalyticsResponse(
        data=IntegratedAlertsAnalytics(**analytics_out.model_dump()), status="success"
    )


# A version's latest alert date never changes, so cache by version.
@alru_cache(maxsize=1)
async def get_latest_alert_date(
    version: str = Depends(get_latest_integrated_alerts_version),
    environment: Environment = Depends(get_environment),
) -> str:
    input_uris = build_input_uris(version, environment)
    try:
        return await IntegratedAlertsLatestDate(
            DuckDbPrecalcQueryService(table_uri=input_uris["admin_results_table_uri"])
        ).get()
    except LatestAlertDateNotFound as error:
        raise HTTPException(
            status_code=503,
            detail="No integrated alerts dates found for the latest version.",
        ) from error


@router.get(
    "/latest",
    response_class=ORJSONResponse,
    response_model=IntegratedAlertsLatestResponse,
    status_code=200,
    summary="Get Latest Integrated Alerts Date",
)
async def get_latest(
    latest_alert_date: str = Depends(get_latest_alert_date),
    version: str = Depends(get_latest_integrated_alerts_version),
):
    return IntegratedAlertsLatestResponse(
        data=IntegratedAlertsLatest(alert_date=latest_alert_date, version=version)
    )


def get_points_analysis_repository(request: Request) -> AnalysisRepository:
    return AwsDynamoDbS3AnalysisRepository(
        POINTS_ANALYTICS_NAME,
        request.app.state.dynamodb_table,
        request.app.state.s3_client,
    )


async def get_points_request_version(
    request: Request,
    latest_version: str = Depends(get_latest_integrated_alerts_version),
) -> str:
    """The alerts version a points request is served from: a cursor's own version,
    so a list keeps paging through the version it started on, else the latest one.
    The pipeline builds a version's points table before marking it latest."""
    try:
        body = await request.json()
    except ValueError:
        return latest_version
    raw_cursor = body.get("cursor") if isinstance(body, dict) else None
    if not raw_cursor:
        return latest_version
    try:
        return decode_cursor(raw_cursor).version
    except ValueError:
        # Request validation reports the malformed cursor as a 422.
        return latest_version


def create_points_analysis_service(
    analysis_repository: AnalysisRepository = Depends(get_points_analysis_repository),
    version: str = Depends(get_points_request_version),
) -> AnalysisService:
    return AnalysisService(
        analysis_repository=analysis_repository,
        analyzer=IntegratedAlertsPointsAnalyzer(
            input_uris=build_points_input_uris(version)
        ),
        event=POINTS_ANALYTICS_NAME,
    )


@router.post(
    "/points",
    response_class=ORJSONResponse,
    response_model=DataMartResourceLinkResponse,
    status_code=202,
    summary="Create Integrated Alerts Points Task",
    description=(
        "List the newest integrated alerts in one AOI (one id, or one feature), "
        "one row per alert pixel with its pixel center (lat, lon), date, "
        "confidence and SBTN natural lands class, newest date first. Only the "
        "most recent alerts are stored; the result metadata gives the stored and "
        "served date ranges, whether the list was cut at `limit`, and a "
        "`next_cursor` to pass as `cursor` for the next page."
    ),
)
async def create_points(
    *,
    data: IntegratedAlertsPointsAnalyticsIn,
    request: Request,
    background_tasks: BackgroundTasks,
    service: AnalysisService = Depends(create_points_analysis_service),
    version: str = Depends(get_points_request_version),
):
    data._version = version
    return await create_analysis(
        data=data,
        service=service,
        request=request,
        background_tasks=background_tasks,
        resource_link_callback=_points_resource_link_response,
    )


def _points_resource_link_response(request, service) -> str:
    return str(
        request.url_for(
            "get_integrated_alerts_points_result",
            resource_id=service.resource_thumbprint(),
        )
    )


@router.get(
    "/points/{resource_id}",
    response_class=ORJSONResponse,
    response_model=IntegratedAlertsPointsResponse,
    status_code=200,
)
async def get_integrated_alerts_points_result(
    resource_id: UUID5,
    response: FastAPIResponse,
    analysis_repository: AnalysisRepository = Depends(get_points_analysis_repository),
):
    analytics_out: AnalyticsOut = await get_analysis(
        resource_id=resource_id,
        analysis_repository=analysis_repository,
        response=response,
    )

    return IntegratedAlertsPointsResponse(
        data=IntegratedAlertsPoints(**analytics_out.model_dump()), status="success"
    )
