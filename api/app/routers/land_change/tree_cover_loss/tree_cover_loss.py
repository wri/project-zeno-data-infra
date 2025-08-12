import logging
import os
import traceback

from app.domain.analyzers.tree_cover_loss_analyzer import TreeCoverLossAnalyzer
from app.domain.models.analysis import Analysis
from app.domain.repositories.analysis_repository import AnalysisRepository
from app.infrastructure.external_services.data_api_compute_service import (
    DataApiComputeService,
)
from app.infrastructure.persistence.file_system_analysis_repository import (
    FileSystemAnalysisRepository,
)
from app.models.common.analysis import AnalysisStatus
from app.models.common.base import DataMartResourceLinkResponse
from app.models.land_change.tree_cover_loss import (
    TreeCoverLossAnalytics,
    TreeCoverLossAnalyticsIn,
    TreeCoverLossAnalyticsResponse,
)
from app.routers.common_analytics import create_analysis
from app.use_cases.analysis.analysis_service import AnalysisService
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request
from fastapi import Response as FastAPIResponse
from fastapi.responses import ORJSONResponse
from pydantic import UUID5

ANALYTICS_NAME = "tree_cover_loss"
router = APIRouter(prefix=f"/{ANALYTICS_NAME}")


def get_analysis_repository() -> AnalysisRepository:
    return FileSystemAnalysisRepository(ANALYTICS_NAME)


def create_analysis_service() -> AnalysisService:
    analysis_repository = FileSystemAnalysisRepository(ANALYTICS_NAME)
    return AnalysisService(
        analysis_repository=analysis_repository,
        analyzer=TreeCoverLossAnalyzer(
            analysis_repository=analysis_repository,
            compute_engine=DataApiComputeService(os.getenv("API_KEY")),
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
    data: TreeCoverLossAnalyticsIn,
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
            "get_tcl_analytics_result", resource_id=service.resource_thumbprint()
        )
    )


@router.get(
    "/analytics/{resource_id}",
    response_class=ORJSONResponse,
    response_model=TreeCoverLossAnalyticsResponse,
    status_code=200,
)
async def get_tcl_analytics_result(
    resource_id: UUID5,
    response: FastAPIResponse,
    analysis_repository: AnalysisRepository = Depends(get_analysis_repository),
):
    analysis: Analysis = Analysis(result=None, metadata=None, status=None)

    try:
        analysis = await analysis_repository.load_analysis(resource_id)
    except Exception as e:
        logging.error(
            {
                "event": "tree_cover_loss_analytics_resource_request_failure",
                "severity": "high",  # Helps with alerting
                "resource_id": resource_id,
                "resource_metadata": analysis.metadata,
                "error_type": e.__class__.__name__,  # e.g., "ValueError", "ConnectionError"
                "error_details": str(e),
                "traceback": traceback.format_exc(),
            }
        )
        raise HTTPException(status_code=500, detail="Internal server error")

    if analysis.status is None:
        raise HTTPException(status_code=404, detail="Analysis not found")

    message = ""

    if analysis.status == AnalysisStatus.pending:
        response.headers["Retry-After"] = "1"
        message = "Resource is still processing, follow Retry-After header."

    if analysis.status == AnalysisStatus.saved:
        message = "Analysis completed successfully."

    if analysis.status == AnalysisStatus.failed:
        message = "Analysis failed. Result is not available."

    return _tree_cover_loss_analytics_response(analysis, message)


def _tree_cover_loss_analytics_response(
    analysis: Analysis, message: str
) -> TreeCoverLossAnalyticsResponse:
    return TreeCoverLossAnalyticsResponse(
        data=TreeCoverLossAnalytics(
            status=analysis.status,
            message=message,
            result=analysis.result,
            metadata=analysis.metadata,
        ),
        status="success",
    )
