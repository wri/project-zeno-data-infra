import logging
import os
import traceback
import uuid
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request
from fastapi import Response as FastAPIResponse
from fastapi.responses import ORJSONResponse

from app.models.common.base import DataMartResourceLink, DataMartResourceLinkResponse
from app.models.common.analysis import AnalysisStatus
from app.models.land_change.tree_cover_loss import (
    TreeCoverLossAnalytics,
    TreeCoverLossAnalyticsIn,
    TreeCoverLossAnalyticsResponse,
)
from app.use_cases.analysis.tree_cover_loss.tree_cover_loss_service import TreeCoverLossService
from app.domain.models.analysis import Analysis
from app.domain.repositories.analysis_repository import AnalysisRepository
from app.domain.analyzers.tree_cover_loss_analyzer import TreeCoverLossAnalyzer
from app.infrastructure.external_services.compute_service import ComputeService
from app.infrastructure.persistence.file_system_analysis_repository import FileSystemAnalysisRepository
from app.infrastructure.external_services.data_api_compute_service import DataApiComputeService

router = APIRouter(prefix="/tree_cover_loss")

def get_analysis_repository() -> AnalysisRepository:
    return FileSystemAnalysisRepository('tree_cover_loss_analysis')

def get_analyzer() -> TreeCoverLossAnalyzer:
    return TreeCoverLossAnalyzer(
        analysis_repository=get_analysis_repository(),
        compute_engine=DataApiComputeService(os.getenv('API_KEY'))
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
    analysis_repository: AnalysisRepository = Depends(get_analysis_repository),
    analyzer: TreeCoverLossAnalyzer = Depends(get_analyzer),
):
    try:
        logging.info(
            {
                "event": "tree_cover_loss_analytics_request",
                "analytics_in": data.model_dump(),
                "resource_id": data.thumbprint(),
            }
        )

        service = TreeCoverLossService(
            analysis_repository=analysis_repository,
            analyzer=analyzer,
        )
        await service.set_resource_from(data)
        background_tasks.add_task(service.do)
        return _datamart_resource_link_response(request, service)
    except Exception as e:
        logging.error(
            {
                "event": "tree_cover_loss_analytics_processing_failure",
                "severity": "high",  # Helps with alerting
                "error_type": e.__class__.__name__,  # e.g., "ValueError", "ConnectionError"
                "error_details": str(e),
                "stack_trace": traceback.format_exc(),
            }
        )
        raise HTTPException(status_code=500, detail="Internal Server Error")


def _datamart_resource_link_response(request, service) -> DataMartResourceLinkResponse:
    link_url = request.url_for(
        "get_tcl_analytics_result", resource_id=service.resource_thumbprint()
    )
    link = DataMartResourceLink(link=str(link_url))
    return DataMartResourceLinkResponse(data=link, status=service.get_status())


@router.get(
    "/analytics/{resource_id}",
    response_class=ORJSONResponse,
    response_model=TreeCoverLossAnalyticsResponse,
    status_code=200,
)
async def get_tcl_analytics_result(
    resource_id: str,
    response: FastAPIResponse,
    analysis_repository: AnalysisRepository = Depends(get_analysis_repository),
):
    # Validate UUID format
    try:
        uuid.UUID(resource_id)
    except ValueError:
        raise HTTPException(
            status_code=400, detail="Invalid resource ID format. Must be a valid UUID."
        )

    analysis: Analysis | None = None

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

def _tree_cover_loss_analytics_response(analysis: Analysis, message: str) -> TreeCoverLossAnalyticsResponse:
        return TreeCoverLossAnalyticsResponse(
            data=TreeCoverLossAnalytics(
                status=analysis.status,
                message=message,
                result=analysis.result,
                metadata=analysis.metadata,
            ),
            status="success",
        )