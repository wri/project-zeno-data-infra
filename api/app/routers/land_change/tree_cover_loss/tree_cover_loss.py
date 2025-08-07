import logging
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


router = APIRouter(prefix="/tree_cover_loss")

def get_analysis_repository() -> AnalysisRepository:
    class FakeAnalysisRepository(AnalysisRepository):
        async def load_analysis(self, resource_id: uuid.UUID) -> Analysis:
            return Analysis(status=AnalysisStatus.pending, metadata=None, result=None)
        async def store_analysis(self, resource_id: uuid.UUID, analytics: Analysis):
            pass
    return FakeAnalysisRepository()

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
    analysis_repository: AnalysisRepository = Depends(get_analysis_repository)
):
    try:
        service = TreeCoverLossService(analysis_repository=analysis_repository)
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
):
    # Validate UUID format
    try:
        uuid.UUID(resource_id)
    except ValueError:
        raise HTTPException(
            status_code=400, detail="Invalid resource ID format. Must be a valid UUID."
        )

    try:
        service = TreeCoverLossService()
        response.headers["Retry-After"] = "1"

        return TreeCoverLossAnalyticsResponse(
            data=TreeCoverLossAnalytics(
                status=service.get_status(),
                message="Resource is still processing, follow Retry-After header.",
                result=None,
                metadata=None,
            ),
            status="success",
        )
    except Exception as e:
        logging.error(
            {
                "event": "tree_cover_loss_analytics_resource_request_failure",
                "severity": "high",  # Helps with alerting
                "resource_id": resource_id,
                "resource_metadata": None,
                "error_type": e.__class__.__name__,  # e.g., "ValueError", "ConnectionError"
                "error_details": str(e),
                "traceback": traceback.format_exc(),
            }
        )
        raise HTTPException(status_code=500, detail="Internal server error")
