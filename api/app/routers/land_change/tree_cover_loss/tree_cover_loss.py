import logging
import traceback
import uuid
from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from fastapi import Response as FastAPIResponse
from fastapi.responses import ORJSONResponse
from app.models.land_change.tree_cover_loss import (
    TreeCoverLossAnalyticsIn,
    TreeCoverLossAnalyticsResponse, TreeCoverLossAnalytics,
)
from app.models.common.base import DataMartResourceLinkResponse, DataMartResourceLink
from app.use_cases.analysis.tree_cover_loss.tree_cover_loss_service import (
    TreeCoverLossService,
)


router = APIRouter(prefix="/tree_cover_loss")


@router.post(
    "/analytics",
    response_class=ORJSONResponse,
    response_model=DataMartResourceLinkResponse,
    status_code=202,
)
def create(
    *,
    data: TreeCoverLossAnalyticsIn,
    request: Request,
    background_tasks: BackgroundTasks,
):
    try:
        service = TreeCoverLossService(background_tasks)
        service.do(data)

        link = DataMartResourceLink(
            link=f"{str(request.base_url).rstrip('/')}/v0/land_change/tree_cover_loss/analytics/{data.thumbprint()}"
        )

        return DataMartResourceLinkResponse(data=link, status=service.get_status())
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


@router.get(
    "/analytics/{resource_id}",
    response_class=ORJSONResponse,
    response_model=TreeCoverLossAnalyticsResponse,
    status_code=200,
)
async def get_analytics_result(
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
