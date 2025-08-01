import logging
import traceback
import uuid

from app.infrastructure.persistence.file_resource import load_resource
from app.models.common.analysis import AnalysisStatus
from app.models.common.base import DataMartResourceLink, DataMartResourceLinkResponse
from app.models.land_change.land_cover import (
    LandCoverChangeAnalyticsIn,
    LandCoverChangeAnalyticsResponse,
)
from app.use_cases.analysis.land_cover.land_cover_change import (
    LandCoverChangeService,
)
from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from fastapi import Response as FastAPIResponse
from fastapi.responses import ORJSONResponse

router = APIRouter(prefix="/land_cover_change")


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
):
    try:
        service = LandCoverChangeService()
        background_tasks.add_task(service.do, data)

        resource_id = data.thumbprint()
        resource = await load_resource(resource_id)

        link_url = request.url_for(
            "get_land_cover_change_analytics_result", resource_id=resource_id
        )
        link = DataMartResourceLink(link=str(link_url))

        return DataMartResourceLinkResponse(data=link, status=resource.status)
    except Exception as e:
        logging.error(
            {
                "event": "land_cover_change_analytics_processing_failure",
                "severity": "high",
                "error_type": e.__class__.__name__,
                "error_details": str(e),
                "stack_trace": traceback.format_exc(),
            }
        )
        raise HTTPException(status_code=500, detail="Internal Server Error")


@router.get(
    "/analytics/{resource_id}",
    response_class=ORJSONResponse,
    response_model=LandCoverChangeAnalyticsResponse,
    status_code=200,
)
async def get_land_cover_change_analytics_result(
    resource_id: str,
    response: FastAPIResponse,
    background_tasks: BackgroundTasks,
):
    # Validate UUID format
    try:
        uuid.UUID(resource_id)
    except ValueError:
        raise HTTPException(
            status_code=400, detail="Invalid resource ID format. Must be a valid UUID."
        )

    try:
        service = LandCoverChangeService()
        resource = await service.get(resource_id)

        if resource.status == AnalysisStatus.pending:
            response.headers["Retry-After"] = "1"

        return LandCoverChangeAnalyticsResponse(data=resource)
    except Exception as e:
        logging.error(
            {
                "event": "land_cover_change_analytics_resource_request_failure",
                "severity": "high",
                "resource_id": resource_id,
                "resource_metadata": None,
                "error_type": e.__class__.__name__,
                "error_details": str(e),
                "traceback": traceback.format_exc(),
            }
        )
        raise HTTPException(status_code=500, detail="Internal server error")
