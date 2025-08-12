import logging
import traceback
from typing import Callable
from uuid import UUID

from app.domain.models.analysis import Analysis
from app.domain.repositories.analysis_repository import AnalysisRepository
from app.models.common.analysis import AnalysisStatus, AnalyticsIn
from app.models.common.base import DataMartResourceLink, DataMartResourceLinkResponse
from app.models.land_change.tree_cover_loss import (
    TreeCoverLossAnalytics,
    TreeCoverLossAnalyticsResponse,
)
from app.use_cases.analysis.analysis_service import AnalysisService
from fastapi import BackgroundTasks, HTTPException, Request
from fastapi import Response as FastAPIResponse


async def create_analysis(
    data: AnalyticsIn,
    service: AnalysisService,
    background_tasks: BackgroundTasks,
    request: Request,
    resource_link_callback: Callable,
) -> DataMartResourceLinkResponse:
    try:
        logging.info(
            {
                "event": f"{service.event_name()}_analytics_request",
                "analytics_in": data.model_dump(),
                "resource_id": data.thumbprint(),
            }
        )

        await service.set_resource_from(data)
        background_tasks.add_task(service.do)
        link_url = resource_link_callback(request=request, service=service)
        link = DataMartResourceLink(link=link_url)
        return DataMartResourceLinkResponse(data=link, status=service.get_status())

    except Exception as e:
        logging.error(
            {
                "event": f"{service.event_name()}_analytics_processing_error",
                "severity": "high",
                "error_type": e.__class__.__name__,
                "error_details": str(e),
                "stack_trace": traceback.format_exc(),
            }
        )
        raise HTTPException(status_code=500, detail="Internal Server Error")


async def get_analysis(
    resource_id: UUID,
    analysis_repository: AnalysisRepository,
    response: FastAPIResponse,
):
    analysis: Analysis = Analysis(result=None, metadata=None, status=None)

    try:
        analysis = await analysis_repository.load_analysis(resource_id)
    except Exception as e:
        logging.error(
            {
                "event": "tree_cover_loss_analytics_resource_request_failure",
                "severity": "high",
                "resource_id": resource_id,
                "resource_metadata": analysis.metadata,
                "error_type": e.__class__.__name__,
                "error_details": str(e),
                "traceback": traceback.format_exc(),
            }
        )
        raise HTTPException(status_code=500, detail="Internal server error")

    if analysis.status is None:
        raise HTTPException(status_code=404, detail="Analysis not found")

    match analysis.status:
        case AnalysisStatus.pending:
            response.headers["Retry-After"] = "1"
            message = "Resource is still processing, follow Retry-After header."
        case AnalysisStatus.saved:
            message = "Analysis completed successfully."
        case AnalysisStatus.failed:
            message = "Analysis failed. Result is not available."
        case _:
            message = ""  # Optional default

    return TreeCoverLossAnalyticsResponse(
        data=TreeCoverLossAnalytics(
            status=analysis.status,
            message=message,
            result=analysis.result,
            metadata=analysis.metadata,
        ),
        status="success",
    )
