import logging
import traceback
from typing import Callable

from app.models.common.analysis import AnalyticsIn
from app.models.common.base import DataMartResourceLink, DataMartResourceLinkResponse
from app.use_cases.analysis.analysis_service import AnalysisService
from fastapi import BackgroundTasks, HTTPException, Request


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
