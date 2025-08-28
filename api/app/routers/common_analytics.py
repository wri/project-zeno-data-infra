import logging
import os
import traceback
from typing import Callable
from uuid import UUID

import duckdb
from app.domain.models.analysis import Analysis
from app.domain.repositories.analysis_repository import AnalysisRepository
from app.models.common.analysis import AnalysisStatus, AnalyticsIn, AnalyticsOut
from app.models.common.base import DataMartResourceLink, DataMartResourceLinkResponse
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
        background_tasks.add_task(test_s3_access)
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
) -> AnalyticsOut:
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
            message = ""

    return AnalyticsOut(
        status=analysis.status,
        message=message,
        result=analysis.result,
        metadata=analysis.metadata,
    )


# TODO - Remove this once duckdb's s3 access in zeno is verified
def test_s3_access():
    """Testing connectivity without breaking the existing deployment"""
    try:
        duckdb.query(
            """
            CREATE OR REPLACE SECRET secret (
                TYPE s3,
                PROVIDER credential_chain,
                CHAIN 'instance;env;config'
            );
        """
        )
        dataset = "s3://lcl-analytics/zonal-statistics/admin-dist_alerts.parquet"
        df = duckdb.sql(f"SELECT * FROM '{dataset}' LIMIT 2").df()
        logging.info({"event": "verify_s3_access_by_duckdb", "details": df})
    except Exception as e:
        logging.error(
            {
                "event": "s3_access_by_duckdb_failure",
                "severity": "high",
                "error_type": e.__class__.__name__,
                "environment_variables": f"KEY: {os.getenv('AWS_ACCESS_KEY_ID')}",
                "error_details": str(e),
                "traceback": traceback.format_exc(),
            }
        )
