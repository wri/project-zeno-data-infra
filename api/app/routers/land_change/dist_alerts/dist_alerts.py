import json
import logging
import traceback
import uuid
from pathlib import Path

from app.analysis.dist_alerts.analysis import do_analytics
from app.models.common.analysis import AnalysisStatus
from app.models.common.base import (
    DataMartResourceLink,
    DataMartResourceLinkResponse,
)
from app.models.land_change.dist_alerts import (
    DistAlertsAnalyticsIn,
    DistAlertsAnalyticsResponse,
)
from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from fastapi import Response as FastAPIResponse
from fastapi.responses import ORJSONResponse

router = APIRouter(prefix="/dist_alerts")

PAYLOAD_STORE_DIR = Path("/tmp/dist_alerts_analytics_payloads")
PAYLOAD_STORE_DIR.mkdir(parents=True, exist_ok=True)


@router.post(
    "/analytics",
    response_class=ORJSONResponse,
    response_model=DataMartResourceLinkResponse,
    status_code=202,
)
async def create(
    *, data: DistAlertsAnalyticsIn, request: Request, background_tasks: BackgroundTasks
):
    try:
        # Convert model to JSON with sorted keys
        payload_dict = data.model_dump()

        # Convert to JSON string with sorted keys
        payload_json = json.dumps(payload_dict, sort_keys=True)

        # Generate deterministic UUID from payload
        resource_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, payload_json))

        logging.info(
            {
                "event": "dist_alerts_analytics_request",
                "analytics_in": payload_dict,
                "resource_id": resource_id,
            }
        )

        # Store payload in /tmp directory
        payload_dir = PAYLOAD_STORE_DIR / resource_id
        metadata_data = payload_dir / "metadata.json"
        analytics_data = payload_dir / "data.json"

        link = DataMartResourceLink(
            link=f"{str(request.base_url).rstrip('/')}/v0/land_change/dist_alerts/analytics/{resource_id}"
        )

        if metadata_data.exists() and analytics_data.exists():
            return DataMartResourceLinkResponse(data=link, status=AnalysisStatus.saved)

        if metadata_data.exists():
            return DataMartResourceLinkResponse(
                data=link, status=AnalysisStatus.pending
            )

        payload_dir.mkdir(parents=True, exist_ok=True)
        metadata_data.write_text(payload_json)
        background_tasks.add_task(
            do_analytics,
            file_path=payload_dir,
            dask_client=request.app.state.dask_client,
        )
        return DataMartResourceLinkResponse(data=link, status=AnalysisStatus.pending)
    except Exception as e:
        logging.error(
            {
                "event": "dist_alerts_analytics_request_failure",
                "severity": "high",  # Helps with alerting
                "analytics_in": await request.json(),
                "resource_id": resource_id,
                "error_type": e.__class__.__name__,  # e.g., "ValueError", "ConnectionError"
                "error_details": str(e),
                "traceback": traceback.format_exc(),
            }
        )
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/analytics/{resource_id}",
    response_class=ORJSONResponse,
    response_model=DistAlertsAnalyticsResponse,
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
        # Construct file path
        file_path = PAYLOAD_STORE_DIR / resource_id
        analytics_metadata = file_path / "metadata.json"
        analytics_data = file_path / "data.json"
        metadata_content = None
        alerts_dict = None

        if analytics_metadata.exists() and analytics_data.exists():
            # load resource from filesystem
            alerts_dict = json.loads(analytics_data.read_text())
            metadata_content = json.loads(analytics_metadata.read_text())

            return DistAlertsAnalyticsResponse(
                data={
                    "result": alerts_dict,
                    "metadata": metadata_content,
                    "status": AnalysisStatus.saved,
                },
                status="success",
            )

        if analytics_metadata.exists():
            metadata_content = json.loads(analytics_metadata.read_text())
            response.headers["Retry-After"] = "1"

            return DistAlertsAnalyticsResponse(
                data={
                    "status": AnalysisStatus.pending,
                    "message": "Resource is still processing, follow Retry-After header.",
                    "result": alerts_dict,
                    "metadata": metadata_content,
                },
                status="success",
            )

    except Exception as e:
        logging.error(
            {
                "event": "dist_alerts_analytics_resource_request_failure",
                "severity": "high",  # Helps with alerting
                "resource_id": resource_id,
                "resource_metadata": metadata_content,
                "error_type": e.__class__.__name__,  # e.g., "ValueError", "ConnectionError"
                "error_details": str(e),
                "traceback": traceback.format_exc(),
            }
        )
        raise HTTPException(status_code=500, detail="Internal server error")

    # Should've found the resource by now. So, assume it doesn't exist
    raise HTTPException(
        status_code=404,
        detail="Requested resource not found. Either expired or never existed.",
    )
