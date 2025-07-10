import json
import uuid
from pathlib import Path

from fastapi import APIRouter, Request, BackgroundTasks, HTTPException
from fastapi import Response as FastAPIResponse
from fastapi.responses import ORJSONResponse

from api.app.analysis.dist_alerts.analysis import do_analytics

from api.app.routers.models.common.analysis import AnalysisStatus
from api.app.routers.models.common.base import (
    DataMartResourceLinkResponse,
    DataMartResourceLink,
)
from api.app.routers.models.land_change.dist_alerts import (
    DistAlertsAnalyticsResponse,
    DistAlertsAnalyticsIn,
)


router = APIRouter(prefix="/dist_alerts")

PAYLOAD_STORE_DIR = Path("/tmp/dist_alerts_analytics_payloads")
PAYLOAD_STORE_DIR.mkdir(parents=True, exist_ok=True)


@router.post(
    "/analytics",
    response_class=ORJSONResponse,
    response_model=DataMartResourceLinkResponse,
    status_code=202,
)
def create(
    *, data: DistAlertsAnalyticsIn, request: Request, background_tasks: BackgroundTasks
):
    # Convert model to JSON with sorted keys
    payload_dict = data.model_dump()

    # Convert to JSON string with sorted keys
    payload_json = json.dumps(payload_dict, sort_keys=True)

    # Generate deterministic UUID from payload
    resource_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, payload_json))

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
        return DataMartResourceLinkResponse(data=link, status=AnalysisStatus.pending)

    payload_dir.mkdir(parents=True, exist_ok=True)
    metadata_data.write_text(payload_json)
    background_tasks.add_task(do_analytics, file_path=payload_dir)
    return DataMartResourceLinkResponse(data=link, status=AnalysisStatus.pending)


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
    print("In the GET")
    # Validate UUID format
    try:
        uuid.UUID(resource_id)
    except ValueError:
        raise HTTPException(
            status_code=400, detail="Invalid resource ID format. Must be a valid UUID."
        )

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

    if metadata_content:
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

    raise HTTPException(
        status_code=404,
        detail="Requested resource not found. Either expired or never existed.",
    )
