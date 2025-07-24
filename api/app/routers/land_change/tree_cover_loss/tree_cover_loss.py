import uuid
from pathlib import Path

from app.models.land_change.tree_cover_loss import TreeCoverLossAnalyticsIn
from app.models.common.base import DataMartResourceLinkResponse

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from fastapi import Response as FastAPIResponse
from fastapi.responses import ORJSONResponse

from app.models.common.base import StrictBaseModel
from app.use_cases.analysis.tree_cover_loss.tree_cover_loss_service import TreeCoverLossService


router = APIRouter(prefix="/tree_cover_loss")

# PAYLOAD_STORE_DIR = Path("/tmp/tree_cover_loss_analytics_payloads")
# PAYLOAD_STORE_DIR.mkdir(parents=True, exist_ok=True)


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
    service = TreeCoverLossService()
    service.do(data)

    # # Convert model to JSON with sorted keys
    # payload_dict = data.model_dump()
    #
    # # Convert to JSON string with sorted keys
    # payload_json = json.dumps(payload_dict, sort_keys=True)
    #
    # # Generate deterministic UUID from payload
    # resource_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, payload_json))
    #
    # # Store payload in /tmp directory
    # payload_dir = PAYLOAD_STORE_DIR / resource_id
    # metadata_data = payload_dir / "metadata.json"
    # analytics_data = payload_dir / "data.json"
    #
    # link = DataMartResourceLink(
    #     link=f"{str(request.base_url).rstrip('/')}/v0/land_change/tree_cover_loss/analytics/{resource_id}"
    # )
    #
    # if metadata_data.exists() and analytics_data.exists():
    #     return DataMartResourceLinkResponse(data=link, status=AnalysisStatus.saved)
    #
    # if metadata_data.exists():
    #     return DataMartResourceLinkResponse(data=link, status=AnalysisStatus.pending)
    #
    # payload_dir.mkdir(parents=True, exist_ok=True)
    # metadata_data.write_text(payload_json)
    # background_tasks.add_task(do_analytics, file_path=payload_dir)
    raise HTTPException(status_code=501, detail="Not Implemented")


class TreeCoverLossAnalyticsResponse(StrictBaseModel):
    pass


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

    # # Construct file path
    # file_path = PAYLOAD_STORE_DIR / resource_id
    # analytics_metadata = file_path / "metadata.json"
    # analytics_data = file_path / "data.json"
    # metadata_content = None
    # results_dict = None
    #
    # if analytics_metadata.exists() and analytics_data.exists():
    #     # load resource from filesystem
    #     results_dict = json.loads(analytics_data.read_text())
    #     metadata_content = json.loads(analytics_metadata.read_text())
    #
    #     return TreeCoverLossAnalyticsResponse(
    #         # data={
    #         #     "result": results_dict,
    #         #     "metadata": metadata_content,
    #         #     "status": AnalysisStatus.saved,
    #         # },
    #         # status="success",
    #     )
    #
    # if analytics_metadata.exists():
    #     metadata_content = json.loads(analytics_metadata.read_text())
    #     response.headers["Retry-After"] = "1"
    #
    #     return TreeCoverLossAnalyticsResponse(
    #         # data={
    #         #     "status": AnalysisStatus.pending,
    #         #     "message": "Resource is still processing, follow Retry-After header.",
    #         #     "result": results_dict,
    #         #     "metadata": metadata_content,
    #         # },
    #         # status="success",
    #     )

    raise HTTPException(
        status_code=404,
        detail="Requested resource not found. Either expired or never existed.",
    )
