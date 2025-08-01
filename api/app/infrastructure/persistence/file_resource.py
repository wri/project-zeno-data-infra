import json
import uuid
from pathlib import Path

from app.models.land_change.land_cover import AnalysisStatus, LandCoverChangeAnalytics

PAYLOAD_STORE_DIR = Path("/tmp/land_cover_change_analytics_payloads")
PAYLOAD_STORE_DIR.mkdir(parents=True, exist_ok=True)


async def load_resource(resource_id: uuid.UUID) -> LandCoverChangeAnalytics:
    file_path = PAYLOAD_STORE_DIR / str(resource_id)
    analytics_metadata = file_path / "metadata.json"
    analytics_data = file_path / "data.json"
    data = None
    metadata = None
    status = AnalysisStatus.pending

    if analytics_data.exists():
        data_content = analytics_data.read_text()
        data = json.loads(data_content)

    if analytics_metadata.exists():
        metadata_content = analytics_metadata.read_text()
        metadata = json.loads(metadata_content)
        status = AnalysisStatus.saved

    return LandCoverChangeAnalytics(
        result=data,
        metadata=metadata,
        status=status,
    )


async def write_resource(resource_id: uuid.UUID, analytics: LandCoverChangeAnalytics):
    file_path = PAYLOAD_STORE_DIR / str(resource_id)
    analytics_metadata = file_path / "metadata.json"
    analytics_data = file_path / "data.json"

    if analytics.metadata is not None:
        # Write metadata to file
        file_path.mkdir(parents=True, exist_ok=True)
        analytics_metadata.write_text(json.dumps(analytics.metadata.model_dump()))

    if analytics.result is not None:
        # Write data to file
        analytics_data.write_text(json.dumps(analytics.result.model_dump()))
