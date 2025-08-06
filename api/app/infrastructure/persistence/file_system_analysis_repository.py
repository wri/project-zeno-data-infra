import json
import uuid
from pathlib import Path

from app.models.common.analysis import AnalysisStatus
from app.domain.models.analysis import Analysis
from app.domain.repositories.analysis_repository import AnalysisRepository

PAYLOAD_STORE_DIR = Path("/tmp")
PAYLOAD_STORE_DIR.mkdir(parents=True, exist_ok=True)

class FileSystemAnalysisRepository(AnalysisRepository):
    def __init__(self, resource_directory):
        self.resource_directory = resource_directory

    async def load_analysis(self, resource_id: uuid.UUID) -> Analysis:
        file_path = PAYLOAD_STORE_DIR / self.resource_directory / str(resource_id)
        analytics_metadata = file_path / "metadata.json"
        analytics_data = file_path / "data.json"
        data = None
        metadata = None
        status = AnalysisStatus.pending

        if analytics_data.exists():
            data_content = analytics_data.read_text()
            data = json.loads(data_content)
            status = AnalysisStatus.saved

        if analytics_metadata.exists():
            metadata_content = analytics_metadata.read_text()
            metadata = json.loads(metadata_content)

        return Analysis(
            result=data,
            metadata=metadata,
            status=status,
        )

    async def store_analysis(self, resource_id: uuid.UUID, analytics: Analysis):
        file_path = PAYLOAD_STORE_DIR / self.resource_directory / str(resource_id)
        analytics_metadata = file_path / "metadata.json"
        analytics_data = file_path / "data.json"

        if analytics.metadata is not None:
            # Write metadata to file
            file_path.mkdir(parents=True, exist_ok=True)
            analytics_metadata.write_text(json.dumps(analytics.metadata))

        if analytics.result is not None:
            # Write data to file
            analytics_data.write_text(json.dumps(analytics.result))
