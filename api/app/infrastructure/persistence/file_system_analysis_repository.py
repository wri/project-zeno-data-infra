import json
import uuid
from enum import Enum
from pathlib import Path
from typing import Tuple

from app.domain.models.analysis import Analysis
from app.domain.repositories.analysis_repository import AnalysisRepository
from app.models.common.analysis import AnalysisStatus

PAYLOAD_STORE_DIR = Path("/tmp")
PAYLOAD_STORE_DIR.mkdir(parents=True, exist_ok=True)


class EnumEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Enum):
            return obj.value
        return super().default(obj)


class FileSystemAnalysisRepository(AnalysisRepository):
    def __init__(self, resource_directory):
        self.resource_directory = resource_directory

    async def load_analysis(self, resource_id: uuid.UUID) -> Analysis:
        analytics_metadata, analytics_data, status_data = await self._get_data_files(
            resource_id
        )

        data = None
        metadata = None
        status = None

        if analytics_metadata.exists():
            metadata = json.loads(analytics_metadata.read_text())
            status = AnalysisStatus.pending

        if analytics_data.exists():
            data = json.loads(analytics_data.read_text())
            status = AnalysisStatus.saved

        if status_data.exists():
            status = json.loads(status_data.read_text())

        return Analysis(
            result=data,
            metadata=metadata,
            status=status,
        )

    async def store_analysis(self, resource_id: uuid.UUID, analytics: Analysis):
        analytics_metadata, analytics_data, status_data = await self._get_data_files(
            resource_id, analytics
        )

        if analytics.metadata is not None:
            analytics_metadata.write_text(
                json.dumps(analytics.metadata, cls=EnumEncoder)
            )

        if analytics.result is not None:
            analytics_data.write_text(json.dumps(analytics.result))

        if analytics.status is not None:
            status_data.write_text(json.dumps(analytics.status))

    async def _get_data_files(
        self,
        resource_id: uuid.UUID,
        analytics: Analysis = Analysis(metadata=None, result=None, status=None),
    ) -> Tuple[Path, Path, Path]:
        file_path = PAYLOAD_STORE_DIR / self.resource_directory / str(resource_id)

        if analytics.metadata is not None:
            file_path.mkdir(parents=True, exist_ok=True)

        analytics_metadata = file_path / "metadata.json"
        analytics_data = file_path / "data.json"
        status_data = file_path / "status.json"

        return analytics_metadata, analytics_data, status_data
