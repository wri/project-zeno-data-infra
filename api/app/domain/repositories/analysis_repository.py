import uuid
from abc import ABC, abstractmethod

from app.domain.models.analysis import Analysis


class AnalysisRepository(ABC):
    @abstractmethod
    async def load_analysis(self, resource_id: uuid.UUID) -> Analysis:
        pass

    @abstractmethod
    async def store_analysis(self, resource_id: uuid.UUID, analytics: Analysis):
        pass
