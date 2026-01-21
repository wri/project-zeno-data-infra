from abc import ABC, abstractmethod

from app.domain.models.dataset import DatasetQuery


class AnalyticsOTFHandler(ABC):
    @abstractmethod
    async def handle(self, aoi, query: DatasetQuery):
        raise NotImplementedError()
