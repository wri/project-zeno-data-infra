from abc import ABC, abstractmethod

from app.domain.models.dataset import DatasetQuery


class AnalyticsOTFHandler(ABC):
    @abstractmethod
    async def handle(self, aoi_type, aoi_ids, query: DatasetQuery):
        raise NotImplementedError()
