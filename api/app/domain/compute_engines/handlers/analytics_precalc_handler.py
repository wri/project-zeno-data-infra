from abc import ABC, abstractmethod

from app.domain.models.dataset import DatasetQuery


class AnalyticsPrecalcHandler(ABC):
    @abstractmethod
    async def handle(self, aoi_type, aoi_ids, query: DatasetQuery):
        raise NotImplementedError()

    @abstractmethod
    def should_handle(self, aoi_type, aoi_ids, query: DatasetQuery) -> bool:
        raise NotImplementedError()
