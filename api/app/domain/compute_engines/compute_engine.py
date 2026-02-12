from app.domain.compute_engines.handlers.analytics_otf_handler import (
    AnalyticsOTFHandler,
)
from app.domain.compute_engines.handlers.analytics_precalc_handler import (
    AnalyticsPrecalcHandler,
)
from app.domain.models.dataset import DatasetQuery


class ComputeEngine:
    def __init__(self, handler: AnalyticsPrecalcHandler | AnalyticsOTFHandler):
        self.handler = handler

    async def compute(self, aoi, query: DatasetQuery):
        return await self.handler.handle(aoi, query)
