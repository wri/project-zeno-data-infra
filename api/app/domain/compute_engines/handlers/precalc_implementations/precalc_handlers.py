from abc import ABC

from app.domain.compute_engines.handlers.analytics_precalc_handler import (
    AnalyticsPrecalcHandler,
)
from app.domain.models.dataset import Dataset, DatasetQuery


class GeneralPrecalcHandler(AnalyticsPrecalcHandler, ABC):
    def __init__(self, precalc_query_builder, precalc_query_service, next_handler):
        self.precalc_query_builder = precalc_query_builder
        self.precalc_query_service = precalc_query_service
        self.next_handler = next_handler

    async def handle(self, aoi_type, aoi_ids, query: DatasetQuery):
        if self.should_handle(aoi_type, aoi_ids, query):
            sql = self.precalc_query_builder.build(aoi_ids, query)
            return await self.precalc_query_service.execute(sql)

        if self.next_handler is not None:
            return await self.next_handler.handle(aoi_type, aoi_ids, query)

        return None


class TreeCoverPrecalcHandler(GeneralPrecalcHandler):
    def should_handle(self, aoi_type, aoi_ids, query: DatasetQuery) -> bool:
        return (
            aoi_type == "admin"
            and query.aggregate.dataset == Dataset.area_hectares
        )


class TreeCoverGainPrecalcHandler(GeneralPrecalcHandler):
    def should_handle(self, aoi_type, aoi_ids, query: DatasetQuery) -> bool:
        return aoi_type == "admin" and query.aggregate.dataset == Dataset.area_hectares


class TreeCoverLossPrecalcHandler(GeneralPrecalcHandler):
    def should_handle(self, aoi_type, aoi_ids, query: DatasetQuery) -> bool:
        return (
            aoi_type == "admin"
            and query.aggregate.dataset == Dataset.area_hectares
            and query.group_bys == [Dataset.tree_cover_loss]
        )
