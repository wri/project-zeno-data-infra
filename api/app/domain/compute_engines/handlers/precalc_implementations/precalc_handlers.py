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

    async def handle(self, aoi, query: DatasetQuery):
        if self.should_handle(aoi, query):
            sql = self.precalc_query_builder.build(aoi.ids, query)
            return await self.precalc_query_service.execute(sql)

        if self.next_handler is not None:
            return await self.next_handler.handle(aoi, query)

        return None


class TreeCoverPrecalcHandler(GeneralPrecalcHandler):
    def should_handle(self, aoi, query: DatasetQuery) -> bool:
        return aoi.type == "admin" and Dataset.area_hectares in query.aggregate.datasets


class TreeCoverGainPrecalcHandler(GeneralPrecalcHandler):
    def should_handle(self, aoi, query: DatasetQuery) -> bool:
        return aoi.type == "admin" and Dataset.area_hectares in query.aggregate.datasets


class TreeCoverLossPrecalcHandler(GeneralPrecalcHandler):
    def should_handle(self, aoi, query: DatasetQuery) -> bool:
        return (
            aoi.type == "admin"
            and (
                Dataset.area_hectares in query.aggregate.datasets
                or Dataset.carbon_emissions in query.aggregate.datasets
            )
            and (
                query.group_bys == [Dataset.tree_cover_loss]
                or query.group_bys == [Dataset.tree_cover_loss_drivers]
            )
        )
