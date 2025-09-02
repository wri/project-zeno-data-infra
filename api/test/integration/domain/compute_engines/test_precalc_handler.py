from unittest.mock import MagicMock

import pytest
from app.domain.compute_engines.compute_engine import (
    AnalyticsPrecalcHandler,
    DuckDbPrecalcQueryService,
    GeneralPrecalcHandler,
)
from app.domain.models.dataset import (
    Dataset,
    DatasetAggregate,
    DatasetFilter,
    DatasetQuery,
)


class TreeCoverGainPrecalcHandler(AnalyticsPrecalcHandler):
    def __init__(self, handler: GeneralPrecalcHandler):
        self.handler = handler

    async def handle(self, aoi_type, aoi_ids, query: DatasetQuery):
        predicate = (
            lambda: aoi_type == "admin"
            and query.aggregate.dataset == Dataset.area_hectares
        )
        return await self.handler.handle(aoi_type, aoi_ids, query, predicate)


class TestTreeCoverGainPrecalcHandler:
    @pytest.mark.asyncio
    async def test_happy_path(self):
        query = DatasetQuery(
            aggregate=DatasetAggregate(dataset=Dataset.area_hectares, func="sum"),
            group_bys=[],
            filters=[
                DatasetFilter(
                    dataset=Dataset.tree_cover_gain,
                    op="in",
                    value="('2000-2005')",
                ),
            ],
        )

        query_service = MagicMock(spec=DuckDbPrecalcQueryService)

        aoi_type = "admin"

        handler = TreeCoverGainPrecalcHandler(
            GeneralPrecalcHandler(query_service, None)
        )

        await handler.handle(aoi_type, ["AUS"], query)

        query_service.execute.assert_called_once_with(
            None,
            "SELECT aoi_id, aoi_type, SUM(area_ha) AS area_ha FROM data_source WHERE gain_period in ('2000-2005') AND aoi_id in ('AUS') GROUP BY aoi_id, aoi_type",
        )
