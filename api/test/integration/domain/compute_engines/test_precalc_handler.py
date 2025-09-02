from unittest.mock import MagicMock

import pytest
from app.domain.compute_engines.compute_engine import (
    DuckDbPrecalcQueryService,
    GeneralPrecalcHandler,
    PrecalcQueryBuilder,
)
from app.domain.models.dataset import (
    Dataset,
    DatasetAggregate,
    DatasetFilter,
    DatasetQuery,
)


class TestTreeCoverGainPrecalcHandler:
    @pytest.mark.asyncio
    async def test_happy_path(self):
        query_service = MagicMock(spec=DuckDbPrecalcQueryService)
        handler = GeneralPrecalcHandler(PrecalcQueryBuilder(), query_service, None)

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

        aoi_type = "admin"

        def should_handle() -> bool:
            return (
                aoi_type == "admin" and query.aggregate.dataset == Dataset.area_hectares
            )

        await handler.handle(aoi_type, ["AUS"], query, should_handle)

        query_service.execute.assert_called_once_with(
            None,
            "SELECT aoi_id, aoi_type, SUM(area_ha) AS area_ha FROM data_source WHERE gain_period in ('2000-2005') AND aoi_id in ('AUS') GROUP BY aoi_id, aoi_type",
        )
