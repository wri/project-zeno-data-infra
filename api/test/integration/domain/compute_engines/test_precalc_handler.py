from unittest.mock import MagicMock

import pytest
from app.domain.compute_engines.compute_engine import (
    DuckDbPrecalcQueryService,
    GeneralPrecalcHandler,
    PrecalcQueryBuilder,
    TreeCoverGainPrecalcHandler,
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
        handler = TreeCoverGainPrecalcHandler(
            GeneralPrecalcHandler(PrecalcQueryBuilder(), query_service), None
        )

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

        await handler.handle(aoi_type, ["AUS"], query)

        query_service.execute.assert_called_once_with(
            "SELECT aoi_id, aoi_type, SUM(area_ha) AS area_ha FROM data_source WHERE gain_period in ('2000-2005') AND aoi_id in ('AUS') GROUP BY aoi_id, aoi_type",
        )
