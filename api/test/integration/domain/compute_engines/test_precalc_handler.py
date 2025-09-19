from unittest.mock import MagicMock

import pytest
from app.domain.compute_engines.handlers.precalc_implementations.precalc_handlers import (
    TreeCoverGainPrecalcHandler,
)
from app.domain.compute_engines.handlers.precalc_implementations.precalc_sql_query_builder import (
    PrecalcSqlQueryBuilder,
)
from app.domain.models.dataset import (
    Dataset,
    DatasetAggregate,
    DatasetFilter,
    DatasetQuery,
)
from app.infrastructure.external_services.duck_db_query_service import (
    DuckDbPrecalcQueryService,
)


class TestTreeCoverGainPrecalcHandler:
    @pytest.mark.asyncio
    async def test_one_range(self):
        query_service = MagicMock(spec=DuckDbPrecalcQueryService)
        handler = TreeCoverGainPrecalcHandler(
            precalc_query_builder=PrecalcSqlQueryBuilder(),
            precalc_query_service=query_service,
            next_handler=None,
        )

        query = DatasetQuery(
            aggregate=DatasetAggregate(datasets=[Dataset.area_hectares], func="sum"),
            group_bys=[],
            filters=[
                DatasetFilter(
                    dataset=Dataset.tree_cover_gain,
                    op="in",
                    value=("2000-2005",),
                ),
            ],
        )

        aoi_type = "admin"

        await handler.handle(aoi_type, ["AUS"], query)

        query_service.execute.assert_called_once_with(
            "SELECT aoi_id, aoi_type, SUM(area_ha) AS area_ha FROM data_source WHERE tree_cover_gain_period in ('2000-2005') AND aoi_id in ('AUS') GROUP BY aoi_id, aoi_type"
        )

    @pytest.mark.asyncio
    async def test_multiple_ranges(self):
        query_service = MagicMock(spec=DuckDbPrecalcQueryService)
        handler = TreeCoverGainPrecalcHandler(
            precalc_query_builder=PrecalcSqlQueryBuilder(),
            precalc_query_service=query_service,
            next_handler=None,
        )

        query = DatasetQuery(
            aggregate=DatasetAggregate(datasets=[Dataset.area_hectares], func="sum"),
            group_bys=[],
            filters=[
                DatasetFilter(
                    dataset=Dataset.tree_cover_gain,
                    op="in",
                    value=("2000-2005", "2005-2010"),
                ),
            ],
        )

        aoi_type = "admin"

        await handler.handle(aoi_type, ["AUS"], query)

        query_service.execute.assert_called_once_with(
            "SELECT aoi_id, aoi_type, SUM(area_ha) AS area_ha FROM data_source WHERE tree_cover_gain_period in ('2000-2005', '2005-2010') AND aoi_id in ('AUS') GROUP BY aoi_id, aoi_type"
        )


#
# class TestTreeCoverLossPrecalcHandler:
#     @pytest.mark.asyncio
#     async def test_one_range(self):
#         query_service = MagicMock(spec=DuckDbPrecalcQueryService)
#
#         handler = TreeCoverLossPrecalcHandler(
#             precalc_query_builder=PrecalcSqlQueryBuilder(),
#             precalc_query_service=query_service,
#             next_handler=None,
#         )
#
#         analytics_in = TreeCoverLossAnalyticsIn(
#             aoi=AdminAreaOfInterest(type="admin", ids=["IDN.24.9", "IDN.14", "BRA"]),
#             start_year="2015",
#             end_year="2022",
#             canopy_cover=30,
#             intersections=[],
#         )
#
#         query = DatasetQuery(
#             aggregate=DatasetAggregate(datasets=[Dataset.area_hectares], func="sum"),
#             group_bys=[],
#             filters=[
#                 DatasetFilter(
#                     dataset=Dataset.tree_cover_gain,
#                     op="in",
#                     value=("2000-2005",),
#                 ),
#             ],
#         )
#
#         aoi_type = "admin"
#
#         await handler.handle(aoi_type, ["AUS"], query)
#
#         query_service.execute.assert_called_once_with(
#             "SELECT aoi_id, aoi_type, SUM(area_ha) AS area_ha FROM data_source WHERE tree_cover_gain_period in ('2000-2005') AND aoi_id in ('AUS') GROUP BY aoi_id, aoi_type"
#         )
