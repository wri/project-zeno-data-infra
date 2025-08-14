import duckdb
import pandas as pd
import pytest
from app.domain.compute_engines.compute_engine import (
    AreaOfInterestList,
    ComputeEngine,
    PrecalcHandler,
)

from api.app.models.common.areas_of_interest import AdminAreaOfInterest


@pytest.mark.asyncio
async def test_get_tree_cover_loss_precalc_handler_happy_path():
    class MockParquetQueryService:
        async def execute(self, data_source: str, query: str):
            data_source = pd.DataFrame(
                {
                    "id": ["BRA", "BRA", "BRA"],
                    "loss_year": [2015, 2020, 2023],
                    "area_ha": [1, 10, 100],
                    "canopy_cover": [20, 30, 50],
                }
            )

            return duckdb.sql(query).df()

    compute_engine = ComputeEngine(handler=PrecalcHandler(MockParquetQueryService()))

    aois = AreaOfInterestList(
        AdminAreaOfInterest(ids=["BRA", "IDN", "COD"]), compute_engine=compute_engine
    )
    results = await aois.get_tree_cover_loss(30, 2020, 2024, "primary_forest")

    pd.testing.assert_frame_equal(
        results,
        pd.DataFrame(
            {
                "id": ["BRA", "BRA"],
                "loss_year": [2020, 2023],
                "area_ha": [10.0, 100.0],
            },
        ),
        check_like=True,
    )


# @pytest.mark.asyncio
# async def test_flox_handler_happy_path(stub_analysis_in, stub_analysis_query_result):
#     dask_client = MagicMock()
#     dataset_repository = MagicMock()

#     class DatasetRepository:
#         {"tree_cover_loss": "s3://tree_cover_loss.zarr"}

#         def get_data(dataset):
#             return load_zarr()

#         xarray_reduce()


#     class MockFloxQueryService:
#         DATASET_ZARRS = {
#             Dataset.tree_cover_loss: "s3://tree_cover_loss.zarr",
#         }

#         def __init__(self, geojson_query_service=GeoJSONQueryService()):
#             pass

#         async def execute(query: FloxQuery):
#             data = pd.DataFrame(
#                 {
#                     "iso": ["BRA", "BRA"],
#                     "year": [2015, 2020, 2023],
#                     "tree_cover_loss_ha": [1, 10, 100],
#                 }
#             )

#             return duckdb.sql(query).df()

#     dataset_repository.get_xarray.return_value = None  # mock data array
#     compute_engine = ComputeEngine(
#         handler=TreeCoverLossFloxHandler(
#             query_service=FloxQueryService(
#                 dask_client=dask_client, geojson_service=GeojsonService()
#             )
#         )
#     )
#     result = await compute_engine.compute(stub_analysis_in)

#     assert result == {
#         "id": ["BRA", "BRA"],
#         "year": [2020, 2023],
#         "tree_cover_loss_ha": [10, 100],
#     }
