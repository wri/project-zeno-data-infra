from enum import Enum
from unittest.mock import MagicMock

import duckdb
import pandas as pd
import pytest
from app.domain.compute_engines.tree_cover_loss_compute_engine import (
    ComputeEngine,
    TreeCoverLossParquetPrecalcHandler,
)
from app.models.common.areas_of_interest import AreaOfInterest

from api.app.models.land_change.tree_cover_loss import TreeCoverLossAnalyticsIn


@pytest.fixture
def stub_analysis_in():
    return TreeCoverLossAnalyticsIn(
        aoi={
            "type": "admin",
            "ids": ["BRA"],
        },
        start_year="2020",
        end_year="2023",
        canopy_cover=30,
    )


class Dataset(Enum):
    tree_cover_loss = "tree_cover_loss"


@pytest.mark.asyncio
async def test_parquet_handler_happy_path(stub_analysis_in, stub_analysis_query_result):
    query_service = MagicMock()
    query_service.query.return_value = stub_analysis_query_result

    class ParquetQuery:
        dataset: Dataset
        sql: str

    class MockParquetQueryService:
        DATASET_PARQUETS = {
            Dataset.tree_cover_loss: "s3://tree_cover_loss.parquet",
        }

        async def execute(query: ParquetQuery):
            data = pd.DataFrame(
                {
                    "iso": ["BRA", "BRA"],
                    "year": [2015, 2020, 2023],
                    "tree_cover_loss_ha": [1, 10, 100],
                }
            )

            return duckdb.sql(query.sql).df()

    compute_engine = ComputeEngine(
        handler=TreeCoverLossParquetPrecalcHandler(
            query_service=MockParquetQueryService()
        )
    )
    result = await compute_engine.compute(stub_analysis_in)

    assert result == {
        "id": ["BRA", "BRA"],
        "year": [2020, 2023],
        "tree_cover_loss_ha": [10, 100],
    }

    assert query_service.query.call_args[0] == ""


@pytest.mark.asyncio
async def test_flox_handler_happy_path(stub_analysis_in, stub_analysis_query_result):
    dask_client = MagicMock()
    dataset_repository = MagicMock()

    class DatasetRepository:
        {"tree_cover_loss": "s3://tree_cover_loss.zarr"}

        def get_data(dataset):
            return load_zarr()

        xarray_reduce()

    @dataclass
    class FloxQuery:
        aoi: AreaOfInterest
        dataset: Dataset
        obj: xr.DataArray
        by: List[xr.DataArray]
        func: Literal["sum", "count"]

    class MockFloxQueryService:
        DATASET_ZARRS = {
            Dataset.tree_cover_loss: "s3://tree_cover_loss.zarr",
        }

        def __init__(self, geojson_query_service=GeoJSONQueryService()):
            pass

        async def execute(query: FloxQuery):
            data = pd.DataFrame(
                {
                    "iso": ["BRA", "BRA"],
                    "year": [2015, 2020, 2023],
                    "tree_cover_loss_ha": [1, 10, 100],
                }
            )

            return duckdb.sql(query).df()

    dataset_repository.get_xarray.return_value = None  # mock data array
    compute_engine = ComputeEngine(
        handler=TreeCoverLossFloxHandler(
            query_service=FloxQueryService(
                dask_client=dask_client, geojson_service=GeojsonService()
            )
        )
    )
    result = await compute_engine.compute(stub_analysis_in)

    assert result == {
        "id": ["BRA", "BRA"],
        "year": [2020, 2023],
        "tree_cover_loss_ha": [10, 100],
    }
