import duckdb
import numpy as np
import pandas as pd
import pytest
import xarray as xr
from app.domain.compute_engines.compute_engine import (
    AreaOfInterestList,
    ComputeEngine,
    Dataset,
    FloxOTFHandler,
    PrecalcHandler,
)
from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
    ProtectedAreaOfInterest,
)
from distributed import Client, LocalCluster
from shapely.geometry import box


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


@pytest.mark.asyncio
async def test_flox_handler_happy_path():
    dask_cluster = LocalCluster(asynchronous=True)
    dask_client = Client(dask_cluster)

    class TestDatasetRepository:
        def load(self, dataset, geometry=None):
            if dataset == Dataset.area_hectares:
                # all values are 0.5
                data = np.full((10, 10), 0.5)
                coords = {"x": np.arange(10), "y": np.arange(10)}
                xarr = xr.DataArray(data, coords=coords, dims=("x", "y"))
                xarr.name = "area_ha"
            elif dataset == Dataset.canopy_cover:
                # left half is 1s, right half is 5s
                data = np.hstack([np.ones((10, 5)), np.full((10, 5), 5)])
                coords = {"x": np.arange(10), "y": np.arange(10)}
                xarr = xr.DataArray(data, coords=coords, dims=("x", "y"))
                xarr.name = "canopy_cover"
            elif dataset == Dataset.tree_cover_loss:
                # top half is 15s, bottom half is 5s
                data = np.vstack([np.full((5, 10), 15), np.full((5, 10), 5)])
                coords = {"x": np.arange(10), "y": np.arange(10)}
                xarr = xr.DataArray(data, coords=coords, dims=("x", "y"))
                xarr.name = "loss_year"
            else:
                raise ValueError("Not a valid dataset for this test")

            return xarr

    class TestAoiGeometryRepository:
        def load(self, aoi_type, aoi_ids):
            return [box(10, 0, 0, 10)]

    compute_engine = ComputeEngine(
        handler=FloxOTFHandler(
            dataset_repository=TestDatasetRepository(),
            aoi_geometry_repository=TestAoiGeometryRepository(),
            dask_client=dask_client,
        )
    )
    aois = AreaOfInterestList(
        ProtectedAreaOfInterest(ids=["1234"]), compute_engine=compute_engine
    )
    results = await aois.get_tree_cover_loss(3, 10, 20, "primary_forest")

    pd.testing.assert_frame_equal(
        results,
        pd.DataFrame(
            {
                "loss_year": [15],
                "area_ha": [12.5],
            },
        ),
        check_like=True,
    )
