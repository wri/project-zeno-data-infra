import duckdb
import numpy as np
import pandas as pd
import pytest
import xarray as xr
from app.domain.compute_engines.compute_engine import (
    ComputeEngine,
    FloxOTFHandler,
    PrecalcHandler,
)
from app.domain.models.area_of_interest import AreaOfInterestList
from app.domain.models.dataset import Dataset
from app.domain.repositories.zarr_dataset_repository import ZarrDatasetRepository
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
                    "aoi_id": ["BRA", "BRA", "BRA"],
                    "aoi_type": ["admin", "admin", "admin"],
                    "tree_cover_loss__year": [2015, 2020, 2023],
                    "area__ha": [1, 10, 100],
                    "canopy_cover__percent": [20, 30, 50],
                }
            )

            return duckdb.sql(query).df()

    compute_engine = ComputeEngine(
        handler=PrecalcHandler(MockParquetQueryService(), next_handler=None)
    )

    aois = AreaOfInterestList(
        AdminAreaOfInterest(ids=["BRA", "IDN", "COD"]), compute_engine=compute_engine
    )
    results = await aois.get_tree_cover_loss(30, 2020, 2024, "primary_forest")
    assert "BRA" in results.aoi_id.to_list()
    assert 2020 in results.tree_cover_loss__year.to_list()
    assert 2023 in results.tree_cover_loss__year.to_list()
    assert 10.0 in results.area__ha.to_list()
    assert 100.0 in results.area__ha.to_list()
    assert "admin" in results.aoi_type
    assert results.size == 6


@pytest.mark.asyncio
async def test_flox_handler_happy_path():
    dask_cluster = LocalCluster(asynchronous=True)
    dask_client = Client(dask_cluster)

    class TestDatasetRepository(ZarrDatasetRepository):
        def load(self, dataset, geometry=None):
            if dataset == Dataset.area_hectares:
                # all values are 0.5
                data = np.full((10, 10), 0.5)
                coords = {"x": np.arange(10), "y": np.arange(10)}
                xarr = xr.DataArray(data, coords=coords, dims=("x", "y"))
                xarr.name = "area__ha"
            elif dataset == Dataset.canopy_cover:
                # left half is 1s, right half is 5s
                data = np.hstack([np.ones((10, 5)), np.full((10, 5), 5)])
                coords = {"x": np.arange(10), "y": np.arange(10)}
                xarr = xr.DataArray(data, coords=coords, dims=("x", "y"))
                xarr.name = "canopy_cover__percent"
            elif dataset == Dataset.tree_cover_loss:
                # top half is 15s, bottom half is 5s
                data = np.vstack([np.full((5, 10), 15), np.full((5, 10), 5)])
                coords = {"x": np.arange(10), "y": np.arange(10)}
                xarr = xr.DataArray(data, coords=coords, dims=("x", "y"))
                xarr.name = "tree_cover_loss__year"
            else:
                raise ValueError("Not a valid dataset for this test")

            return xarr

    class TestAoiGeometryRepository:
        async def load(self, aoi_type, aoi_ids):
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
    results = await aois.get_tree_cover_loss(20, 2010, 2020, "primary_forest")

    pd.testing.assert_frame_equal(
        pd.DataFrame(results),
        pd.DataFrame(
            {
                "tree_cover_loss__year": [2015],
                "area__ha": [12.5],
                "aoi_id": ["1234"],
                "aoi_type": ["protected_area"],
            },
        ),
        check_like=True,
    )
