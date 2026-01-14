import duckdb
import numpy as np
import pandas as pd
import pytest
import xarray as xr
from distributed import Client, LocalCluster
from shapely.geometry import box

from app.domain.analyzers.tree_cover_loss_analyzer import TreeCoverLossAnalyzer
from app.domain.compute_engines.compute_engine import (
    ComputeEngine,
)
from app.domain.compute_engines.handlers.otf_implementations.flox_otf_handler import (
    FloxOTFHandler,
)
from app.domain.compute_engines.handlers.precalc_implementations.precalc_handlers import (
    TreeCoverLossPrecalcHandler,
)
from app.domain.compute_engines.handlers.precalc_implementations.precalc_sql_query_builder import (
    PrecalcSqlQueryBuilder,
)
from app.domain.models.analysis import Analysis
from app.domain.models.dataset import Dataset
from app.domain.repositories.zarr_dataset_repository import ZarrDatasetRepository
from app.models.common.analysis import AnalysisStatus
from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
    ProtectedAreaOfInterest,
)
from app.models.land_change.tree_cover_loss import TreeCoverLossAnalyticsIn


@pytest.mark.asyncio
async def test_get_tree_cover_loss_precalc_handler_happy_path():
    class MockParquetQueryService:
        async def execute(self, query: str):
            data_source = pd.DataFrame(  # noqa
                {
                    "aoi_id": ["BRA", "BRA", "BRA"],
                    "aoi_type": ["admin", "admin", "admin"],
                    "tree_cover_loss_year": [2015, 2020, 2023],
                    "area_ha": [1, 10, 100],
                    "canopy_cover": [20, 30, 50],
                    "carbon_emissions_MgCO2e": [0.1, 0.2, 0.3],
                }
            )

            return duckdb.sql(query).df()

    compute_engine = ComputeEngine(
        handler=TreeCoverLossPrecalcHandler(
            precalc_query_builder=PrecalcSqlQueryBuilder(),
            precalc_query_service=MockParquetQueryService(),
            next_handler=None,
        )
    )

    aoi = AdminAreaOfInterest(ids=["BRA", "IDN", "COD"])
    analytics_in = TreeCoverLossAnalyticsIn(
        aoi=aoi,
        canopy_cover=30,
        start_year="2020",
        end_year="2024",
        intersections=[],
    ).model_dump()

    analysis = Analysis(None, analytics_in, AnalysisStatus.saved)

    analyzer = TreeCoverLossAnalyzer(compute_engine=compute_engine)
    results = await analyzer.analyze(analysis)

    assert "BRA" in results.aoi_id.to_list()
    assert 2020 in results.tree_cover_loss_year.to_list()
    assert 2023 in results.tree_cover_loss_year.to_list()
    assert 10.0 in results.area_ha.to_list()
    assert 100.0 in results.area_ha.to_list()
    assert "admin" in results.aoi_type.to_list()
    assert results.size == 10


@pytest.mark.asyncio
async def test_flox_handler_happy_path():
    dask_cluster = LocalCluster(asynchronous=True)
    dask_client = Client(dask_cluster)

    class TestDatasetRepository(ZarrDatasetRepository):
        def load(self, dataset, geometry=None):
            if dataset == Dataset.area_hectares:
                # all values are 0.5
                data = np.full((10, 10), 5000)
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
                xarr.name = "tree_cover_loss_year"
            elif dataset == Dataset.carbon_emissions:
                # top half is 1.5s, bottom half is 0.5s
                data = np.vstack([np.full((5, 10), 1.5), np.full((5, 10), 0.5)])
                coords = {"x": np.arange(10), "y": np.arange(10)}
                xarr = xr.DataArray(data, coords=coords, dims=("x", "y"))
                xarr.name = "carbon_emissions_MgCO2e"
            else:
                raise ValueError(f"Not a valid dataset for this test:{dataset}")

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
    aoi = ProtectedAreaOfInterest(ids=["1234"])
    analytics_in = TreeCoverLossAnalyticsIn(
        aoi=aoi,
        canopy_cover=30,
        start_year="2010",
        end_year="2020",
        intersections=[],
    ).model_dump()

    analysis = Analysis(None, analytics_in, AnalysisStatus.saved)

    analyzer = TreeCoverLossAnalyzer(compute_engine=compute_engine)
    results = await analyzer.analyze(analysis)

    pd.testing.assert_frame_equal(
        pd.DataFrame(results),
        pd.DataFrame(
            {
                "tree_cover_loss_year": [2015],
                "area_ha": [125000.0],
                "aoi_id": ["1234"],
                "aoi_type": ["protected_area"],
                "carbon_emissions_MgCO2e": [37.5],
            },
        ),
        check_like=True,
        check_exact=False,  # Allow approximate comparison for numbers
        atol=1e-8,  # Absolute tolerance
        rtol=1e-4,  # Relative tolerance
    )


@pytest.mark.asyncio
async def test_flox_handler_natural_forests():
    dask_cluster = LocalCluster(asynchronous=True)
    dask_client = Client(dask_cluster)

    class TestDatasetRepository(ZarrDatasetRepository):
        def load(self, dataset, geometry=None):
            if dataset == Dataset.area_hectares:
                # all values are 0.5
                data = np.full((10, 10), 5000)
                coords = {"x": np.arange(10), "y": np.arange(10)}
                xarr = xr.DataArray(data, coords=coords, dims=("x", "y"))
                xarr.name = "area_ha"
            elif dataset == Dataset.natural_lands:
                # left half is 1s, right half is 5s. 5 is a valid natural forest class.
                data = np.hstack([np.ones((10, 5)), np.full((10, 5), 5)])
                coords = {"x": np.arange(10), "y": np.arange(10)}
                xarr = xr.DataArray(data, coords=coords, dims=("x", "y"))
                xarr.name = "natural_lands"
            elif dataset == Dataset.tree_cover_loss:
                # top half is 24s, bottom half is 5s
                data = np.vstack([np.full((5, 10), 24), np.full((5, 10), 5)])
                coords = {"x": np.arange(10), "y": np.arange(10)}
                xarr = xr.DataArray(data, coords=coords, dims=("x", "y"))
                xarr.name = "tree_cover_loss_year"
            elif dataset == Dataset.carbon_emissions:
                # top half is 1.5s, bottom half is 0.5s
                data = np.vstack([np.full((5, 10), 1.5), np.full((5, 10), 0.5)])
                coords = {"x": np.arange(10), "y": np.arange(10)}
                xarr = xr.DataArray(data, coords=coords, dims=("x", "y"))
                xarr.name = "carbon_emissions_MgCO2e"
            else:
                raise ValueError(f"Not a valid dataset for this test:{dataset}")

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
    aoi = ProtectedAreaOfInterest(ids=["1234"])
    analytics_in = TreeCoverLossAnalyticsIn(
        aoi=aoi,
        start_year="2021",
        end_year="2024",
        forest_filter="natural_forest",
        intersections=[],
    ).model_dump()

    analysis = Analysis(None, analytics_in, AnalysisStatus.saved)

    analyzer = TreeCoverLossAnalyzer(compute_engine=compute_engine)
    results = await analyzer.analyze(analysis)

    pd.testing.assert_frame_equal(
        pd.DataFrame(results),
        pd.DataFrame(
            {
                "tree_cover_loss_year": [2024],
                "area_ha": [125000.0],
                "aoi_id": ["1234"],
                "aoi_type": ["protected_area"],
                "carbon_emissions_MgCO2e": [37.5],
            },
        ),
        check_like=True,
        check_exact=False,  # Allow approximate comparison for numbers
        atol=1e-8,  # Absolute tolerance
        rtol=1e-4,  # Relative tolerance
    )
