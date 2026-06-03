from typing import Dict
from unittest.mock import patch

import duckdb
import numpy as np
import pandas as pd
import pytest
import xarray as xr
from distributed import Client, LocalCluster
from shapely.geometry import box, mapping

from app.domain.analyzers.tree_cover_loss_analyzer import (
    INPUT_URIS,
    TreeCoverLossAnalyzer,
)
from app.domain.compute_engines.compute_engine import (
    ComputeEngine,
)
from app.domain.compute_engines.handlers.otf_implementations.flox_otf_handler import (
    FloxOTFHandler,
)
from app.domain.models.analysis import Analysis
from app.domain.models.dataset import Dataset
from app.domain.models.environment import Environment
from app.domain.repositories.zarr_dataset_repository import ZarrDatasetRepository
from app.models.common.analysis import AnalysisStatus
from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
    CustomAreaOfInterest,
    ProtectedAreaOfInterest,
)
from app.models.land_change.tree_cover_loss import TreeCoverLossAnalyticsIn


class TestAoiGeometryRepository:
    async def load(self, aoi_type, aoi_ids):
        geometries = [box(10.1, -0.1, -0.1, 10.1)]
        areas_ha = [1000.0] * len(geometries)
        return geometries, areas_ha


class TestDatasetRepository(ZarrDatasetRepository):
    def open_source(self, dataset):
        if dataset == Dataset.area_hectares:
            # all values are 5000
            data = np.full((10, 10), 5000)
            coords = {"x": np.arange(10), "y": np.arange(9, -1, -1)}
            xarr = xr.DataArray(data, coords=coords, dims=("x", "y"))
            xarr.name = "area_ha"
        elif dataset == Dataset.canopy_cover:
            # left half is 1s, right half is 5s
            # only right half is tcd>30
            data = np.hstack([np.ones((10, 5)), np.full((10, 5), 5)])
            coords = {"x": np.arange(10), "y": np.arange(9, -1, -1)}
            xarr = xr.DataArray(data, coords=coords, dims=("x", "y"))
            xarr.name = "canopy_cover"
        elif dataset == Dataset.tree_cover_loss:
            # top half is 21s, bottom half is 5s
            data = np.vstack([np.full((5, 10), 21), np.full((5, 10), 5)])
            coords = {"x": np.arange(10), "y": np.arange(9, -1, -1)}
            xarr = xr.DataArray(data, coords=coords, dims=("x", "y"))
            xarr.name = "tree_cover_loss_year"
        elif dataset == Dataset.carbon_emissions:
            # top half is 1.5s, bottom half is 0.5s
            data = np.vstack([np.full((5, 10), 1.5), np.full((5, 10), 0.5)])
            coords = {"x": np.arange(10), "y": np.arange(9, -1, -1)}
            xarr = xr.DataArray(data, coords=coords, dims=("x", "y"))
            xarr.name = "carbon_emissions_MgCO2e"
        elif dataset == Dataset.natural_forests:
            # left half is 0s, middle is 1s,right is 2s
            data = np.hstack([np.zeros((10, 3)), np.ones((10, 4)), np.full((10, 3), 2)])
            coords = {"x": np.arange(10), "y": np.arange(9, -1, -1)}
            xarr = xr.DataArray(data, coords=coords, dims=("x", "y"))
            xarr.name = "natural_forests"
        elif dataset == Dataset.intact_forest:
            # top 3 rows are 1s, remaining rows are 0s
            data = np.vstack([np.ones((3, 10)), np.zeros((7, 10))])
            coords = {"x": np.arange(10), "y": np.arange(9, -1, -1)}
            xarr = xr.DataArray(data, coords=coords, dims=("x", "y"))
            xarr.name = "is_intact_forest"
        elif dataset == Dataset.tree_cover_loss_from_fires:
            # Year codes per the real zarr: fire-loss year mirrors TCL year.
            # Top half = 21 (fire-loss in 2021), bottom half = 0 (no fire loss).
            data = np.vstack([np.full((5, 10), 21), np.full((5, 10), 0)])
            coords = {"x": np.arange(10), "y": np.arange(9, -1, -1)}
            xarr = xr.DataArray(data, coords=coords, dims=("x", "y"))
            xarr.name = "tree_cover_loss_from_fires_area_ha"
        else:
            raise ValueError(f"Not a valid dataset for this test:{dataset}")

        return xarr


@pytest.mark.asyncio
async def test_get_tree_cover_loss_precalc_handler_happy_path():
    class MockParquetQueryService:
        async def execute(self, query: str) -> Dict:
            # DuckDB references this table implicitly bc its in scope when we run .sql()
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
            return duckdb.sql(query).df().to_dict(orient="list")

    aoi = AdminAreaOfInterest(ids=["BRA", "IDN", "COD"])
    analytics_in = TreeCoverLossAnalyticsIn(
        aoi=aoi,
        canopy_cover=30,
        start_year="2020",
        end_year="2024",
        intersections=[],
    ).model_dump()

    analysis = Analysis(None, analytics_in, AnalysisStatus.saved)

    analyzer = TreeCoverLossAnalyzer(
        compute_engine=None, input_uris=INPUT_URIS[Environment.production]
    )
    with patch("app.domain.analyzers.tree_cover_loss_analyzer.DuckDbPrecalcQueryService") as mock_qs:
        mock_qs.return_value.execute = MockParquetQueryService().execute
        await analyzer.analyze(analysis)
    result_dict = analysis.result

    results = pd.DataFrame(result_dict)

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
        end_year="2022",
        intersections=[],
    ).model_dump()

    analysis = Analysis(None, analytics_in, AnalysisStatus.saved)

    analyzer = TreeCoverLossAnalyzer(
        compute_engine=compute_engine, input_uris=INPUT_URIS[Environment.production]
    )
    await analyzer.analyze(analysis)
    results = analysis.result

    pd.testing.assert_frame_equal(
        pd.DataFrame(results),
        pd.DataFrame(
            {
                "tree_cover_loss_year": [2021],
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

    analyzer = TreeCoverLossAnalyzer(
        compute_engine=compute_engine, input_uris=INPUT_URIS[Environment.production]
    )
    await analyzer.analyze(analysis)
    results = analysis.result

    pd.testing.assert_frame_equal(
        pd.DataFrame(results),
        pd.DataFrame(
            {
                "tree_cover_loss_year": [2021, 2021, 2021],
                "natural_forests_class": [
                    "Unknown",
                    "Natural Forest",
                    "Non-Natural Forest",
                ],
                "area_ha": [75000.0, 100000.0, 75000.0],
                "carbon_emissions_MgCO2e": [22.5, 30.0, 22.5],
                "aoi_id": ["1234", "1234", "1234"],
                "aoi_type": ["protected_area", "protected_area", "protected_area"],
            },
        ),
        check_like=True,
        check_exact=False,  # Allow approximate comparison for numbers
        atol=1e-8,  # Absolute tolerance
        rtol=1e-4,  # Relative tolerance
    )


@pytest.mark.asyncio
async def test_flox_handler_intact_forest():
    dask_cluster = LocalCluster(asynchronous=True)
    dask_client = Client(dask_cluster)

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
        forest_filter="intact_forest",
        intersections=[],
    ).model_dump()

    analysis = Analysis(None, analytics_in, AnalysisStatus.saved)

    analyzer = TreeCoverLossAnalyzer(
        compute_engine=compute_engine, input_uris=INPUT_URIS[Environment.production]
    )
    await analyzer.analyze(analysis)
    results = analysis.result

    # intact_forest=1 keeps top 3 rows, year filter keeps top half (5 rows)
    # → rows 0-2, all 10 cols: 30 pixels × 5000 ha = 150000, carbon 30 × 1.5 = 45.0
    pd.testing.assert_frame_equal(
        pd.DataFrame(results),
        pd.DataFrame(
            {
                "tree_cover_loss_year": [2021],
                "area_ha": [150000.0],
                "aoi_id": ["1234"],
                "aoi_type": ["protected_area"],
                "carbon_emissions_MgCO2e": [45.0],
            },
        ),
        check_like=True,
        check_exact=False,
        atol=1e-8,
        rtol=1e-4,
    )


@pytest.mark.asyncio
async def test_flox_handler_custom_area():
    dask_cluster = LocalCluster(asynchronous=True)
    dask_client = Client(dask_cluster)

    class TestAoiGeometryRepository:
        async def load(self, aoi_type, aoi_ids):
            raise ValueError("This should not be called for custom AOI!")

    compute_engine = ComputeEngine(
        handler=FloxOTFHandler(
            dataset_repository=TestDatasetRepository(),
            aoi_geometry_repository=TestAoiGeometryRepository(),
            dask_client=dask_client,
        )
    )

    feature_collection = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "id": "my_feature",
                "geometry": mapping(box(10.1, -0.1, -0.1, 10.1)),
            }
        ],
    }
    aoi = CustomAreaOfInterest(feature_collection=feature_collection)
    analytics_in = TreeCoverLossAnalyticsIn(
        aoi=aoi,
        canopy_cover=30,
        start_year="2010",
        end_year="2022",
        intersections=[],
    ).model_dump()

    analysis = Analysis(None, analytics_in, AnalysisStatus.saved)

    analyzer = TreeCoverLossAnalyzer(
        compute_engine=compute_engine, input_uris=INPUT_URIS[Environment.production]
    )
    await analyzer.analyze(analysis)
    results = analysis.result

    pd.testing.assert_frame_equal(
        pd.DataFrame(results),
        pd.DataFrame(
            {
                "tree_cover_loss_year": [2021],
                "area_ha": [125000.0],
                "aoi_id": ["my_feature"],
                "aoi_type": ["feature_collection"],
                "carbon_emissions_MgCO2e": [37.5],
            },
        ),
        check_like=True,
        check_exact=False,  # Allow approximate comparison for numbers
        atol=1e-8,  # Absolute tolerance
        rtol=1e-4,  # Relative tolerance
    )


@pytest.mark.asyncio
async def test_get_tree_cover_loss_from_fires_precalc_handler_happy_path():
    class MockParquetQueryService:
        async def execute(self, query: str) -> Dict:
            # DuckDB references this table implicitly bc its in scope when we run .sql()
            data_source = pd.DataFrame(  # noqa
                {
                    "aoi_id": ["BRA", "BRA", "BRA"],
                    "aoi_type": ["admin", "admin", "admin"],
                    "tree_cover_loss_year": [2015, 2020, 2023],
                    "area_ha": [1, 10, 100],
                    "canopy_cover": [20, 30, 50],
                    "carbon_emissions_MgCO2e": [0.1, 0.2, 0.3],
                    "tree_cover_loss_from_fires_area_ha": [0.2, 0.3, 0.4],
                    "tree_cover_loss_non_fires_area_ha": [0.8, 9.7, 99.6]
                }
            )
            return duckdb.sql(query).df().to_dict(orient="list")

    aoi = AdminAreaOfInterest(ids=["BRA", "IDN", "COD"])
    analytics_in = TreeCoverLossAnalyticsIn(
        aoi=aoi,
        canopy_cover=30,
        start_year="2020",
        end_year="2024",
        intersections=["fire"],
    ).model_dump()

    analysis = Analysis(None, analytics_in, AnalysisStatus.saved)

    analyzer = TreeCoverLossAnalyzer(
        compute_engine=None, input_uris=INPUT_URIS[Environment.production]
    )
    with patch("app.domain.analyzers.tree_cover_loss_analyzer.DuckDbPrecalcQueryService") as mock_qs:
        mock_qs.return_value.execute = MockParquetQueryService().execute
        await analyzer.analyze(analysis)
    result_dict = analysis.result
    results = pd.DataFrame(result_dict)

    assert "BRA" in results.aoi_id.to_list()
    assert 2020 in results.tree_cover_loss_year.to_list()
    assert 2023 in results.tree_cover_loss_year.to_list()
    assert 10.0 in results.area_ha.to_list()
    assert 100.0 in results.area_ha.to_list()
    assert "admin" in results.aoi_type.to_list()
    assert 0.3 in results.tree_cover_loss_from_fires_area_ha.to_list()
    assert 99.6 in results.tree_cover_loss_non_fires_area_ha.to_list() # TCL-TCLF
    assert results.size == 14


@pytest.mark.asyncio
async def test_flox_handler_tree_cover_loss_from_fires():
    dask_cluster = LocalCluster(asynchronous=True)
    dask_client = Client(dask_cluster)

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
        end_year="2022",
        intersections=["fire"],
    ).model_dump()

    analysis = Analysis(None, analytics_in, AnalysisStatus.saved)

    analyzer = TreeCoverLossAnalyzer(
        compute_engine=compute_engine, input_uris=INPUT_URIS[Environment.production]
    )
    await analyzer.analyze(analysis)
    results = analysis.result

    # Fixture: fires year=21 in the same top half where TCL year=21. With
    # canopy_cover>=30 (right 5 cols) and TCL year filter 2010-2022 (top 5 rows),
    # the 25 overlapping pixels are all fire-driven loss: fires = TCL = 125000;
    # non-fires = 0.
    pd.testing.assert_frame_equal(
        pd.DataFrame(results),
        pd.DataFrame(
            {
                "tree_cover_loss_year": [2021],
                "area_ha": [125000.0],
                "aoi_id": ["1234"],
                "aoi_type": ["protected_area"],
                "carbon_emissions_MgCO2e": [37.5],
                "tree_cover_loss_from_fires_area_ha": [125000.0],
                "tree_cover_loss_non_fires_area_ha": [0.0],
            },
        ),
        check_like=True,
        check_exact=False,  # Allow approximate comparison for numbers
        atol=1e-8,  # Absolute tolerance
        rtol=1e-4,  # Relative tolerance
    )
