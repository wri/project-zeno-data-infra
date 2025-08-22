from unittest.mock import patch

import duckdb
import numpy as np
import pandas as pd
import pytest
import pytest_asyncio
import xarray as xr
from app.domain.analyzers.land_cover_composition_analyzer import (
    LandCoverCompositionAnalyzer,
)
from app.domain.models.analysis import Analysis
from app.models.common.analysis import AnalysisStatus
from app.models.land_change.land_cover_composition import (
    LandCoverCompositionAnalyticsIn,
)
from dask.distributed import Client


@pytest_asyncio.fixture
async def async_dask_client():
    async with Client(
        processes=False,
        n_workers=1,
        threads_per_worker=2,
        silence_logs=True,
        dashboard_address=None,
        asynchronous=True,
    ) as client:
        yield client


class DummyAnalysisRepository:
    def __init__(self):
        self.analysis = None

    async def store_analysis(self, resource_id, analysis):
        self.analysis = analysis


class TestLandCoverCompositionCustomAois:
    @pytest.fixture
    def land_cover_composition_datacube(self):
        years = np.array([2015, 2024])
        y_vals = np.linspace(48.0, 47.99775, 10)
        x_vals = np.linspace(105.0, 105.00225, 10)

        data_2015 = np.array(
            [
                [2, 2, 1, 1, 1, 1, 6, 6, 8, 8],
                [2, 2, 1, 1, 1, 1, 6, 6, 8, 8],
                [1, 1, 1, 1, 3, 3, 1, 1, 1, 1],
                [1, 1, 1, 1, 3, 3, 1, 1, 1, 1],
                [1, 1, 1, 1, 4, 4, 1, 1, 1, 1],
                [1, 1, 1, 1, 4, 4, 1, 1, 1, 1],
                [6, 6, 1, 1, 1, 1, 1, 1, 6, 6],
                [6, 6, 1, 1, 1, 1, 1, 1, 6, 6],
                [8, 8, 8, 8, 1, 1, 8, 8, 8, 8],
                [8, 8, 8, 8, 1, 1, 8, 8, 8, 8],
            ],
            dtype=np.uint8,
        )

        data_2024 = np.array(
            [
                [6, 6, 1, 1, 1, 1, 7, 7, 6, 6],
                [6, 6, 1, 1, 1, 1, 7, 7, 6, 6],
                [1, 1, 6, 6, 3, 3, 6, 6, 1, 1],
                [1, 1, 6, 6, 3, 3, 6, 6, 1, 1],
                [1, 1, 6, 6, 4, 4, 6, 6, 1, 1],
                [1, 1, 6, 6, 4, 4, 6, 6, 1, 1],
                [7, 7, 6, 6, 1, 1, 6, 6, 7, 7],
                [7, 7, 6, 6, 1, 1, 6, 6, 7, 7],
                [6, 6, 6, 6, 7, 7, 6, 6, 6, 6],
                [6, 6, 6, 6, 7, 7, 6, 6, 6, 6],
            ],
            dtype=np.uint8,
        )

        data = np.stack([data_2015, data_2024], axis=0)

        band_data = xr.DataArray(
            data,
            coords={
                "year": years,
                "y": y_vals,
                "x": x_vals,
                "spatial_ref": ((), 0, {}),
            },
            dims=["year", "y", "x"],
            name="band_data",
        )
        return xr.Dataset({"band_data": band_data})

    @pytest.fixture
    def pixel_area(self):
        # Define the base column (along y) — 10 values
        column_vals = np.array(
            [
                518.6011,
                518.6036,
                518.606,
                518.6085,
                518.611,
                518.61346,
                518.61597,
                518.61847,
                518.6209,
                518.6234,
            ],
            dtype=np.float32,
        )

        y_vals = np.linspace(48.0, 47.99775, 10)  # latitude
        x_vals = np.linspace(105.0, 105.00225, 10)  # longitude

        areas_2d = np.tile(column_vals[:, np.newaxis], (1, 10))
        areas_3d = areas_2d[np.newaxis, :, :]  # shape: (1, 10, 10)
        pixel_area = xr.DataArray(
            areas_3d,
            coords={"band": [1], "y": y_vals, "x": x_vals},
            dims=["band", "y", "x"],
            name="band_data",
        )

        ds = xr.Dataset({"band_data": pixel_area})
        return ds

    @pytest_asyncio.fixture(autouse=True)
    @patch(
        "app.domain.analyzers.land_cover_composition_analyzer.read_zarr_clipped_to_geojson"
    )
    async def run_analysis(
        self,
        mock_read_zarr_clipped_to_geojson,
        land_cover_composition_datacube,
        pixel_area,
        async_dask_client,
    ):
        mock_read_zarr_clipped_to_geojson.side_effect = [
            land_cover_composition_datacube,
            pixel_area,
        ]
        self.analysis_repo = DummyAnalysisRepository()
        analyzer = LandCoverCompositionAnalyzer(
            analysis_repository=self.analysis_repo, compute_engine=async_dask_client
        )

        feature_collection = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"id": "test_aoi"},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [105.0006, 47.9987],
                                [105.0016, 47.9987],
                                [105.0016, 47.9978],
                                [105.0006, 47.9978],
                                [105.0006, 47.9987],
                            ]
                        ],
                    },
                }
            ],
        }
        self.metadata = LandCoverCompositionAnalyticsIn(
            aoi={
                "type": "feature_collection",
                "feature_collection": feature_collection,
            },
        ).model_dump()

        analysis = Analysis(None, self.metadata, AnalysisStatus.saved)
        await analyzer.analyze(analysis)

    @pytest.mark.asyncio
    async def test_analysis_result(self):
        assert self.analysis_repo.analysis is not None
        assert self.analysis_repo.analysis.status == AnalysisStatus.saved
        assert self.analysis_repo.analysis.metadata == self.metadata

        expected = pd.DataFrame(
            {
                "land_cover_class_area__ha": [
                    1.4521043300628662,
                    0.20744292438030243,
                    0.20744489133358002,
                    2.489346504211426,
                    0.8297836780548096,
                ],
                "land_cover_class": [
                    "Short vegetation",
                    "Wetland – short vegetation",
                    "Water",
                    "Cropland",
                    "Built-up",
                ],
                "aoi_type": ["feature", "feature", "feature", "feature", "feature"],
                "aoi_id": ["test_aoi", "test_aoi", "test_aoi", "test_aoi", "test_aoi"],
            }
        )

        pd.testing.assert_frame_equal(
            pd.DataFrame(self.analysis_repo.analysis.result), expected, check_like=True
        )


class TestLandCoverChangeAdminAois:
    @pytest.fixture
    def parquet_mock_data(self):
        all_data = [
            # ADM2 level
            ("BRA.12.1", "Tree cover", 2.15032),
            ("BRA.12.1", "Cropland", 4.89045),
            ("BRA.12.1", "Short vegetation", 1.78934),
            ("BRA.12.1", "Built-up", 0.85667),
            ("BRA.12.1", "Wetland – short vegetation", 0.34521),
            # ADM0 level - Indonesia country
            ("IDN", "Tree cover", 987654.32),
            ("IDN", "Cropland", 567890.12),
            ("IDN", "Short vegetation", 345678.90),
            ("IDN", "Wetland – short vegetation", 234567.89),
            ("IDN", "Water", 198765.43),
            ("IDN", "Built-up", 87654.32),
            ("IDN", "Bare and sparse vegetation", 45678.90),
        ]

        return pd.DataFrame(
            all_data,
            columns=[
                "aoi_id",
                "land_cover_class",
                "land_cover_class_area__ha",
            ],
        )

    @pytest_asyncio.fixture(autouse=True)
    async def analyzer_with_test_data(self, parquet_mock_data, async_dask_client):
        self.analysis_repo = DummyAnalysisRepository()
        analyzer = LandCoverCompositionAnalyzer(
            analysis_repository=self.analysis_repo, compute_engine=async_dask_client
        )

        table_name = "test_parquet"
        duckdb.register(table_name, parquet_mock_data)
        analyzer.admin_results_local_uri = table_name

        return analyzer

    @pytest_asyncio.fixture(autouse=True)
    async def run_analysis(
        self,
        analyzer_with_test_data,
    ):
        analytics_in = LandCoverCompositionAnalyticsIn(
            aoi={"type": "admin", "ids": ["BRA.12.1", "IDN"]},
        )

        analysis = Analysis(
            metadata=analytics_in.model_dump(),
            result=None,
            status=AnalysisStatus.pending,
        )

        await analyzer_with_test_data.analyze(analysis)

    def test_analysis_result(self):
        expected = pd.DataFrame(
            {
                "land_cover_class": [
                    "Tree cover",
                    "Cropland",
                    "Short vegetation",
                    "Built-up",
                    "Wetland – short vegetation",
                    "Tree cover",
                    "Cropland",
                    "Short vegetation",
                    "Wetland – short vegetation",
                    "Water",
                    "Built-up",
                    "Bare and sparse vegetation",
                ],
                "land_cover_class_area__ha": [
                    2.15032,
                    4.89045,
                    1.78934,
                    0.85667,
                    0.34521,
                    987654.32,
                    567890.12,
                    345678.90,
                    234567.89,
                    198765.43,
                    87654.32,
                    45678.90,
                ],
                "aoi_id": [
                    "BRA.12.1",
                    "BRA.12.1",
                    "BRA.12.1",
                    "BRA.12.1",
                    "BRA.12.1",
                    "IDN",
                    "IDN",
                    "IDN",
                    "IDN",
                    "IDN",
                    "IDN",
                    "IDN",
                ],
                "aoi_type": [
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                ],
            }
        )

        pd.testing.assert_frame_equal(
            pd.DataFrame(self.analysis_repo.analysis.result),
            expected,
            check_like=True,
            rtol=1e-4,
            atol=1e-4,
        )
