import os
from unittest.mock import patch

import dask
import numpy as np
import pandas as pd
import pytest
import pytest_asyncio
import xarray as xr
from dask.distributed import Client

from app.domain.analyzers.land_cover_change_analyzer import LandCoverChangeAnalyzer
from app.domain.models.analysis import Analysis
from app.infrastructure.external_services.duck_db_query_service import (
    DuckDbPrecalcQueryService,
)
from app.models.common.analysis import AnalysisStatus
from app.models.land_change.land_cover_change import (
    LandCoverChangeAnalyticsIn,
)


# Add this fixture to override the config
@pytest.fixture(autouse=True)
def clear_dask_scheduler_config():
    """Clear Dask scheduler config to force local cluster creation."""
    with dask.config.set({"scheduler-address": None}):
        yield


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


class TestLandCoverChangeCustomAois:
    @pytest.fixture
    def land_cover_change_datacube(self):
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
        column_vals = (
            np.array(
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
            / 10000
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
        "app.domain.analyzers.land_cover_change_analyzer.read_zarr_clipped_to_geojson"
    )
    async def run_analysis(
        self,
        mock_read_zarr_clipped_to_geojson,
        land_cover_change_datacube,
        pixel_area,
        async_dask_client,
    ):
        mock_read_zarr_clipped_to_geojson.side_effect = [
            land_cover_change_datacube,
            pixel_area,
        ]
        self.analysis_repo = DummyAnalysisRepository()
        analyzer = LandCoverChangeAnalyzer(
            analysis_repository=self.analysis_repo, compute_engine=async_dask_client
        )

        feature_collection = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "id": "test_aoi",
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
        self.metadata = LandCoverChangeAnalyticsIn(
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
                "area_ha": [
                    1.2446691989898682,
                    0.20744886994361877,
                    0.20744091272354126,
                    0.6223347783088684,
                    1.0372363328933716,
                ],
                "land_cover_class_start": [
                    "Short vegetation",
                    "Short vegetation",
                    "Tree cover",
                    "Cropland",
                    "Cultivated grasslands",
                ],
                "land_cover_class_end": [
                    "Cropland",
                    "Built-up",
                    "Cropland",
                    "Built-up",
                    "Cropland",
                ],
                "aoi_type": ["feature", "feature", "feature", "feature", "feature"],
                "aoi_id": ["test_aoi", "test_aoi", "test_aoi", "test_aoi", "test_aoi"],
            }
        )

        pd.testing.assert_frame_equal(
            pd.DataFrame(self.analysis_repo.analysis.result),
            expected,
            check_like=True,
            check_exact=False,  # Allow approximate comparison for numbers
            atol=1e-8,  # Absolute tolerance
            rtol=1e-4,  # Relative tolerance
        )


class TestLandCoverChangeAdminAois:
    @pytest.fixture
    def parquet_mock_data(self):
        """Mock data that simulates the actual parquet file structure for the AOI"""
        brazil_data = [
            ("BRA.12.1", "Tree cover", "Cropland", 0.12505),
            ("BRA.12.1", "Tree cover", "Built-up", 0.03208),
            ("BRA.12.1", "Cropland", "Short vegetation", 0.08902),
            ("BRA.12.2", "Tree cover", "Cropland", 0.08003),
            ("BRA.12.2", "Tree cover", "Built-up", 0.01507),
            ("BRA.12.2", "Cultivated grasslands", "Cropland", 0.01804),
            ("BRA.12.3", "Tree cover", "Cropland", 0.09502),
            ("BRA.12.3", "Wetland – short vegetation", "Tree cover", 0.01204),
        ]

        indonesia_data = [
            ("IDN.24.9", "Tree cover", "Cropland", 0.21503),
            ("IDN.24.9", "Tree cover", "Built-up", 0.05674),
            ("IDN.24.9", "Wetland – short vegetation", "Water", 0.02348),
        ]

        all_data = brazil_data + indonesia_data

        return pd.DataFrame(
            all_data,
            columns=[
                "aoi_id",
                "land_cover_class_start",
                "land_cover_class_end",
                "area_ha",
            ],
        )

    @pytest_asyncio.fixture(autouse=True)
    async def analyzer_with_test_data(self, parquet_mock_data, async_dask_client):
        self.analysis_repo = DummyAnalysisRepository()
        table_name = "/tmp/test.parquet"
        parquet_mock_data.to_parquet("/tmp/test.parquet", index=False)

        query_service = DuckDbPrecalcQueryService(table_name)
        analyzer = LandCoverChangeAnalyzer(
            analysis_repository=self.analysis_repo,
            compute_engine=async_dask_client,
            query_service=query_service,
        )

        analyzer.admin_results_uri = table_name

        return analyzer

    @pytest_asyncio.fixture(autouse=True)
    async def run_analysis(
        self,
        analyzer_with_test_data,
    ):
        analytics_in = LandCoverChangeAnalyticsIn(
            aoi={"type": "admin", "ids": ["BRA.12.1", "IDN.24.9"]},
        )

        analysis = Analysis(
            metadata=analytics_in.model_dump(),
            result=None,
            status=AnalysisStatus.pending,
        )

        await analyzer_with_test_data.analyze(analysis)

    def test_analysis_result(self):
        try:
            expected = pd.DataFrame(
                {
                    "land_cover_class_start": [
                        "Tree cover",
                        "Tree cover",
                        "Cropland",
                        "Tree cover",
                        "Tree cover",
                        "Wetland – short vegetation",
                    ],
                    "land_cover_class_end": [
                        "Cropland",
                        "Built-up",
                        "Short vegetation",
                        "Cropland",
                        "Built-up",
                        "Water",
                    ],
                    "area_ha": [
                        0.12505,
                        0.03208,
                        0.08902,
                        0.2150,
                        0.05674,
                        0.02348,
                    ],
                    "aoi_id": [
                        "BRA.12.1",
                        "BRA.12.1",
                        "BRA.12.1",
                        "IDN.24.9",
                        "IDN.24.9",
                        "IDN.24.9",
                    ],
                    "aoi_type": [
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
        finally:
            os.remove("/tmp/test.parquet")
