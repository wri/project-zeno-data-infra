import pytest
import pytest_asyncio
import xarray as xr
import numpy as np
from dask.distributed import Client
from unittest.mock import patch

from app.domain.analyzers.land_cover_change_analyzer import LandCoverChangeAnalyzer
from app.domain.models.analysis import Analysis
from app.models.common.analysis import AnalysisStatus
from app.models.land_change.land_cover import LandCoverChangeAnalyticsIn


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
        return xr.Dataset({"lc_classes": band_data})

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
        assert self.analysis_repo.analysis.result == {
            "change_area": [
                12446.6923828125,
                2074.48876953125,
                2074.4091796875,
                6223.34765625,
                10372.36328125,
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
