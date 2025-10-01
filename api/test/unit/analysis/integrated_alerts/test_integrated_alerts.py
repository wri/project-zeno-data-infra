from unittest.mock import patch

import dask
import numpy as np
import pandas as pd
import pytest
import rioxarray  # noqa: F401
import xarray as xr
from app.domain.analyzers.integrated_alerts_analyzer import IntegratedAlertsAnalyzer
from app.infrastructure.external_services.duck_db_query_service import (
    DuckDbPrecalcQueryService,
)
from dask.dataframe import DataFrame as DaskDataFrame


class TestIntegratedAlertsPreComputedAnalysis:
    @pytest.fixture
    def precomputed_gadm_results(self, tmp_path):
        data = [
            ["2023-01-01", "low", "BRA.1.2", 100],
            ["2024-01-01", "high", "BRA.1.2", 200],
            ["2025-01-01", "high", "BRA.1.2", 300],
        ]

        df = pd.DataFrame(
            data,
            columns=[
                "integrated_alerts_date",
                "integrated_alerts_confidence",
                "aoi_id",
                "integrated_alerts_count",
            ],
        )

        parquet_file = tmp_path / "integrated_alerts_data.parquet"
        df.to_parquet(parquet_file, index=False)

        return parquet_file

    @pytest.mark.asyncio
    async def test_precomputed_zonal_stats_for_region(self, precomputed_gadm_results):
        gadm_id = "BRA.1.2"

        integrated_alerts_analyzer = IntegratedAlertsAnalyzer(
            query_service=DuckDbPrecalcQueryService(table_uri=precomputed_gadm_results)
        )
        result_df: DaskDataFrame = await integrated_alerts_analyzer.analyze_admin_areas(
            [gadm_id],
        )

        # Aggregated yearly data
        data = [
            ["2023-01-01", "low", 100],
            ["2024-01-01", "high", 200],
            ["2025-01-01", "high", 300],
        ]

        expected_df = pd.DataFrame(
            data,
            columns=[
                "integrated_alerts_date",
                "integrated_alerts_confidence",
                "integrated_alerts_count",
            ],
        )

        expected_df["aoi_id"] = "BRA.1.2"
        expected_df["aoi_type"] = "admin"

        pd.testing.assert_frame_equal(
            expected_df,
            pd.DataFrame(result_df),
            check_like=True,
            check_dtype=False,
            check_exact=False,  # Allow approximate comparison for numbers
            atol=1e-8,  # Absolute tolerance
            rtol=1e-4,  # Relative tolerance
        )


class TestIntegratedAlertsOTFAnalysis:
    @pytest.fixture
    def integrated_alerts_datacube(self):
        y_vals = np.linspace(48.0, 47.99775, 10)
        x_vals = np.linspace(105.0, 105.00225, 10)

        values = np.tile(np.arange(4, dtype=np.uint8), 10 * 10 // 4)
        data = values.reshape((10, 10))

        band_data = xr.DataArray(
            data,
            coords={"y": y_vals, "x": x_vals},
            dims=["y", "x"],
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

    @pytest.mark.asyncio
    @patch("app.analysis.common.analysis.read_zarr")
    async def test_integrated_alerts_otf_analysis(
        self, mock_read_zarr, integrated_alerts_datacube, pixel_area
    ):
        mock_read_zarr.side_effect = [integrated_alerts_datacube, pixel_area]

        aoi = {
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

        with dask.config.set(scheduler="synchronous"):
            result_df = IntegratedAlertsAnalyzer.analyze_area(
                aoi, aoi["geometry"], 2000, 2022
            )
            computed_df = result_df.compute()

        years = np.arange(2000, 2023)
        expected_df = pd.DataFrame(
            {
                "year": years,
                "area_ha": np.array(([1555.85522] * len(years))).astype(np.float32),
                "aoi_type": ["feature"] * len(years),
                "aoi_id": ["test_aoi"] * len(years),
            }
        )

        pd.testing.assert_frame_equal(
            expected_df,
            computed_df,
            check_like=True,
            check_exact=False,  # Allow approximate comparison for numbers
            atol=1e-8,  # Absolute tolerance
            rtol=1e-4,  # Relative tolerance
        )
