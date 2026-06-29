import asyncio
from unittest.mock import patch

import dask
import numpy as np
import pandas as pd
import pytest
import rioxarray  # noqa: F401
import xarray as xr

from app.domain.analyzers import zonal_statistics_analyzer
from app.domain.analyzers.integrated_alerts_analyzer import IntegratedAlertsAnalyzer
from app.domain.models.analysis import Analysis
from app.domain.models.dataset import Dataset
from app.infrastructure.external_services.duck_db_query_service import (
    DuckDbPrecalcQueryService,
)
from app.models.land_change.integrated_alerts import IntegratedAlertsAnalyticsIn

# Pixel areas down one column of latitude; reused for both fixtures and for
# computing expected per-group sums so nothing is a magic number.
COLUMN_AREAS = np.array(
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

Y_VALS = np.linspace(48.0, 47.99775, 10)  # latitude, descending (matches clip slice)
X_VALS = np.linspace(105.0, 105.00225, 10)  # longitude, ascending


def make_analysis(aoi, start_date="2029-01-01", end_date="2032-12-31") -> Analysis:
    metadata = IntegratedAlertsAnalyticsIn(
        aoi=aoi, start_date=start_date, end_date=end_date
    ).model_dump()
    return Analysis(result=None, metadata=metadata, status="pending")


class TestOtfTimeout:
    @pytest.mark.asyncio
    async def test_otf_times_out_when_compute_hangs(self, monkeypatch):
        class HangingEngine:
            def map(self, *args, **kwargs):
                return ["future"]

            async def gather(self, *args, **kwargs):
                await asyncio.sleep(10)

            async def compute(self, *args, **kwargs):
                await asyncio.sleep(10)

        async def fake_get_geojson(aois):
            return [{"type": "Polygon", "coordinates": []}]

        monkeypatch.setattr(zonal_statistics_analyzer, "get_geojson", fake_get_geojson)

        analyzer = IntegratedAlertsAnalyzer(
            compute_engine=HangingEngine(),
            input_uris={"x": "y"},
            otf_timeout_seconds=0.01,
        )
        analysis = make_analysis({"type": "protected_area", "ids": ["555625448"]})

        with pytest.raises(asyncio.TimeoutError):
            await analyzer.analyze(analysis)


class TestPrecomputedAdminAnalysis:
    @pytest.fixture
    def precomputed_admin_results(self, tmp_path):
        # Parquet keyed by aoi_id and preaggregated to grain
        # (aoi_id, alert_date, alert_confidence).
        rows = [
            ("BRA.1", "2024-01-01", "low", 10.0),
            ("BRA.1", "2024-01-01", "high", 5.0),
            ("BRA.1", "2024-06-15", "highest", 2.0),
            ("BRA.1", "2024-12-31", "low", 1.0),
            ("BRA.1", "2023-12-31", "high", 99.0),  # before window -> excluded
            ("BRA.1.2", "2024-03-03", "low", 7.0),  # other aoi -> excluded
        ]
        df = pd.DataFrame(
            rows,
            columns=[
                "aoi_id",
                "alert_date",
                "alert_confidence",
                "area_ha",
            ],
        )
        df["alert_date"] = pd.to_datetime(df["alert_date"])

        parquet_file = tmp_path / "admin-integrated-alerts.parquet"
        df.to_parquet(parquet_file, index=False)
        return parquet_file

    @pytest.mark.asyncio
    async def test_filters_by_aoi_id_and_date_window(self, precomputed_admin_results):
        analyzer = IntegratedAlertsAnalyzer(
            duckdb_query_service=DuckDbPrecalcQueryService(
                table_uri=precomputed_admin_results
            ),
            input_uris={},
        )
        analysis = make_analysis(
            {"type": "admin", "ids": ["BRA.1"]},
            start_date="2024-01-01",
            end_date="2024-12-31",
        )

        await analyzer.analyze(analysis)
        df = pd.DataFrame(analysis.result)

        # ordered by aoi_id, alert_date, alert_confidence (high < highest < low)
        expected = pd.DataFrame(
            {
                "aoi_id": ["BRA.1", "BRA.1", "BRA.1", "BRA.1"],
                "alert_date": [
                    "2024-01-01",
                    "2024-01-01",
                    "2024-06-15",
                    "2024-12-31",
                ],
                "alert_confidence": ["high", "low", "highest", "low"],
                "area_ha": [5.0, 10.0, 2.0, 1.0],
                "aoi_type": ["admin", "admin", "admin", "admin"],
            }
        )

        pd.testing.assert_frame_equal(expected, df, check_like=True, check_dtype=False)


class TestOtfAnalysis:
    @pytest.fixture
    def alerts_datacube(self):
        # alert_date constant; confidence in three bands so 2/3/4 -> low/high/highest
        # all appear in the result.
        confidence_by_row = np.array([2, 2, 2, 3, 3, 3, 3, 4, 4, 4], dtype=np.uint8)
        confidence = np.tile(confidence_by_row[:, np.newaxis], (1, 10))
        alert_date = np.full((10, 10), 1095, dtype=np.int64)  # -> 2023-12-31

        return xr.Dataset(
            {
                "alert_date": (["y", "x"], alert_date),
                "confidence": (["y", "x"], confidence),
            },
            coords={"y": Y_VALS, "x": X_VALS},
        )

    @pytest.fixture
    def pixel_area(self):
        areas_2d = np.tile(COLUMN_AREAS[:, np.newaxis], (1, 10))
        areas_3d = areas_2d[np.newaxis, :, :]  # (band, y, x)
        return xr.Dataset(
            {"band_data": (["band", "y", "x"], areas_3d)},
            coords={"band": [1], "y": Y_VALS, "x": X_VALS},
        )

    @pytest.mark.asyncio
    @patch("app.analysis.common.analysis.read_zarr")
    async def test_otf_groups_by_date_and_confidence(
        self, mock_read_zarr, alerts_datacube, pixel_area
    ):
        mock_read_zarr.side_effect = [alerts_datacube, pixel_area]

        input_uris = {
            "integrated_alerts_zarr_uri": "memory://alerts",
            str(Dataset.pixel_area_m2_10m): "memory://area",
        }
        aoi = {"type": "Feature", "properties": {"id": "test_otf"}}
        # polygon encloses the whole grid so every pixel survives the clip
        geojson = {
            "type": "Polygon",
            "coordinates": [
                [
                    [104.9999, 47.9976],
                    [105.0024, 47.9976],
                    [105.0024, 48.0001],
                    [104.9999, 48.0001],
                    [104.9999, 47.9976],
                ]
            ],
        }

        with dask.config.set(scheduler="synchronous"):
            result_df = IntegratedAlertsAnalyzer.analyze_area(
                input_uris, aoi, geojson, "2020-01-01", "2099-12-31"
            )
            computed = result_df.compute()

        computed = computed.sort_values("alert_confidence").reset_index(drop=True)

        expected = (
            pd.DataFrame(
                {
                    "alert_date": ["2023-12-31", "2023-12-31", "2023-12-31"],
                    "alert_confidence": ["high", "highest", "low"],
                    # COLUMN_AREAS are m² per pixel; 10 pixels/row, m² -> ha (/10000).
                    # high=rows3-6, highest=rows7-9, low=rows0-2.
                    "area_ha": [
                        10 * COLUMN_AREAS[3:7].sum() / 10000,
                        10 * COLUMN_AREAS[7:10].sum() / 10000,
                        10 * COLUMN_AREAS[0:3].sum() / 10000,
                    ],
                    "aoi_type": ["feature", "feature", "feature"],
                    "aoi_id": ["test_otf", "test_otf", "test_otf"],
                }
            )
            .sort_values("alert_confidence")
            .reset_index(drop=True)
        )

        pd.testing.assert_frame_equal(
            expected,
            computed,
            check_like=True,
            check_dtype=False,
            rtol=1e-4,
        )
