import asyncio

import numpy as np
import pandas as pd
import pytest
from dask.dataframe import DataFrame as DaskDataFrame

from app.analysis.dist_alerts.analysis import zonal_statistics


class TestDistAlertsZonalStats:
    @pytest.mark.asyncio
    async def test_zonal_statistics_drivers_happy_path(self) -> None:
        geojson = {
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
        }

        _: DaskDataFrame = await zonal_statistics(
            aoi={"type": "indigenous_land", "id": "1918"},
            geojson=geojson,
            version="v20251004",
            intersection="driver",
        )

    @pytest.mark.asyncio
    async def test_zonal_statistics_grasslands_happy_path(self) -> None:
        geojson = {
            "type": "Polygon",
            "coordinates": [
                [
                    [34.92, -2.96],
                    [34.96, -2.96],
                    [34.96, -3.0],
                    [34.92, -3.0],
                    [34.92, -2.96],
                ]
            ],
        }

        aoi = {
            "type": "Feature",
            "properties": {"id": "test_aoi"},
            "start_date": "2024-03-16",
            "end_date": "2024-08-17",
            "geometry": geojson,
        }
        result_df: DaskDataFrame = await zonal_statistics(
            aoi, aoi["geometry"], version="v20251004", intersection="grasslands"
        )

        loop = asyncio.get_event_loop()
        computed_df = await loop.run_in_executor(None, result_df.compute)
        expected_df = pd.DataFrame(
            {
                "dist_alert_date": [
                    "2024-03-16",
                    "2024-08-08",
                    "2024-08-13",
                    "2024-08-16",
                    "2024-08-16",
                    "2024-08-18",
                    "2025-01-07",
                    "2025-01-23",
                    "2025-01-23",
                    "2025-02-04",
                    "2025-02-16",
                    "2025-02-19",
                    "2025-02-24",
                    "2025-03-04",
                    "2025-04-30",
                    "2025-05-02",
                    "2025-06-11",
                    "2025-06-14",
                ],
                "dist_alert_confidence": [
                    "high",
                    "high",
                    "high",
                    "high",
                    "high",
                    "high",
                    "high",
                    "low",
                    "high",
                    "high",
                    "high",
                    "high",
                    "high",
                    "low",
                    "low",
                    "low",
                    "high",
                    "high",
                ],
                "grasslands": [
                    "grasslands",
                    "non-grasslands",
                    "non-grasslands",
                    "non-grasslands",
                    "grasslands",
                    "non-grasslands",
                    "non-grasslands",
                    "non-grasslands",
                    "non-grasslands",
                    "non-grasslands",
                    "non-grasslands",
                    "non-grasslands",
                    "non-grasslands",
                    "non-grasslands",
                    "grasslands",
                    "grasslands",
                    "grasslands",
                    "grasslands",
                ],
                "area_ha": np.array(
                    [
                        4609.902832,
                        2304.878756,
                        150585.3119,
                        21512.226562,
                        2304.879883,
                        3073.176270,
                        2304.8821,
                        768.290833,
                        3841.4596,
                        1536.5877,
                        2304.882080,
                        768.291687,
                        768.294250,
                        768.291687,
                        1536.6202,
                        12292.9770,
                        6146.5273,
                        768.3173,
                    ],
                    dtype=np.float32,
                )
                / 10000,
                "aoi_type": [
                    "feature",
                    "feature",
                    "feature",
                    "feature",
                    "feature",
                    "feature",
                    "feature",
                    "feature",
                    "feature",
                    "feature",
                    "feature",
                    "feature",
                    "feature",
                    "feature",
                    "feature",
                    "feature",
                    "feature",
                    "feature",
                ],
                "aoi_id": [
                    "test_aoi",
                    "test_aoi",
                    "test_aoi",
                    "test_aoi",
                    "test_aoi",
                    "test_aoi",
                    "test_aoi",
                    "test_aoi",
                    "test_aoi",
                    "test_aoi",
                    "test_aoi",
                    "test_aoi",
                    "test_aoi",
                    "test_aoi",
                    "test_aoi",
                    "test_aoi",
                    "test_aoi",
                    "test_aoi",
                ],
            }
        )
        print(computed_df)
        pd.testing.assert_frame_equal(
            expected_df,
            computed_df,
            check_like=True,
            check_dtype=False,
            check_exact=False,  # Allow approximate comparison for numbers
            atol=1e-8,  # Absolute tolerance
            rtol=1e-4,  # Relative tolerance
        )

    @pytest.mark.asyncio
    async def test_zonal_statistics_land_cover_happy_path(self) -> None:
        geojson = {
            "type": "Polygon",
            "coordinates": [
                [
                    [34.92, -2.997],
                    [34.93, -2.997],
                    [34.93, -3.0],
                    [34.92, -3.0],
                    [34.92, -2.997],
                ]
            ],
        }

        aoi = {
            "type": "Feature",
            "properties": {"id": "test_aoi"},
            "geometry": geojson,
        }
        result_df: DaskDataFrame = await zonal_statistics(
            aoi, aoi["geometry"], version="v20251004", intersection="land_cover"
        )

        loop = asyncio.get_event_loop()
        computed_df = await loop.run_in_executor(None, result_df.compute)
        expected_df = pd.DataFrame(
            {
                "dist_alert_date": [
                    "2024-08-13",
                    "2024-08-13",
                    "2024-08-13",
                    "2024-08-13",
                    "2024-08-16",
                    "2025-01-23",
                    "2025-01-23",
                    "2025-01-23",
                    "2025-02-19",
                    "2025-03-04",
                ],
                "dist_alert_confidence": [
                    "high",
                    "high",
                    "high",
                    "high",
                    "high",
                    "low",
                    "high",
                    "high",
                    "high",
                    "low",
                ],
                "land_cover_class": [
                    "Short vegetation",
                    "Tree cover",
                    "Wetland – short vegetation",
                    "Cropland",
                    "Short vegetation",
                    "Cropland",
                    "Short vegetation",
                    "Cropland",
                    "Tree cover",
                    "Short vegetation",
                ],
                "area_ha": np.array(
                    [
                        33036.54,
                        3841.4575,
                        2304.8757,
                        63768.2417,
                        2304.8762,
                        768.29083,
                        2304.876,
                        1536.5837,
                        768.2917,
                        768.2917,
                    ],
                    dtype=np.float32,
                )
                / 10000,
                "aoi_type": [
                    "feature",
                    "feature",
                    "feature",
                    "feature",
                    "feature",
                    "feature",
                    "feature",
                    "feature",
                    "feature",
                    "feature",
                ],
                "aoi_id": [
                    "test_aoi",
                    "test_aoi",
                    "test_aoi",
                    "test_aoi",
                    "test_aoi",
                    "test_aoi",
                    "test_aoi",
                    "test_aoi",
                    "test_aoi",
                    "test_aoi",
                ],
            }
        )
        print(computed_df)
        pd.testing.assert_frame_equal(
            expected_df,
            computed_df,
            check_dtype=False,
            check_like=True,
            check_exact=False,  # Allow approximate comparison for numbers
            atol=1e-8,  # Absolute tolerance
            rtol=1e-4,  # Relative tolerance
        )
