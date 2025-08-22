import pytest

from app.analysis.dist_alerts.analysis import zonal_statistics
from app.models.land_change.dist_alerts import DistAlertsAnalytics

import asyncio
import numpy as np
import pandas as pd
from dask.dataframe import DataFrame as DaskDataFrame


class TestDistAlertsZonalStats:

    @pytest.mark.asyncio
    async def test_zonal_statistics_drivers_happy_path(self):
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

        _: DistAlertsAnalytics = await zonal_statistics(
            aoi={"type": "indigenous_land", "id": "1918"},
            geojson=geojson,
            intersection="driver",
        )



    @pytest.mark.asyncio
    async def test_zonal_statistics_grasslands_happy_path(
        self
    ) -> None:
        geojson = {
            "type": "Polygon",
            "coordinates": [
                [
                    [34.92, -2.96],
                    [34.96, -2.96],
                    [34.96, -3.0],
                    [34.92, -3.0],
                    [34.92, -2.96]
                ]
            ],
        }

        aoi = {
            "type": "Feature",
            "properties": {"id": "test_aoi"},
            # "start_date": "2024-03-16",
            # "end_date": "2024-08-17",
            "geometry": geojson,
        }
        result_df: DaskDataFrame = await zonal_statistics(aoi, aoi["geometry"], intersection="grasslands")

        loop = asyncio.get_event_loop()
        computed_df = await loop.run_in_executor(None, result_df.compute)
        expected_df = pd.DataFrame(
            {
                "alert_date": ["2024-03-16", "2024-08-08", "2024-08-13", "2024-08-16", "2024-08-16", "2024-08-18", "2025-01-07", "2025-01-23", "2025-01-23", "2025-02-04", "2025-02-16", "2025-02-19", "2025-02-24", "2025-03-04"],
                "confidence": ["high", "high", "high", "high", "high", "high", "high", "low", "high", "high", "high", "high", "high", "low", ],
                "grasslands": ["grasslands", "non-grasslands", "non-grasslands", "non-grasslands", "grasslands", "non-grasslands", "non-grasslands", "non-grasslands", "non-grasslands", "non-grasslands", "non-grasslands", "non-grasslands", "non-grasslands", "non-grasslands", ],
                "value": np.array([4609.902832, 768.292908, 152121.796875, 21512.226562, 2304.879883, 2304.880127, 3073.176270, 768.290833, 4609.751465, 768.293762, 2304.882080, 768.291687, 768.294250, 768.291687], dtype=np.float32),
                "aoi_type": ["feature", "feature", "feature", "feature", "feature", "feature", "feature", "feature", "feature", "feature", "feature", "feature", "feature", "feature", ],
                "aoi_id": ["test_aoi", "test_aoi", "test_aoi", "test_aoi", "test_aoi", "test_aoi", "test_aoi", "test_aoi", "test_aoi", "test_aoi", "test_aoi", "test_aoi", "test_aoi", "test_aoi", ],
            }
        )
        print(computed_df)
        pd.testing.assert_frame_equal(expected_df, computed_df, check_like=True)
