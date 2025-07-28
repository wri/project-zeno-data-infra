import pandas as pd
import pytest

from api.app.models.land_change.land_cover import LandCoverChangeAnalyticsIn
from api.app.use_cases.analysis.land_cover.land_cover_change import (
    LandCoverChangeService,
)


class TestLandCoverChangeMockData:
    @pytest.mark.asyncio
    async def test_mock_results_gadm(self):
        land_cover_change_service = LandCoverChangeService(mock=True)
        land_cover_change_in = LandCoverChangeAnalyticsIn(
            aoi={"type": "admin", "ids": ["BRA.1.12", "IDN.24.8"]}
        )

        mock_results = await land_cover_change_service.get_results(land_cover_change_in)
        mock_results_df = pd.DataFrame(mock_results)
        expected_results_df = pd.DataFrame(
            {
                "id": (["BRA.1.12"] * 9) + (["IDN.24.8"] * 9),
                "land_cover_class_start": [
                    "Bare and sparse vegetation",
                    "Short vegetation",
                    "Tree cover",
                    "Wetland-short vegetation",
                    "Water",
                    "Snow/ice",
                    "Cropland",
                    "Built-up",
                    "Cultivated grasslands",
                ]
                * 2,
                "land_cover_class_end": [
                    "Cultivated grasslands",
                    "Built-up",
                    "Cropland",
                    "Snow/ice",
                    "Water",
                    "Wetland-short vegetation",
                    "Tree cover",
                    "Short vegetation",
                    "Bare and sparse vegetation",
                ]
                * 2,
                "area_ha": [1, 2, 3, 4, 5, 6, 7, 8, 9] * 2,
            }
        )

        pd.testing.assert_frame_equal(
            expected_results_df, mock_results_df, check_like=True
        )
