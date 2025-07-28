import pandas as pd

from api.app.models.land_change.land_cover import LandCoverChangeAnalyticsIn
from api.app.use_cases.analysis.land_cover.land_cover_change import (
    LandCoverChangeService,
)


class TestLandCoverChangeMockData:
    def test_mock_results_gadm(self):
        land_cover_change_service = LandCoverChangeService(mock=True)
        land_cover_change_in = LandCoverChangeAnalyticsIn(
            aoi={"type": "admin", "ids": ["BRA.1.12", "IDN.24.8"]}
        )

        mock_results = land_cover_change_service.get_results(land_cover_change_in)
        mock_results_df = pd.DataFrame(mock_results)
        expected_results_df = pd.DataFrame(
            {
                "id": [
                    "BRA.1.12",
                    "BRA.1.12",
                    "BRA.1.12",
                    "IDN.24.8",
                    "IDN.24.8",
                    "IDN.24.8",
                ],
                "land_cover_class_start": [
                    "Forest",
                    "Croplands",
                    "Cultivated Grasslands",
                    "Forest",
                    "Croplands",
                    "Cultivated Grasslands",
                ],
                "land_cover_class_end": [
                    "Cultivated Grasslands",
                    "Croplands",
                    "Forest",
                    "Cultivated Grasslands",
                    "Croplands",
                    "Forest",
                ],
                "area_ha": [1, 2, 3, 1, 2, 3],
            }
        )

        pd.testing.assert_frame_equal(
            expected_results_df, mock_results_df, check_like=True
        )
