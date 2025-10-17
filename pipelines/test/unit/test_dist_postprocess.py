import pandas as pd

from pipelines.disturbance.stages import postprocess_result_dataframe


def test_postprocess_result_dataframe():
    test_df = pd.DataFrame(
        {
            "country": ["BRA", "BRA", "BRA"],
            "region": [1, 2, 3],
            "subregion": [4, 5, 6],
            "land_cover_class": ["Forest", "Grasslands", "Grasslands"],
            "alert_date": [100, 200, 200],
            "confidence": [2, 3, 3],
            "value": [10, 100, 1000],
        }
    )

    results_df = postprocess_result_dataframe(test_df)
    assert len(results_df) == 8
    assert set(results_df.columns) == set(
        {
            "aoi_id",
            "land_cover_class",
            "dist_alert_confidence",
            "area_ha",
            "dist_alert_date",
        }
    )
