from unittest.mock import patch

import pandas as pd
from shapely.geometry import box

from pipelines.test.integration.tree_cover_loss.conftest import (
    FakeGoogleEarthEngineDatasetRepository,
    FakeQCRepository,
)
from pipelines.tree_cover_loss import stages


def _make_result_df(area_ha, canopy_cover, driver, natural_forest_class):
    return pd.DataFrame(
        {
            "area_ha": area_ha,
            "tree_cover_loss_year": [2021, 2022],
            "canopy_cover": canopy_cover,
            "driver": driver,
            "aoi_id": ["AFG.1.1", "AFG.1.1"],
            "natural_forest_class": natural_forest_class,
        }
    )


def test_tcl_validation_flow():
    result_df = _make_result_df(
        [100.0, 200.0],
        ["30", "30"],
        ["Agriculture", "Permanent settlement"],
        ["Natural Forest", "Non-natural Forest"],
    )

    with patch(
        "pipelines.tree_cover_loss.stages.get_validation_statistics"
    ) as mock_validation:
        mock_validation.return_value = {
            "driver_results": pd.DataFrame({"area_ha": [100.0, 200.0]}),
            "natural_forests_results": pd.DataFrame({"area_ha": [100.0, 200.0]}),
        }

        assert (
            stages.qc_against_validation_source(
                result_df,
                qc_feature_repository=FakeQCRepository(),
            )
            is True
        )

        mock_validation.return_value = {
            "driver_results": pd.DataFrame({"area_ha": [100.0, 150.0]}),
            "natural_forests_results": pd.DataFrame({"area_ha": [100.0, 150.0]}),
        }
        assert (
            stages.qc_against_validation_source(
                result_df,
                qc_feature_repository=FakeQCRepository(),
            )
            is False
        )

    # canopy_cover below 30 → driver filter yields less area → should fail
    result_df_low_canopy = _make_result_df(
        [100.0, 200.0],
        ["10", "30"],
        ["Agriculture", "Permanent settlement"],
        ["Natural Forest", "Non-natural Forest"],
    )

    with patch(
        "pipelines.tree_cover_loss.stages.get_validation_statistics"
    ) as mock_validation:
        mock_validation.return_value = {
            "driver_results": pd.DataFrame({"area_ha": [100.0, 200.0]}),
            "natural_forests_results": pd.DataFrame({"area_ha": [100.0, 200.0]}),
        }
        assert (
            stages.qc_against_validation_source(
                result_df_low_canopy,
                qc_feature_repository=FakeQCRepository(),
            )
            is False
        )

    # "Unknown" driver → filtered out by driver != "Unknown" check → should fail
    result_df_unknown_driver = _make_result_df(
        [100.0, 200.0],
        ["30", "30"],
        ["Unknown", "Permanent settlement"],
        ["Natural Forest", "Non-natural Forest"],
    )

    with patch(
        "pipelines.tree_cover_loss.stages.get_validation_statistics"
    ) as mock_validation:
        mock_validation.return_value = {
            "driver_results": pd.DataFrame({"area_ha": [100.0, 200.0]}),
            "natural_forests_results": pd.DataFrame({"area_ha": [100.0, 200.0]}),
        }
        assert (
            bool(
                stages.qc_against_validation_source(
                    result_df_unknown_driver,
                    qc_feature_repository=FakeQCRepository(),
                )
            )
            is False
        )


def test_get_validation_statistics_with_fake_repo():
    geom = box(0, 0, 1, 1)

    result = stages.get_validation_statistics(
        geom, gee_repository=FakeGoogleEarthEngineDatasetRepository()
    )["driver_results"]

    expected = pd.DataFrame({"driver": [1.0, 3.0], "area_ha": [2.0, 2.0]})
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)
