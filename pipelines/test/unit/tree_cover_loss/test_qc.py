from unittest.mock import patch

import numpy as np
import pandas as pd
from shapely.geometry import box

from pipelines.test.integration.tree_cover_loss.conftest import (
    FakeGoogleEarthEngineDatasetRepository,
    FakeQCRepository,
)
from pipelines.tree_cover_loss.stages import TreeCoverLossTasks


def test_tcl_validation_flow():
    with patch(
        "pipelines.tree_cover_loss.stages.TreeCoverLossTasks.get_sample_statistics"
    ) as mock_sample, patch(
        "pipelines.tree_cover_loss.stages.TreeCoverLossTasks.get_validation_statistics"
    ) as mock_validation:
        tasks = TreeCoverLossTasks(qc_feature_repository=FakeQCRepository())
        mock_sample.return_value = pd.DataFrame(
            {
                "area_ha": [100.0, 200.0],
                "canopy_cover": ["30", "30"],
                "driver": ["Agriculture", "Permanent settlement"],
            }
        )
        mock_validation.return_value = pd.DataFrame({"area_ha": [100.0, 200.0]})

        assert bool(tasks.qc_against_validation_source()) is True

        mock_validation.return_value = pd.DataFrame({"area_ha": [100.0, 150.0]})
        assert bool(tasks.qc_against_validation_source()) is False

        mock_sample.return_value = pd.DataFrame(
            {
                "area_ha": [100.0, 200.0],
                "canopy_cover": ["10", "30"],
                "driver": ["Agriculture", "Permanent settlement"],
            }
        )
        assert bool(tasks.qc_against_validation_source()) is False

        mock_sample.return_value = pd.DataFrame(
            {
                "area_ha": [100.0, 200.0],
                "canopy_cover": ["30", "30"],
                "driver": [np.nan, "Permanent settlement"],
            }
        )
        assert bool(tasks.qc_against_validation_source()) is False


def test_get_sample_statistics_accepts_injected_geometry_lookup():
    geom = box(0, 0, 1, 1)
    expected = pd.DataFrame({"area_ha": [123.0]})

    with patch("pipelines.tree_cover_loss.stages.compute_tree_cover_loss") as mock_flow:
        mock_flow.return_value = expected
        tasks = TreeCoverLossTasks()

        result = tasks.get_sample_statistics(geom)

        assert result is expected

        mock_flow.assert_called_once()
        _, kwargs = mock_flow.call_args
        assert kwargs["bbox"] is geom


def test_get_validation_statistics_with_fake_repo():
    geom = box(0, 0, 1, 1)
    tasks = TreeCoverLossTasks(gee_repository=FakeGoogleEarthEngineDatasetRepository())

    result = tasks.get_validation_statistics(geom)

    expected = pd.DataFrame({"driver": [1.0, 3.0], "area_ha": [2.0, 2.0]})
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)
