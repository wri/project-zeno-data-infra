from unittest.mock import patch

import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr
from shapely.geometry import box

from pipelines.tree_cover_loss.stages import TreeCoverLossTasks


def test_tcl_validation_flow():
    with patch(
        "pipelines.tree_cover_loss.stages.TreeCoverLossTasks.get_sample_statistics"
    ) as mock_sample, patch(
        "pipelines.tree_cover_loss.stages.TreeCoverLossTasks.get_validation_statistics"
    ) as mock_validation:

        class FakeQCRepository:
            def load(self, aoi_id=None, aoi_type=None):
                return gpd.GeoDataFrame(geometry=[box(0, 0, 1, 1)])

        tasks = TreeCoverLossTasks(qc_feature_repository=FakeQCRepository())
        mock_sample.return_value = pd.DataFrame({"area_ha": [100.0, 200.0]})
        mock_validation.return_value = pd.DataFrame({"area_ha": [100.0, 200.0]})

        assert bool(tasks.qc_against_validation_source()) is True

        mock_validation.return_value = pd.DataFrame({"area_ha": [100.0, 150.0]})
        assert bool(tasks.qc_against_validation_source()) is False


def test_get_sample_statistics_accepts_injected_geometry_lookup():
    geom = box(0, 0, 1, 1)
    expected = pd.DataFrame({"area_ha": [123.0]})

    with patch("pipelines.tree_cover_loss.stages.umd_tree_cover_loss") as mock_flow:
        mock_flow.return_value = expected

        result = TreeCoverLossTasks.get_sample_statistics(geom)

        assert result is expected

        mock_flow.assert_called_once()
        _, kwargs = mock_flow.call_args
        assert kwargs["bbox"] is geom.bounds


def test_get_validation_statistics_with_fake_repo():
    class FakeGoogleEarthEngineDatasetRepository:
        def load(self, dataset, geometry, like=None):
            if dataset == "loss":
                return xr.Dataset(
                    data_vars={
                        "loss": (
                            ("y", "x"),
                            np.array([[1, 0], [1, 1]], dtype=np.uint8),
                        ),
                        "treecover2000": (
                            ("y", "x"),
                            np.array([[40, 20], [50, 31]], dtype=np.uint8),
                        ),
                    }
                )
            if dataset == "tcl_drivers":
                return xr.Dataset(
                    data_vars={
                        "classification": (
                            ("y", "x"),
                            np.array([[1, 2], [1, 3]], dtype=np.uint8),
                        )
                    }
                )
            if dataset == "area":
                return xr.DataArray(
                    np.array([[10000, 10000], [10000, 20000]], dtype=np.float32),
                    dims=("y", "x"),
                    name="area",
                )
            raise ValueError(f"Unknown dataset: {dataset}")

    geom = box(0, 0, 1, 1)
    tasks = TreeCoverLossTasks(gee_repository=FakeGoogleEarthEngineDatasetRepository())

    result = tasks.get_validation_statistics(geom)

    expected = pd.DataFrame({"driver": [1.0, 3.0], "area_ha": [2.0, 2.0]})
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_get_statistics_with_fake_repo():
    class FakeGoogleEarthEngineDatasetRepository:
        def load(self, dataset, geometry, like=None):
            if dataset == "loss":
                return xr.Dataset(
                    data_vars={
                        "loss": (
                            ("y", "x"),
                            np.array([[1, 0], [1, 1]], dtype=np.uint8),
                        ),
                        "treecover2000": (
                            ("y", "x"),
                            np.array([[40, 20], [50, 31]], dtype=np.uint8),
                        ),
                    }
                )
            if dataset == "tcl_drivers":
                return xr.Dataset(
                    data_vars={
                        "classification": (
                            ("y", "x"),
                            np.array([[1, 2], [1, 3]], dtype=np.uint8),
                        )
                    }
                )
            if dataset == "area":
                return xr.DataArray(
                    np.array([[10000, 10000], [10000, 20000]], dtype=np.float32),
                    dims=("y", "x"),
                    name="area",
                )
            raise ValueError(f"Unknown dataset: {dataset}")

    geom = box(0, 0, 1, 1)
    tasks = TreeCoverLossTasks(gee_repository=FakeGoogleEarthEngineDatasetRepository())

    result = tasks.get_validation_statistics(geom)

    expected = pd.DataFrame({"driver": [1.0, 3.0], "area_ha": [2.0, 2.0]})
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)
