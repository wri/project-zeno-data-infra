import pandas as pd
import pytest
import numpy as np
import xarray as xr


@pytest.fixture
def expected_groups():
    return (
        # Match expected groups to minimal data values
        [12],  # Country values in minimal data
        [7],  # Region values
        [124, 125],  # Subregion values
        [731, 750, 800],  # Alert date values in minimal data
        [2, 3],  # Confidence values in minimal data
    )


@pytest.fixture
def mock_loader():
    def _loader(dist_zarr_uri: str):
        # 1. dist_alerts dataset
        confidence_data = np.array([[[3, 2], [2, 3]]], dtype=np.int16)
        alert_date_data = np.array([[[750, 731], [731, 800]]], dtype=np.int16)
        dist_alerts = xr.Dataset(
            data_vars={
                "confidence": (("band", "y", "x"), confidence_data),
                "alert_date": (("band", "y", "x"), alert_date_data),
            },
            coords={
                "band": ("band", [1], {}),
                "y": ("y", [60.0, 59.99975], {}),
                "x": ("x", [-180.0, -179.99975], {}),
                "spatial_ref": ((), 0, {}),
            },
            attrs={},
        )

        # 2. GADM datasets
        country = xr.Dataset(
            data_vars={
                "band_data": (
                    ("band", "y", "x"),
                    np.array([[[12, 12], [12, 12]]], dtype=np.uint16),
                )
            },
        )

        region = xr.Dataset(
            data_vars={
                "band_data": (
                    ("band", "y", "x"),
                    np.array([[[7, 7], [7, 7]]], dtype=np.uint16),
                )
            },
        )

        subregion = xr.Dataset(
            data_vars={
                "band_data": (
                    ("band", "y", "x"),
                    np.array([[[124, 124], [124, 125]]], dtype=np.uint16),
                )
            },
        )

        return dist_alerts, country, region, subregion

    return _loader


@pytest.fixture
def spy_saver():
    def _saver(df: pd.DataFrame, uri: str):
        """Capture the DataFrame passed in to be later used in assertions"""
        _saver.saved_data = df

    return _saver
