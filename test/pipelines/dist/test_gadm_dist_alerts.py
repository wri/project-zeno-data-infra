import pytest
import xarray as xr
import numpy as np
import pandas as pd

from pipelines.dist.gadm_dist_alerts import gadm_dist_alerts

@pytest.fixture
def expected_groups():
    return (
        # Match expected groups to minimal data values
        [12],        # Country values in minimal data
        [7],        # Region values
        [124],        # Subregion values
        [731, 750, 800],  # Alert date values in minimal data
        [2, 3]        # Confidence values in minimal data
    )

@pytest.fixture
def mock_loader():
    """In-memory dataset loader (replaces Zarr I/O)"""
    def _loader(dist_zarr_uri: str):
        # SHARED COORDINATES FOR ALL DATASETS
        band = [1]
        y_vals = [60.0, 59.99975]  # <-- Use same y for everything
        x_vals = [-180.0, -179.99975]  # <-- Use same x for everything
        crs_attrs = {
            'crs_wkt': 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AXIS["Latitude",NORTH],AXIS["Longitude",EAST],AUTHORITY["EPSG","4326"]]',
            'semi_major_axis': 6378137.0,
            'semi_minor_axis': 6356752.314245179,
            'inverse_flattening': 298.257223563,
            'reference_ellipsoid_name': 'WGS 84',
            'longitude_of_prime_meridian': 0.0,
            'prime_meridian_name': 'Greenwich',
            'geographic_crs_name': 'WGS 84',
            'horizontal_datum_name': 'World Geodetic System 1984',
            'grid_mapping_name': 'latitude_longitude',
            'spatial_ref': 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AXIS["Latitude",NORTH],AXIS["Longitude",EAST],AUTHORITY["EPSG","4326"]]',
            'GeoTransform': '-180.0 0.00025 0.0 60.0 0.0 -0.00025'  # Consistent with coords
        }

        # 1. dist_alerts dataset (unchanged)
        confidence_data = np.array([[[3, 2], [2, 3]]], dtype=np.int16)
        alert_date_data = np.array([[[750, 731], [731, 800]]], dtype=np.int16)
        dist_alerts = xr.Dataset(
            data_vars={
                "confidence": (("band", "y", "x"), confidence_data),
                "alert_date": (("band", "y", "x"), alert_date_data)
            },
            coords={
                "band": ("band", band, {}),
                "y": ("y", y_vals, {}),
                "x": ("x", x_vals, {}),
                "spatial_ref": ((), 0, crs_attrs)
            },
            attrs={}
        )

        # 2. country/region/subregion datasets (USE SAME COORDS)
        country = xr.Dataset(
            data_vars={
                "band_data": (
                    ("band", "y", "x"),
                    np.array([[[12, 12], [12, 12]]], dtype=np.uint16),
                    {
                        'AREA_OR_POINT': 'Area',
                        'STATISTICS_MAXIMUM': 12,
                        'STATISTICS_MEAN': 12,
                        'STATISTICS_MINIMUM': 12,
                        'STATISTICS_STDDEV': 0,
                        'STATISTICS_VALID_PERCENT': 0.0116
                    }
                )
            },
        )

        region = xr.Dataset(
            data_vars={
                "band_data": (
                    ("band", "y", "x"),
                    np.array([[[7, 7], [7, 7]]], dtype=np.uint16),
                    {
                        'AREA_OR_POINT': 'Area',
                        'STATISTICS_MAXIMUM': 7,
                        'STATISTICS_MEAN': 7,
                        'STATISTICS_MINIMUM': 7,
                        'STATISTICS_STDDEV': 0,
                        'STATISTICS_VALID_PERCENT': 0.0116
                    }
                )
            },
        )

        subregion = xr.Dataset(
            data_vars={
                "band_data": (
                    ("band", "y", "x"),
                    np.array([[[124, 124], [124, 124]]], dtype=np.uint16),
                    {
                        'AREA_OR_POINT': 'Area',
                        'STATISTICS_MAXIMUM': 124,
                        'STATISTICS_MEAN': 124,
                        'STATISTICS_MINIMUM': 124,
                        'STATISTICS_STDDEV': 0,
                        'STATISTICS_VALID_PERCENT': 0.0116
                    }
                )
            },
        )

        return dist_alerts, country, region, subregion
    return _loader

@pytest.fixture
def mock_saver():
    def _saver(df: pd.DataFrame, uri: str):
        _saver.saved_data = df
        _saver.results_uri = uri
    return _saver


def test_gadm_dist_alerts_happy_path(mock_loader, expected_groups, mock_saver):
    """Test full workflow with in-memory dependencies"""
    result_uri = gadm_dist_alerts(
        dist_zarr_uri="s3://dummy_zarr_uri",
        dist_version="test_v1",
        loader=mock_loader,
        groups=expected_groups,
        saver=mock_saver,
    )

    # Verify output
    print(f"\nDIST Alerts Result:\n{mock_saver.saved_data}")
    print(f"Result saved to: {mock_saver.results_uri}")
    assert result_uri == "s3://gfw-data-lake/umd_glad_dist_alerts/test_v1/tabular/epsg-4326/zonal_stats/dist_alerts_by_adm2_raw_test.parquet"
