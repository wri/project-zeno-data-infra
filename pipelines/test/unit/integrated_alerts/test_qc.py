from datetime import date

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

from pipelines.integrated_alerts.qc import qc_against_gee

RUN_DATE = date(2026, 6, 30)  # 8-month cutoff -> 2025-10-30
OLD = pd.Timestamp("2024-06-01")  # in window [2023-01-01, cutoff] -> compared
RECENT = pd.Timestamp("2026-05-01")  # within 8 months -> excluded


class FakeQCFeatures:
    """Stand-in for QCFeaturesRepository with one tropical AOI."""

    def __init__(self, aoi_id="BRA.1"):
        self.aoi_id = aoi_id
        self.written = None

    def load_tropical(self, lat_limit=30.0, limit=None, start=0):
        return gpd.GeoDataFrame(
            {"aoi_id": [self.aoi_id], "GID_2": [f"{self.aoi_id}_1"]},
            geometry=[Point(0, 0)],
            crs="EPSG:4326",
        )

    def write_results(self, results, analysis, version):
        self.written = results


def gee_returning(df):
    return lambda geom: df.copy()


def run(result_df, gee_df, repo=None):
    repo = repo or FakeQCFeatures()
    return qc_against_gee(
        result_df,
        qc_feature_repository=repo,
        gee_area_fn=gee_returning(gee_df),
        run_date=RUN_DATE,
    )


def test_passes_when_areas_agree():
    result_df = pd.DataFrame(
        {
            "aoi_id": ["BRA.1", "BRA.1"],
            "alert_date": [OLD, OLD],
            "alert_confidence": ["high", "low"],
            "area_ha": [1000.0, 5.0],
        }
    )
    gee_df = pd.DataFrame(
        {
            "alert_date": [OLD, OLD],
            "alert_confidence": ["high", "low"],
            "area_ha": [1000.0, 5.0],
        }
    )
    assert run(result_df, gee_df) is True


def test_fails_on_large_relative_and_absolute_diff():
    result_df = pd.DataFrame(
        {
            "aoi_id": ["BRA.1"],
            "alert_date": [OLD],
            "alert_confidence": ["high"],
            "area_ha": [1000.0],
        }
    )
    gee_df = pd.DataFrame(
        {"alert_date": [OLD], "alert_confidence": ["high"], "area_ha": [2000.0]}
    )  # 67% rel, 1000 ha abs -> flagged
    assert run(result_df, gee_df) is False


def test_tiny_absolute_diff_not_flagged():
    result_df = pd.DataFrame(
        {
            "aoi_id": ["BRA.1"],
            "alert_date": [OLD],
            "alert_confidence": ["low"],
            "area_ha": [2.0],
        }
    )
    gee_df = pd.DataFrame(
        {"alert_date": [OLD], "alert_confidence": ["low"], "area_ha": [14.0]}
    )  # 150% rel but only 12 ha abs (< 50) -> not flagged
    assert run(result_df, gee_df) is True


def test_recent_dates_excluded_from_comparison():
    # our run lacks a huge alert that GEE has, but it's within 8 months -> excluded
    result_df = pd.DataFrame(
        {
            "aoi_id": ["BRA.1"],
            "alert_date": [OLD],
            "alert_confidence": ["high"],
            "area_ha": [1000.0],
        }
    )
    gee_df = pd.DataFrame(
        {
            "alert_date": [OLD, RECENT],
            "alert_confidence": ["high", "high"],
            "area_ha": [1000.0, 999999.0],
        }
    )
    assert run(result_df, gee_df) is True


def test_writes_results_when_version_given():
    repo = FakeQCFeatures()
    result_df = pd.DataFrame(
        {
            "aoi_id": ["BRA.1"],
            "alert_date": [OLD],
            "alert_confidence": ["high"],
            "area_ha": [1000.0],
        }
    )
    gee_df = pd.DataFrame(
        {"alert_date": [OLD], "alert_confidence": ["high"], "area_ha": [1000.0]}
    )
    qc_against_gee(
        result_df,
        version="test_v1",
        qc_feature_repository=repo,
        gee_area_fn=gee_returning(gee_df),
        run_date=RUN_DATE,
    )
    assert repo.written is not None
    assert bool(repo.written["qc_pass"].iloc[0]) is True
