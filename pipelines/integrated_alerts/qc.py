"""QC for the integrated-alerts admin precompute.

Compares this run's flox zonal-stats result (area by aoi_id / confidence / date)
against the GFW integrated-DIST GEE asset (tropics only) for a sample of tropical
admin-2 AOIs. Only alerts in the window ``[START_DATE, run_date - MONTHS_BACK]``
are compared:

- ``START_DATE`` matches the precompute's ``expected_groups`` date floor, so the
  pre-2023 history the GEE asset carries (but the precompute deliberately drops)
  is not counted as a difference.
- the ``MONTHS_BACK`` cutoff excludes recent alerts, which can still be ingested
  or change confidence in either source.

A confidence group is flagged only when it differs by BOTH more than
``REL_THRESHOLD`` and more than ``ABS_THRESHOLD_HA`` (small areas are noisy at 10 m).
"""

from datetime import date, timedelta

import ee
import pandas as pd
from dateutil.relativedelta import relativedelta
from shapely.geometry import mapping

from pipelines.prefect_flows.common_stages import symmetric_relative_difference
from pipelines.repositories.google_earth_engine_dataset_repository import (
    GoogleEarthEngineDatasetRepository,
)
from pipelines.repositories.qc_feature_repository import QCFeaturesRepository

ASSET_KEY = "integrated_alerts"
EPOCH = date(2014, 12, 31)  # packed days are days since 2014-12-31
CONFIDENCE_BY_CODE = {2: "low", 3: "high", 4: "highest"}
CONFIDENCE_LEVELS = ("low", "high", "highest")
TROPICS_LAT = 30.0
SCALE_M = 10
START_DATE = date(2023, 1, 1)  # matches the precompute's expected_groups floor
MONTHS_BACK = 8
REL_THRESHOLD = 0.05
ABS_THRESHOLD_HA = 50.0


def gee_area_by_confidence_date(geom, gee_repository=None, scale=SCALE_M):
    """Area (ha) by confidence and date for one AOI from the GEE asset.

    ``pixelArea()`` is summed server-side, grouped by the packed
    ``confidence * 10000 + days_since_2014-12-31`` value, then decoded.
    """
    gee_repository = gee_repository or GoogleEarthEngineDatasetRepository()
    gee_repository.initialize()

    packed = gee_repository.EE_ASSETS[ASSET_KEY]().select(0).toInt().rename("packed")
    image = ee.Image.pixelArea().addBands(packed)
    grouped = image.reduceRegion(
        reducer=ee.Reducer.sum().group(groupField=1, groupName="packed"),
        geometry=ee.Geometry(mapping(geom)),
        scale=scale,
        maxPixels=int(1e13),
        tileScale=4,
    )

    rows = []
    for group in grouped.get("groups").getInfo() or []:
        confidence_code, days = divmod(int(group["packed"]), 10000)
        if confidence_code not in CONFIDENCE_BY_CODE:
            continue
        rows.append(
            {
                "alert_date": EPOCH + timedelta(days=int(days)),
                "alert_confidence": CONFIDENCE_BY_CODE[confidence_code],
                "area_ha": group["sum"] / 10000.0,
            }
        )
    return pd.DataFrame(rows, columns=["alert_date", "alert_confidence", "area_ha"])


def _within_window(df, start, cutoff):
    """Keep rows whose alert_date is in [start, cutoff] (as datetime.date)."""
    if df.empty:
        return df
    df = df.copy()
    df["alert_date"] = pd.to_datetime(df["alert_date"]).dt.date
    return df[(df["alert_date"] >= start) & (df["alert_date"] <= cutoff)]


def qc_against_gee(
    result_df,
    version=None,
    qc_feature_repository=None,
    gee_area_fn=None,
    run_date=None,
    start_date=START_DATE,
    months_back=MONTHS_BACK,
    rel_threshold=REL_THRESHOLD,
    abs_threshold_ha=ABS_THRESHOLD_HA,
):
    """Return True if this run's ``result_df`` matches the GEE asset for every
    sampled tropical AOI / confidence level (within the comparison window)."""
    qc_feature_repository = qc_feature_repository or QCFeaturesRepository()
    gee_area_fn = gee_area_fn or gee_area_by_confidence_date
    run_date = run_date or date.today()
    cutoff = run_date - relativedelta(months=months_back)

    features = qc_feature_repository.load_tropical(
        lat_limit=TROPICS_LAT, limit=20
    ).copy()

    sample = _within_window(result_df, start_date, cutoff)
    sample_rollup = sample.groupby(["aoi_id", "alert_confidence"])["area_ha"].sum()

    def qc_feature(row):
        gee_rollup = (
            _within_window(gee_area_fn(row.geometry), start_date, cutoff)
            .groupby("alert_confidence")["area_ha"]
            .sum()
        )
        metrics = {}
        passed = True
        for confidence in CONFIDENCE_LEVELS:
            sample_ha = float(sample_rollup.get((row.aoi_id, confidence), 0.0))
            gee_ha = float(gee_rollup.get(confidence, 0.0))
            rel = symmetric_relative_difference(gee_ha, sample_ha)
            abs_diff = abs(gee_ha - sample_ha)
            flagged = rel > rel_threshold and abs_diff > abs_threshold_ha
            passed = passed and not flagged
            metrics[f"sample_{confidence}_ha"] = sample_ha
            metrics[f"gee_{confidence}_ha"] = gee_ha
            metrics[f"reldiff_{confidence}"] = rel
        metrics["qc_pass"] = passed
        return pd.Series(metrics)

    features = features.join(features.apply(qc_feature, axis=1))

    if version is not None:
        qc_feature_repository.write_results(
            features, "admin-integrated-alerts", version
        )

    return bool(features["qc_pass"].all())
