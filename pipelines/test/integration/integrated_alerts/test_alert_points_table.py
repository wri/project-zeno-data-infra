import numpy as np
import pyarrow.parquet as pq
import pytest
import xarray as xr
from shapely.geometry import box

from pipelines.globals import sbtn_natural_lands_zarr_uri
from pipelines.integrated_alerts.alert_points_table import (
    TABLE_SCHEMA,
    build_alert_points_table,
    latest_alert_day,
)
from pipelines.integrated_alerts.stages import resample_to_alerts_grid
from pipelines.prefect_flows.common_stages import clip_ds_to_bbox

# A published release, and a small area in Pará straddling the corner of four
# alerts tiles (tile edges fall on whole degrees).
VERSION = "v20261018"
ALERTS_URI = (
    f"s3://lcl-analytics/zarr/gfw_integrated_dist_alerts/{VERSION}/date_conf.zarr"
)
ADMIN_URI = (
    f"s3://lcl-analytics/zonal-statistics/integrated-alerts/{VERSION}/"
    "admin-integrated-alerts.parquet"
)
BBOX = box(-52.02, -3.02, -51.98, -2.98)
DAYS_STORED = 30


def _open(uri):
    return xr.open_zarr(uri).squeeze("band", drop=True)


@pytest.mark.slow
@pytest.mark.integration
def test_bbox_table_matches_a_raster_scan(tmp_path):
    result_uri = str(tmp_path / "by-date.parquet")

    build_alert_points_table(
        ALERTS_URI, ADMIN_URI, VERSION, result_uri, DAYS_STORED, BBOX
    )

    table = pq.read_table(result_uri).to_pandas()
    latest_day = latest_alert_day(ADMIN_URI)
    alerts = clip_ds_to_bbox(_open(ALERTS_URI), BBOX).load()
    natural_lands = _open(sbtn_natural_lands_zarr_uri)["band_data"]
    classes = resample_to_alerts_grid(natural_lands, alerts).values
    alert_date, confidence = alerts.alert_date.values, alerts.confidence.values
    rows, cols = np.nonzero(
        (alert_date > latest_day - DAYS_STORED)
        & (alert_date <= latest_day)
        & np.isin(confidence, [2, 3, 4])
    )
    row_offset = int(np.searchsorted(-_open(ALERTS_URI).y.values, -alerts.y.values[0]))
    col_offset = int(np.searchsorted(_open(ALERTS_URI).x.values, alerts.x.values[0]))
    expected = {
        (
            int(row_offset + r),
            int(col_offset + c),
            int(alert_date[r, c]),
            int(confidence[r, c]),
            int(classes[r, c]),
        )
        for r, c in zip(rows, cols)
    }

    assert len(expected) > 0
    assert set(table[TABLE_SCHEMA.names].itertuples(index=False, name=None)) == expected
    assert len(table) == len(expected)
