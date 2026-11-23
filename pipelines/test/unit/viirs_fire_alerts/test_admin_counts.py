import datetime

import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from shapely.geometry import box

from pipelines.viirs_fire_alerts import admin_counts
from pipelines.viirs_fire_alerts.iceberg_alert_store import IcebergAlertStore

NOV_20 = datetime.date(2026, 11, 20)


def admin_units() -> gpd.GeoDataFrame:
    """Country AAA with level-2 units under AAA.1 and none under AAA.2, and
    country BBB with no level 1 or 2 units."""
    rows = [
        ("AAA", 0, box(0, 0, 2, 2)),
        ("AAA.1", 1, box(0, 0, 2, 1)),
        ("AAA.1.1", 2, box(0, 0, 1, 1)),
        ("AAA.1.2", 2, box(1, 0, 2, 1)),
        ("AAA.2", 1, box(0, 1, 2, 2)),
        ("BBB", 0, box(10, 0, 11, 1)),
    ]
    return gpd.GeoDataFrame(
        {"aoi_id": [r[0] for r in rows], "adm_level": [r[1] for r in rows]},
        geometry=[r[2] for r in rows],
        crs=4326,
    )


def alerts(*rows) -> pd.DataFrame:
    """Alerts from (longitude, latitude, day of November 2026, confidence) rows."""
    return pd.DataFrame(
        {
            "longitude": [float(r[0]) for r in rows],
            "latitude": [float(r[1]) for r in rows],
            "acq_date": [datetime.date(2026, 11, r[2]) for r in rows],
            "confidence": [r[3] for r in rows],
        }
    ).astype({"confidence": "category"})


def counts_by_area(counts: pd.DataFrame) -> dict[str, int]:
    return counts.groupby("aoi_id").alert_count.sum().to_dict()


def test_finest_units_are_units_without_children():
    finest = admin_counts.select_finest_units(admin_units())

    assert sorted(finest.aoi_id) == ["AAA.1.1", "AAA.1.2", "AAA.2", "BBB"]


def test_counts_roll_up_to_every_level():
    finest = admin_counts.select_finest_units(admin_units())

    counts = admin_counts.admin_alert_counts(
        alerts(
            (0.5, 0.5, 1, "h"),
            (0.6, 0.5, 1, "h"),
            (1.5, 0.5, 1, "n"),
            (0.5, 1.5, 2, "l"),
            (10.5, 0.5, 3, "h"),
        ),
        finest,
    )

    assert counts_by_area(counts) == {
        "AAA": 4,
        "AAA.1": 3,
        "AAA.1.1": 2,
        "AAA.1.2": 1,
        "AAA.2": 1,
        "BBB": 1,
    }


def test_counts_are_split_by_date_and_confidence_with_output_columns():
    finest = admin_counts.select_finest_units(admin_units())

    counts = admin_counts.admin_alert_counts(
        alerts((0.5, 0.5, 1, "h"), (0.5, 0.5, 1, "h"), (0.5, 0.5, 2, "n")),
        finest,
    )
    leaf = counts[counts.aoi_id == "AAA.1.1"]

    assert list(counts.columns) == admin_counts.OUTPUT_COLUMNS
    assert set(counts.aoi_type) == {"admin"}
    assert leaf[["alert_date", "alert_confidence", "alert_count"]].astype(
        {"alert_confidence": str}
    ).to_dict("records") == [
        {
            "alert_date": datetime.date(2026, 11, 1),
            "alert_confidence": "high",
            "alert_count": 2,
        },
        {
            "alert_date": datetime.date(2026, 11, 2),
            "alert_confidence": "nominal",
            "alert_count": 1,
        },
    ]


def test_alerts_outside_every_unit_or_on_a_shared_edge_are_not_counted():
    finest = admin_counts.select_finest_units(admin_units())

    counts = admin_counts.admin_alert_counts(
        alerts((50, 50, 1, "h"), (1.0, 0.5, 1, "h")), finest
    )

    assert counts.empty


def test_counts_are_sorted_by_aoi_id():
    finest = admin_counts.select_finest_units(admin_units())

    counts = admin_counts.admin_alert_counts(
        alerts((10.5, 0.5, 1, "h"), (1.5, 0.5, 2, "n"), (0.5, 0.5, 1, "h")), finest
    )

    assert list(counts.aoi_id) == sorted(counts.aoi_id)


def test_finest_units_are_read_from_geoparquet_and_limited_to_a_bbox(tmp_path):
    uri = str(tmp_path / "aois.parquet")
    units = admin_units()
    deeper = gpd.GeoDataFrame(
        {"aoi_id": ["AAA.1.1.1"], "adm_level": [3]},
        geometry=[box(0, 0, 1, 1)],
        crs=4326,
    )
    pd.concat([units, deeper]).to_parquet(uri, write_covering_bbox=True)

    every = admin_counts.read_finest_admin_units(uri)
    in_box = admin_counts.read_finest_admin_units(uri, bbox=box(9, -1, 12, 2))

    assert sorted(every.aoi_id) == ["AAA.1.1", "AAA.1.2", "AAA.2", "BBB"]
    assert list(in_box.aoi_id) == ["BBB"]


def test_window_start():
    assert admin_counts.window_start(NOV_20) == datetime.date(2025, 11, 21)


@pytest.fixture
def table(tmp_path):
    catalog = SqlCatalog(
        "test",
        uri=f"sqlite:///{tmp_path}/catalog.db",
        warehouse=f"file://{tmp_path}/warehouse",
    )
    catalog.create_namespace("alerts")
    store = IcebergAlertStore(catalog, "alerts.viirs_fire_alerts", cell_level=13)
    days = [
        datetime.date(2025, 11, 20),
        datetime.date(2025, 11, 21),
        datetime.date(2026, 6, 1),
        NOV_20,
    ]
    store.append_new(
        pa.table(
            {
                "latitude": [0.5, 0.5, 0.5, 30.0],
                "longitude": [0.5, 0.5, 0.5, 30.0],
                "acq_date": days,
                "acq_time": ["0100"] * 4,
                "confidence": ["h", "n", "l", "h"],
                "bright_ti4": [300.0] * 4,
                "bright_ti5": [290.0] * 4,
                "frp": [5.0] * 4,
            }
        )
    )
    return store.table()


def test_alerts_are_read_for_the_last_365_days(table):
    read = admin_counts.read_alerts(table, NOV_20)

    assert list(read.columns) == list(admin_counts.ALERT_COLUMNS)
    assert sorted(read.acq_date) == [
        datetime.date(2025, 11, 21),
        datetime.date(2026, 6, 1),
        NOV_20,
    ]


def test_alerts_are_read_only_in_a_bbox(table):
    read = admin_counts.read_alerts(table, NOV_20, bbox=box(29, 29, 31, 31))

    assert list(read.acq_date) == [NOV_20]
