import datetime
import os

import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pytest
from prefect.logging import disable_run_logger
from pyiceberg.catalog.sql import SqlCatalog
from shapely.geometry import box

from pipelines.viirs_fire_alerts.iceberg_alert_store import IcebergAlertStore
from pipelines.viirs_fire_alerts.prefect_flows import viirs_admin_counts_flow as flow

TABLE = "alerts.viirs_fire_alerts"
TASKS = ("read_alerts", "read_admin_units", "count_alerts", "write_counts")


@pytest.fixture
def catalog(tmp_path, monkeypatch) -> SqlCatalog:
    catalog = SqlCatalog(
        "test",
        uri=f"sqlite:///{tmp_path}/catalog.db",
        warehouse=f"file://{tmp_path}/warehouse",
    )
    catalog.create_namespace("alerts")
    monkeypatch.setattr(flow, "load_catalog", lambda name: catalog)
    return catalog


@pytest.fixture
def outputs(tmp_path, monkeypatch) -> dict:
    """Run the tasks as plain functions, write results under tmp_path, and
    record the versions marked latest instead of writing to S3."""
    for name in TASKS:
        monkeypatch.setattr(flow, name, getattr(flow, name).fn)
    latest: list[str] = []
    monkeypatch.setattr(
        flow, "write_version", lambda path, version: latest.append(version)
    )
    monkeypatch.setattr(
        flow, "result_uri", lambda version: str(tmp_path / version / flow.RESULT_NAME)
    )
    monkeypatch.setattr(flow, "s3_uri_exists", os.path.exists)
    (tmp_path / "v20261120").mkdir()
    return {"latest": latest, "dir": tmp_path}


@pytest.fixture
def gadm_uri(tmp_path) -> str:
    uri = str(tmp_path / "aois.parquet")
    gpd.GeoDataFrame(
        {"aoi_id": ["AAA", "AAA.1", "BBB"], "adm_level": [0, 1, 0]},
        geometry=[box(0, 0, 2, 2), box(0, 0, 2, 2), box(10, 0, 11, 1)],
        crs=4326,
    ).to_parquet(uri, write_covering_bbox=True)
    return uri


def append_alerts(catalog, *rows) -> None:
    """Append alerts from (longitude, latitude, day of November 2026) rows."""
    IcebergAlertStore(catalog, TABLE, cell_level=13).append_new(
        pa.table(
            {
                "latitude": [float(r[1]) for r in rows],
                "longitude": [float(r[0]) for r in rows],
                "acq_date": [datetime.date(2026, 11, r[2]) for r in rows],
                "acq_time": [f"{i:04d}" for i in range(len(rows))],
                "confidence": ["h"] * len(rows),
                "bright_ti4": [300.0] * len(rows),
                "bright_ti5": [290.0] * len(rows),
                "frp": [5.0] * len(rows),
            }
        )
    )


def run_flow(gadm_uri, overwrite=False):
    with disable_run_logger():
        return flow.viirs_admin_counts_flow.fn(
            catalog_name="test",
            table_name=TABLE,
            cell_level=13,
            overwrite=overwrite,
            gadm_aois_uri=gadm_uri,
        )


def test_counts_are_written_as_the_newest_alert_date_version(
    catalog, outputs, gadm_uri
):
    append_alerts(catalog, (0.5, 0.5, 19), (1.5, 1.5, 20), (10.5, 0.5, 20))

    uri = run_flow(gadm_uri)

    assert uri == str(outputs["dir"] / "v20261120" / flow.RESULT_NAME)
    assert outputs["latest"] == ["v20261120"]
    counts = pd.read_parquet(uri)
    assert counts.groupby("aoi_id").alert_count.sum().to_dict() == {
        "AAA": 2,
        "AAA.1": 2,
        "BBB": 1,
    }


def test_an_existing_version_is_only_marked_latest(catalog, outputs, gadm_uri):
    append_alerts(catalog, (0.5, 0.5, 20))
    uri = run_flow(gadm_uri)
    pd.DataFrame({"marker": [1]}).to_parquet(uri)

    assert run_flow(gadm_uri) == uri

    assert list(pd.read_parquet(uri).columns) == ["marker"]
    assert outputs["latest"] == ["v20261120", "v20261120"]


def test_overwrite_recomputes_an_existing_version(catalog, outputs, gadm_uri):
    append_alerts(catalog, (0.5, 0.5, 20))
    uri = run_flow(gadm_uri)
    pd.DataFrame({"marker": [1]}).to_parquet(uri)

    run_flow(gadm_uri, overwrite=True)

    assert "alert_count" in pd.read_parquet(uri).columns


def test_an_empty_table_writes_nothing(catalog, outputs, gadm_uri):
    assert run_flow(gadm_uri) is None
    assert outputs["latest"] == []
