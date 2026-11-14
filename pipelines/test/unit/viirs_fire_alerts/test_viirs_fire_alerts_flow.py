import pytest
from prefect.logging import disable_run_logger
from pyiceberg.catalog.sql import SqlCatalog

from pipelines.viirs_fire_alerts.alerts import FeedGapError
from pipelines.viirs_fire_alerts.prefect_flows import viirs_fire_alerts_flow

HEADER = (
    "latitude,longitude,bright_ti4,scan,track,acq_date,acq_time,satellite,"
    "confidence,version,bright_ti5,frp,daynight\n"
)
FEED = HEADER + (
    "19.39964,-155.28406,346.91,0.43,0.46,2026-11-03,0005,N20,nominal,2.0NRT,"
    "304.81,74.87,D\n"
    "-8.1,-60.2,330.1,0.4,0.4,2026-11-04,1730,N20,high,2.0NRT,295.0,10.0,D\n"
)
TABLE = "alerts.viirs_fire_alerts"


@pytest.fixture
def catalog(tmp_path, monkeypatch) -> SqlCatalog:
    catalog = SqlCatalog(
        "test",
        uri=f"sqlite:///{tmp_path}/catalog.db",
        warehouse=f"file://{tmp_path}/warehouse",
    )
    catalog.create_namespace("alerts")
    monkeypatch.setattr(viirs_fire_alerts_flow, "load_catalog", lambda name: catalog)
    return catalog


def serve_feed(monkeypatch, content: str):
    monkeypatch.setattr(
        viirs_fire_alerts_flow, "download_feed", lambda url: content.encode()
    )


def run_flow():
    with disable_run_logger():
        return viirs_fire_alerts_flow.viirs_fire_alerts_flow.fn(
            feed_url="https://example.com/feed.csv",
            catalog_name="test",
            table_name=TABLE,
        )


def test_flow_appends_new_alerts_then_nothing_on_the_same_feed(catalog, monkeypatch):
    serve_feed(monkeypatch, FEED)

    first = run_flow()
    snapshot = catalog.load_table(TABLE).current_snapshot().snapshot_id
    second = run_flow()

    assert (first, second) == (2, 0)
    assert catalog.load_table(TABLE).current_snapshot().snapshot_id == snapshot
    assert catalog.load_table(TABLE).scan().to_arrow().num_rows == 2


def test_malformed_feed_fails_and_leaves_the_table_unchanged(catalog, monkeypatch):
    serve_feed(monkeypatch, FEED)
    run_flow()
    snapshot = catalog.load_table(TABLE).current_snapshot().snapshot_id
    serve_feed(monkeypatch, "latitude,longitude\n1.0,2.0\n")

    with pytest.raises(Exception):
        run_flow()

    assert catalog.load_table(TABLE).current_snapshot().snapshot_id == snapshot


def test_feed_leaving_missing_days_fails_and_leaves_the_table_unchanged(
    catalog, monkeypatch
):
    serve_feed(monkeypatch, FEED)
    run_flow()
    snapshot = catalog.load_table(TABLE).current_snapshot().snapshot_id
    serve_feed(
        monkeypatch,
        HEADER + "1.0,2.0,330.1,0.4,0.4,2026-11-20,1730,N20,low,2.0NRT,295.0,10.0,N\n",
    )

    with pytest.raises(FeedGapError):
        run_flow()

    assert catalog.load_table(TABLE).current_snapshot().snapshot_id == snapshot
