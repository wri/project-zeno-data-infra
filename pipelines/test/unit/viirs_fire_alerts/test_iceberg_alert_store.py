import datetime

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import CommitFailedException

from pipelines.viirs_fire_alerts.iceberg_alert_store import IcebergAlertStore

TABLE = "alerts.viirs_fire_alerts"


@pytest.fixture
def catalog(tmp_path) -> SqlCatalog:
    catalog = SqlCatalog(
        "test",
        uri=f"sqlite:///{tmp_path}/catalog.db",
        warehouse=f"file://{tmp_path}/warehouse",
    )
    catalog.create_namespace("alerts")
    return catalog


@pytest.fixture
def store(catalog) -> IcebergAlertStore:
    return IcebergAlertStore(catalog, TABLE)


class RacingCatalog:
    """A catalog where another writer appends alerts just after each table is
    loaded, so the loaded table is already out of date."""

    def __init__(self, catalog: SqlCatalog, other_alerts: pa.Table):
        self.catalog = catalog
        self.other_alerts = other_alerts

    def create_table_if_not_exists(self, *args, **kwargs):
        table = self.catalog.create_table_if_not_exists(*args, **kwargs)
        IcebergAlertStore(self.catalog, TABLE).append_unseen(self.other_alerts)
        return table


def alerts(*keys) -> pa.Table:
    """Alert rows for (latitude, day of November 2026, acq_time) keys."""
    return pa.table(
        {
            "latitude": [float(key[0]) for key in keys],
            "longitude": [10.0] * len(keys),
            "acq_date": [datetime.date(2026, 11, key[1]) for key in keys],
            "acq_time": [key[2] for key in keys],
            "confidence": ["n"] * len(keys),
            "bright_ti4": [300.0] * len(keys),
            "bright_ti5": [290.0] * len(keys),
            "frp": [5.0] * len(keys),
            "s2_cell": [1] * len(keys),
            "s2_cell_fine": [2] * len(keys),
        }
    )


def stored_keys(store: IcebergAlertStore):
    rows = (
        store.table()
        .scan()
        .to_arrow()
        .sort_by([("latitude", "ascending"), ("acq_time", "ascending")])
    )
    return list(
        zip(
            rows.column("latitude").to_pylist(),
            [day.day for day in rows.column("acq_date").to_pylist()],
            rows.column("acq_time").to_pylist(),
        )
    )


def test_table_is_partitioned_by_month_and_coarse_cell(store):
    fields = [field.name for field in store.table().spec().fields]

    assert fields == ["acq_date_month", "s2_cell"]


def test_same_feed_twice_stores_each_alert_once(store):
    feed = alerts((1, 3, "0005"), (2, 4, "1210"))

    first = store.append_unseen(feed)
    second = store.append_unseen(feed)

    assert (first, second) == (2, 0)
    assert stored_keys(store) == [(1.0, 3, "0005"), (2.0, 4, "1210")]


def test_overlapping_feed_appends_only_unseen_alerts(store):
    store.append_unseen(alerts((1, 3, "0005"), (2, 4, "1210")))
    # same place and day as a stored alert, but a later overpass
    newer = alerts((2, 4, "1210"), (2, 4, "1352"), (3, 9, "0130"))

    store.append_unseen(newer)

    assert stored_keys(store) == [
        (1.0, 3, "0005"),
        (2.0, 4, "1210"),
        (2.0, 4, "1352"),
        (3.0, 9, "0130"),
    ]


def test_repeated_alerts_within_a_feed_are_stored_once(store):
    store.append_unseen(alerts((1, 3, "0005"), (1, 3, "0005")))

    assert stored_keys(store) == [(1.0, 3, "0005")]


def test_appending_nothing_adds_no_snapshot(store):
    store.append_unseen(alerts((1, 3, "0005")))
    snapshot = store.table().current_snapshot().snapshot_id

    store.append_unseen(alerts((1, 3, "0005")))

    assert store.table().current_snapshot().snapshot_id == snapshot


def test_write_fails_when_another_writer_commits_after_the_read(catalog, store):
    racing = IcebergAlertStore(RacingCatalog(catalog, alerts((1, 3, "0005"))), TABLE)

    with pytest.raises(CommitFailedException):
        racing.append_unseen(alerts((1, 3, "0005")))

    assert stored_keys(store) == [(1.0, 3, "0005")]


def test_latest_date_is_the_newest_stored_alert_date(store):
    assert store.latest_date() is None

    store.append_unseen(alerts((1, 3, "0005"), (2, 9, "1210")))

    assert store.latest_date() == datetime.date(2026, 11, 9)
