import datetime

import pyarrow as pa
import pytest

from pipelines.viirs_fire_alerts.alerts import (
    FeedGapError,
    check_continuity,
    drop_repeated_alerts,
)

NOV_3 = datetime.date(2026, 11, 3)


def test_repeated_alerts_are_kept_once_with_their_first_values():
    alerts = pa.table(
        {
            "latitude": [1.0, 1.0, 2.0],
            "longitude": [10.0, 10.0, 10.0],
            "acq_date": [NOV_3, NOV_3, NOV_3],
            "acq_time": ["0005", "0005", "0005"],
            "frp": [5.0, 6.0, 7.0],
        }
    )

    kept = drop_repeated_alerts(alerts).sort_by("latitude")

    assert kept.column("frp").to_pylist() == [5.0, 7.0]


def test_feed_continuing_from_the_day_after_the_stored_alerts_is_accepted():
    check_continuity(
        latest_stored=NOV_3, earliest_in_feed=NOV_3 + datetime.timedelta(1)
    )


def test_first_feed_is_accepted():
    check_continuity(latest_stored=None, earliest_in_feed=NOV_3)


def test_feed_leaving_a_missing_day_is_rejected():
    with pytest.raises(FeedGapError):
        check_continuity(
            latest_stored=NOV_3, earliest_in_feed=NOV_3 + datetime.timedelta(2)
        )
