import datetime

import pytest

from pipelines.viirs_fire_alerts.feed_continuity import FeedGapError, check_continuity

NOV_3 = datetime.date(2026, 11, 3)


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
