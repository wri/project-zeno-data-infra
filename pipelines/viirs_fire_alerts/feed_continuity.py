"""A feed only holds recent days, so a feed starting after the day following
the newest stored alert means days of alerts are missing and must be backfilled
from an archive."""

from datetime import date, timedelta
from typing import Optional


class FeedGapError(Exception):
    pass


def check_continuity(latest_stored: Optional[date], earliest_in_feed: date) -> None:
    """Raise FeedGapError if days are missing between the stored alerts and the
    feed. Any feed is accepted when nothing is stored yet."""
    if latest_stored is None:
        return
    if earliest_in_feed > latest_stored + timedelta(days=1):
        raise FeedGapError(
            f"No alerts between {latest_stored} (newest stored) and "
            f"{earliest_in_feed} (earliest in feed); backfill them first"
        )
