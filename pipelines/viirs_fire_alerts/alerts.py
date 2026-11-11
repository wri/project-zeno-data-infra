"""Fire alert points. Alerts have no ids, so an alert is identified by its
point and acquisition time.

A feed only holds recent days, so one starting after the day following the
newest stored alert means days of alerts are missing and must be backfilled
from an archive.
"""

from datetime import date, timedelta
from typing import List, Optional

import pyarrow as pa


class FeedGapError(Exception):
    pass


def alert_key_columns() -> List[str]:
    return ["latitude", "longitude", "acq_date", "acq_time"]


def drop_repeated_alerts(alerts: pa.Table) -> pa.Table:
    """The alerts with only the first row of each key kept."""
    numbered = alerts.append_column("row", pa.array(range(alerts.num_rows)))
    firsts = numbered.group_by(alert_key_columns(), use_threads=False).aggregate(
        [("row", "min")]
    )
    return alerts.take(firsts.column("row_min"))


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
