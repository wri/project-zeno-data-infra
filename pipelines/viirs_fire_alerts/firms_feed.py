"""Reads NASA FIRMS active fire CSV files and plans which days to request so
they continue on from the alerts already stored."""

import io
from datetime import date, timedelta
from typing import List, Optional, Tuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as csv


class FeedGapError(Exception):
    pass


def alert_key_columns() -> List[str]:
    """Alerts have no ids, so an alert is identified by its point and
    acquisition time."""
    return ["latitude", "longitude", "acq_date", "acq_time"]


def check_continuity(latest_stored: Optional[date], earliest_in_feed: date) -> None:
    """Raise FeedGapError if days are missing between the stored alerts and the
    feed's earliest available day. Any feed is accepted when nothing is stored
    yet."""
    if latest_stored is None:
        return
    if earliest_in_feed > latest_stored + timedelta(days=1):
        raise FeedGapError(
            f"No alerts between {latest_stored} (newest stored) and "
            f"{earliest_in_feed} (earliest in feed). Download the missing days "
            "from https://firms.modaps.eosdis.nasa.gov/download/, append them with "
            "the alert store, then re-run."
        )


def request_windows(
    start: date, end: date, max_days: int = 5
) -> List[Tuple[date, int]]:
    """(first day, number of days) windows covering start to end inclusive, as
    FIRMS limits each request to a few days."""
    windows = []
    day = start
    while day <= end:
        days = min(max_days, (end - day).days + 1)
        windows.append((day, days))
        day += timedelta(days=days)
    return windows


def parse_feed(content: bytes) -> pa.Table:
    """The feed's alerts, with confidence shortened to its first letter (l, n or
    h) to match the archived science-quality alerts. Raises if the content is
    empty, lacks an alert column, or has an alert without a point or time."""
    alerts = csv.read_csv(
        io.BytesIO(content),
        convert_options=csv.ConvertOptions(
            column_types={
                "latitude": pa.float64(),
                "longitude": pa.float64(),
                "acq_date": pa.date32(),
                "acq_time": pa.string(),
                "confidence": pa.string(),
                "bright_ti4": pa.float64(),
                "bright_ti5": pa.float64(),
                "frp": pa.float64(),
            },
        ),
    )
    columns = alerts.select(
        [
            "latitude",
            "longitude",
            "acq_date",
            "acq_time",
            "confidence",
            "bright_ti4",
            "bright_ti5",
            "frp",
        ]
    )
    for key in alert_key_columns():
        if columns.column(key).null_count > 0:
            raise ValueError(f"Feed has alerts without {key}")
    short_confidence = pc.utf8_slice_codeunits(columns.column("confidence"), 0, 1)
    return columns.set_column(4, "confidence", short_confidence)
