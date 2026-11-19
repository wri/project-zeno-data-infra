"""NASA FIRMS area API: worldwide active fire alerts for any recent days, parsed
into alert tables, with which days to request so they continue on from the
alerts already stored."""

import csv
import io
from datetime import date, timedelta
from typing import List, Optional, Tuple

import httpx
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as arrow_csv


class FirmsAreaApi:
    """Alerts from one FIRMS source, such as VIIRS_NOAA20_NRT."""

    def __init__(self, client: httpx.Client, map_key: str, source: str):
        self.client = client
        self.map_key = map_key
        self.source = source

    def first_available_date(self) -> date:
        response = self.get(f"data_availability/csv/{self.map_key}/{self.source}")
        row = next(csv.DictReader(io.StringIO(response.text)))
        return date.fromisoformat(row["min_date"])

    def download(self, start: date, days: int) -> bytes:
        """CSV of alerts from start for this many days (at most 5)."""
        path = f"area/csv/{self.map_key}/{self.source}/world/{days}/{start}"
        return self.get(path).content

    def get(self, path: str) -> httpx.Response:
        response = self.client.get(f"https://firms.modaps.eosdis.nasa.gov/api/{path}")
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as error:
            # The map key is part of the URL, so it's masked to keep it out of logs.
            masked = str(error).replace(self.map_key, "***")
            raise FirmsRequestError(masked) from None
        return response


class FirmsRequestError(Exception):
    pass


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
    alerts = arrow_csv.read_csv(
        io.BytesIO(content),
        convert_options=arrow_csv.ConvertOptions(
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
