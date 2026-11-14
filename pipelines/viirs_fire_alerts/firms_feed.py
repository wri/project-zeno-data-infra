"""Reads NASA FIRMS active fire CSV files, such as the global VIIRS 7-day file."""

import io

import httpx
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as csv

from pipelines.viirs_fire_alerts.alerts import alert_key_columns


def download_feed(url: str) -> bytes:
    response = httpx.get(url, timeout=300, follow_redirects=True)
    response.raise_for_status()
    return response.content


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
