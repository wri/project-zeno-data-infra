import datetime

import pyarrow as pa
import pytest

from pipelines.viirs_fire_alerts.firms_feed import parse_feed

HEADER = (
    "latitude,longitude,bright_ti4,scan,track,acq_date,acq_time,satellite,"
    "confidence,version,bright_ti5,frp,daynight\n"
)


def test_parse_feed_keeps_alert_columns_with_short_confidence():
    content = HEADER + (
        "19.39964,-155.28406,346.91,0.43,0.46,2026-11-03,0005,N20,nominal,"
        "2.0NRT,304.81,74.87,D\n"
    )

    alerts = parse_feed(content.encode())

    assert alerts.to_pylist() == [
        {
            "latitude": 19.39964,
            "longitude": -155.28406,
            "acq_date": datetime.date(2026, 11, 3),
            "acq_time": "0005",
            "confidence": "n",
            "bright_ti4": 346.91,
            "bright_ti5": 304.81,
            "frp": 74.87,
        }
    ]


def test_parse_feed_rejects_missing_columns():
    content = "latitude,longitude\n1.0,2.0\n"

    with pytest.raises((KeyError, pa.ArrowInvalid)):
        parse_feed(content.encode())


def test_parse_feed_rejects_alerts_missing_a_key_value():
    content = HEADER + (
        ",-155.28406,346.91,0.43,0.46,2026-11-03,0005,N20,nominal,"
        "2.0NRT,304.81,74.87,D\n"
    )

    with pytest.raises(ValueError):
        parse_feed(content.encode())


def test_parse_feed_rejects_empty_content():
    with pytest.raises(pa.ArrowInvalid):
        parse_feed(b"")
