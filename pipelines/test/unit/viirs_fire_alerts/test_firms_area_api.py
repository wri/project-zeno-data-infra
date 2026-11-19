import datetime

import httpx
import pyarrow as pa
import pytest

from pipelines.viirs_fire_alerts.firms_area_api import (
    FeedGapError,
    FirmsAreaApi,
    FirmsRequestError,
    check_continuity,
    parse_feed,
    request_windows,
)

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


NOV_3 = datetime.date(2026, 11, 3)


def test_feed_continuing_from_the_day_after_the_stored_alerts_is_accepted():
    check_continuity(
        latest_stored=NOV_3, earliest_in_feed=NOV_3 + datetime.timedelta(1)
    )


def test_first_feed_is_accepted():
    check_continuity(latest_stored=None, earliest_in_feed=NOV_3)


def test_feed_leaving_a_missing_day_is_rejected_with_backfill_instructions():
    with pytest.raises(FeedGapError, match="firms.modaps"):
        check_continuity(
            latest_stored=NOV_3, earliest_in_feed=NOV_3 + datetime.timedelta(2)
        )


def test_request_windows_cover_every_day_in_windows_of_at_most_five_days():
    windows = request_windows(NOV_3, datetime.date(2026, 11, 14))

    assert windows == [
        (NOV_3, 5),
        (datetime.date(2026, 11, 8), 5),
        (datetime.date(2026, 11, 13), 2),
    ]


def test_request_windows_for_a_single_day():
    assert request_windows(NOV_3, NOV_3) == [(NOV_3, 1)]


def serve(responses: dict) -> httpx.Client:
    return httpx.Client(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, text=responses[request.url.path])
        )
    )


def test_first_available_date_is_the_sources_minimum_date():
    client = serve(
        {
            "/api/data_availability/csv/KEY/VIIRS_NOAA20_NRT": (
                "data_id,min_date,max_date\nVIIRS_NOAA20_NRT,2026-08-01,2026-11-18\n"
            )
        }
    )

    api = FirmsAreaApi(client, "KEY", "VIIRS_NOAA20_NRT")

    assert api.first_available_date() == datetime.date(2026, 8, 1)


def test_download_requests_world_alerts_for_the_window():
    client = serve(
        {"/api/area/csv/KEY/VIIRS_NOAA20_NRT/world/5/2026-11-03": "latitude\n1.0\n"}
    )

    api = FirmsAreaApi(client, "KEY", "VIIRS_NOAA20_NRT")

    assert api.download(datetime.date(2026, 11, 3), 5) == b"latitude\n1.0\n"


def test_failed_request_error_hides_the_map_key():
    client = httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(403))
    )
    api = FirmsAreaApi(client, "SECRETKEY", "VIIRS_NOAA20_NRT")

    with pytest.raises(FirmsRequestError, match="403") as error:
        api.download(NOV_3, 5)

    assert "SECRETKEY" not in str(error.value)
    assert error.value.__suppress_context__
