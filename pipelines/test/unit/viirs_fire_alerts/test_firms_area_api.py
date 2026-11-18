import datetime

import httpx

from pipelines.viirs_fire_alerts.firms_area_api import FirmsAreaApi


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
