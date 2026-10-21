import httpx
import pytest

from analytics_tools.analytics_client import AnalyticsRequestError
from analytics_tools.http_analytics_client import HttpAnalyticsClient

BASE_URL = "http://analytics-api"


def client_returning(response):
    requests = []

    def handler(request):
        requests.append(request)
        return response

    http_client = httpx.AsyncClient(
        base_url=BASE_URL, transport=httpx.MockTransport(handler)
    )
    return HttpAnalyticsClient(http_client), requests


@pytest.mark.asyncio
async def test_refuses_links_outside_the_api():
    client, requests = client_returning(httpx.Response(200, json={}))

    with pytest.raises(AnalyticsRequestError):
        await client.get_analysis("http://169.254.169.254/latest/meta-data")

    assert requests == []


@pytest.mark.asyncio
async def test_validation_errors_carry_the_api_detail():
    detail = [{"loc": ["body", "aoi"], "msg": "Field required"}]
    client, _ = client_returning(httpx.Response(422, json={"detail": detail}))

    with pytest.raises(AnalyticsRequestError, match="Field required"):
        await client.create_analysis("/v0/land_change/x/analytics", {})


@pytest.mark.asyncio
async def test_reads_pending_state_and_retry_after():
    body = {"data": {"status": "pending", "message": "processing"}}
    client, _ = client_returning(
        httpx.Response(200, json=body, headers={"Retry-After": "1"})
    )

    analysis = await client.get_analysis(f"{BASE_URL}/v0/land_change/x/analytics/1")

    assert analysis.status == "pending"
    assert analysis.retry_after_seconds == 1
