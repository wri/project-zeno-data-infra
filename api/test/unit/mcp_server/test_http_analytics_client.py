import httpx
import pytest

from mcp_server.analytics_client import AnalyticsRequestError
from mcp_server.http_analytics_client import HttpAnalyticsClient

BASE_URL = "http://analytics-api"


def client_returning(response, authorization=None, host=None):
    requests = []

    def handler(request):
        requests.append(request)
        return response

    http_client = httpx.AsyncClient(
        base_url=BASE_URL, transport=httpx.MockTransport(handler)
    )
    return HttpAnalyticsClient(http_client, authorization, host), requests


@pytest.mark.asyncio
async def test_refuses_links_outside_the_api():
    client, requests = client_returning(httpx.Response(200, json={}))

    with pytest.raises(AnalyticsRequestError):
        await client.get_analysis("http://169.254.169.254/latest/meta-data")

    assert requests == []


@pytest.mark.asyncio
async def test_the_api_builds_links_for_the_callers_host():
    body = {"data": {"link": "http://analytics.example.org/v0/x/analytics/1"}}
    client, requests = client_returning(
        httpx.Response(202, json=body), host="analytics.example.org"
    )

    await client.create_analysis("/v0/land_change/x/analytics", {})

    assert requests[0].headers["host"] == "analytics.example.org"


@pytest.mark.asyncio
async def test_follows_links_on_the_callers_host_through_the_api():
    body = {"data": {"status": "saved"}}
    client, requests = client_returning(
        httpx.Response(200, json=body), host="analytics.example.org"
    )

    await client.get_analysis("http://analytics.example.org/v0/x/analytics/1")

    assert str(requests[0].url) == f"{BASE_URL}/v0/x/analytics/1"


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


@pytest.mark.asyncio
async def test_search_forwards_the_callers_authorization():
    body = {
        "data": [
            {
                "aoi_type": "admin",
                "aoi_id": "BRA.1",
                "name": "Acre",
                "display_name": "Acre, Brazil",
            }
        ]
    }
    client, requests = client_returning(
        httpx.Response(200, json=body), authorization="Bearer caller-token"
    )

    matches = await client.search_areas("Acre", ["admin"], limit=5)

    assert requests[0].headers["Authorization"] == "Bearer caller-token"
    assert requests[0].url.params.get_list("type") == ["admin"]
    assert requests[0].url.params["q"] == "Acre"
    assert matches[0].aoi == {"type": "admin", "ids": ["BRA.1"]}


@pytest.mark.asyncio
async def test_forbidden_search_carries_the_api_detail():
    client, _ = client_returning(
        httpx.Response(403, json={"detail": "Requires a ResourceWatch admin account"})
    )

    with pytest.raises(AnalyticsRequestError, match="admin account"):
        await client.search_areas("Acre", None, limit=5)
