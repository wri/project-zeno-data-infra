import httpx
import pytest

from pipelines.zeno_api import Subscription, ZenoApiClient

SUBSCRIPTION_JSON = {
    "subscription_id": "00000000-0000-0000-0000-000000000001",
    "dashboard_id": "00000000-0000-0000-0000-000000000002",
    "user_id": "user-1",
    "user_email": "user@example.org",
    "aoi_type": "admin",
    "aoi_id": "BRA.16",
    "dataset": "integrated_alerts",
}


def _client(handler):
    return ZenoApiClient(
        token="superuser-key",
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )


def test_list_subscriptions_gets_admin_endpoint_with_bearer_token():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, json=[SUBSCRIPTION_JSON])

    subscriptions = _client(handler).list_subscriptions()

    (request,) = requests
    assert str(request.url) == (
        "https://api.globalnaturewatch.org/api/admin/subscriptions"
    )
    assert request.headers["Authorization"] == "Bearer superuser-key"
    assert subscriptions == [Subscription(**SUBSCRIPTION_JSON)]


def test_list_subscriptions_raises_on_error():
    def handler(request):
        return httpx.Response(403, json={"detail": "Not a superuser"})

    with pytest.raises(httpx.HTTPStatusError):
        _client(handler).list_subscriptions()
