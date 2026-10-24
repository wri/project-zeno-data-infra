import json

import httpx
import pytest

from pipelines.wur_alert_drivers.data_api import DataApiClient


def _client(handler):
    return DataApiClient(
        token="secret-token",
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )


def test_create_version_puts_body_with_bearer_token():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, json={"status": "success", "data": {}})

    _client(handler).create_version("my_dataset", "v20261018", {"a": 1})

    (request,) = requests
    assert request.method == "PUT"
    assert str(request.url) == (
        "https://data-api.globalforestwatch.org/dataset/my_dataset/v20261018"
    )
    assert request.headers["Authorization"] == "Bearer secret-token"
    assert json.loads(request.content) == {"a": 1}


def test_get_version_returns_none_when_version_does_not_exist():
    def handler(request):
        return httpx.Response(404, json={"status": "failed"})

    assert _client(handler).get_version("my_dataset", "v20261018") is None


def test_errors_are_raised():
    def handler(request):
        return httpx.Response(500, json={"status": "error"})

    with pytest.raises(httpx.HTTPStatusError):
        _client(handler).get_assets("my_dataset", "v20261018")
