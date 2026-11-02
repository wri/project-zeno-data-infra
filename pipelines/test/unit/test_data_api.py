import json

import httpx
import pytest

from pipelines.data_api import DataApiClient


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


def test_get_latest_version_needs_no_token():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(
            200, json={"status": "success", "data": {"version": "v20250501"}}
        )

    client = DataApiClient(
        http_client=httpx.Client(transport=httpx.MockTransport(handler))
    )

    assert client.get_latest_version("umd_glad_dist_alerts") == "v20250501"
    (request,) = requests
    assert str(request.url) == (
        "https://data-api.globalforestwatch.org/dataset/umd_glad_dist_alerts/latest"
    )
    assert "Authorization" not in request.headers


def test_get_latest_version_raises_if_no_latest_version():
    def handler(request):
        return httpx.Response(200, json={"status": "error", "data": {}})

    with pytest.raises(ValueError):
        _client(handler).get_latest_version("invalid_dataset")
