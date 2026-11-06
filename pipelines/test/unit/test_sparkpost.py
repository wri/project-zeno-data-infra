import json

import httpx
import pytest

from pipelines.sparkpost import SparkPostClient


def _client(handler, **kwargs):
    return SparkPostClient(
        api_key="secret-key",
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        **kwargs,
    )


def _ok(requests):
    def handler(request):
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "results": {
                    "id": "123",
                    "total_accepted_recipients": 1,
                    "total_rejected_recipients": 0,
                }
            },
        )

    return handler


def test_send_template_posts_transmission_with_raw_api_key():
    requests = []

    results = _client(_ok(requests)).send_template(
        "weekly-alerts", {"total_ha": "12"}, ["a@example.com"]
    )

    assert results["id"] == "123"
    (request,) = requests
    assert request.method == "POST"
    assert str(request.url) == "https://api.sparkpost.com/api/v1/transmissions"
    assert request.headers["Authorization"] == "secret-key"
    assert json.loads(request.content) == {
        "content": {"template_id": "weekly-alerts"},
        "substitution_data": {"total_ha": "12"},
        "recipients": [{"address": {"email": "a@example.com"}}],
        "options": {"open_tracking": False, "click_tracking": False},
    }


def test_send_template_leaves_tracking_to_template_when_link_tracking_enabled():
    requests = []

    _client(_ok(requests), track_links=True).send_template(
        "weekly-alerts", {}, ["a@example.com"]
    )

    assert "options" not in json.loads(requests[0].content)


def test_send_template_raises_on_error():
    def handler(request):
        return httpx.Response(400, json={"errors": [{"message": "bad template"}]})

    with pytest.raises(httpx.HTTPStatusError):
        _client(handler).send_template("weekly-alerts", {}, ["a@example.com"])
