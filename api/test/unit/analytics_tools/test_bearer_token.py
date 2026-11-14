from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from analytics_tools.bearer_token import RequireBearerToken


def protected_client():
    app = Starlette(routes=[Route("/", lambda request: PlainTextResponse("ok"))])
    return TestClient(RequireBearerToken(app))


def test_requests_without_a_bearer_token_are_rejected():
    assert protected_client().get("/").status_code == 401


def test_requests_with_a_bearer_token_pass_through():
    response = protected_client().get("/", headers={"Authorization": "Bearer abc"})

    assert response.status_code == 200
