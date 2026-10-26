from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from app.request_size_limit import RequestSizeLimit


def client(max_bytes):
    app = FastAPI()

    @app.put("/limited/items")
    async def put_limited(items: list[str]):
        return {"received": len(items)}

    @app.put("/other")
    async def put_other(request: Request):
        return {"received": len(await request.body())}

    # the API also has function middleware, which wraps how the body is received
    @app.middleware("http")
    async def pass_through(request: Request, call_next):
        return await call_next(request)

    app.add_middleware(RequestSizeLimit, max_bytes=max_bytes, path_prefix="/limited")
    return TestClient(app)


def test_bodies_within_the_limit_pass_through():
    response = client(max_bytes=10).put("/limited/items", json=["a", "b"])

    assert response.json() == {"received": 2}


def test_bodies_over_the_limit_are_rejected():
    response = client(max_bytes=10).put("/limited/items", json=["a" * 20])

    assert response.status_code == 413


def test_bodies_without_a_length_header_are_counted_as_they_stream():
    def chunks():
        yield b'["aaaaaa",'
        yield b'"bbbbbb"]'

    response = client(max_bytes=10).put(
        "/limited/items",
        content=chunks(),
        headers={"Content-Type": "application/json"},
    )

    assert response.status_code == 413


def test_other_paths_are_not_limited():
    response = client(max_bytes=10).put("/other", content=b"x" * 11)

    assert response.json() == {"received": 11}
