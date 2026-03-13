import pytest
from fastapi import Depends, FastAPI  # noqa: E402
from fastapi.testclient import TestClient

from app.dependencies import get_environment
from app.domain.models.environment import Environment

# FastAPI dependencies can't be used directly as default values in path
# functions without Depends() — use a thin wrapper instead.

_app = FastAPI()


@_app.get("/test-env")
async def env_endpoint2(env: Environment = Depends(get_environment)):
    return {"environment": env}


_client = TestClient(_app)


class TestGetEnvironmentDependency:
    @pytest.mark.asyncio
    async def test_missing_header_defaults_to_production(self):
        response = _client.get("/test-env")
        assert response.status_code == 200
        assert response.json()["environment"] == "production"

    @pytest.mark.asyncio
    async def test_staging_header_returns_staging(self):
        response = _client.get("/test-env", headers={"x-environment": "staging"})
        assert response.status_code == 200
        assert response.json()["environment"] == "staging"

    @pytest.mark.asyncio
    async def test_production_header_returns_production(self):
        response = _client.get("/test-env", headers={"x-environment": "production"})
        assert response.status_code == 200
        assert response.json()["environment"] == "production"

    @pytest.mark.asyncio
    async def test_invalid_header_value_returns_422(self):
        response = _client.get("/test-env", headers={"x-environment": "invalid"})
        assert response.status_code == 422
