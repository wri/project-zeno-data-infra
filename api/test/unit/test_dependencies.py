import re

import pytest
from fastapi import Depends, FastAPI  # noqa: E402
from fastapi.testclient import TestClient

import app.dependencies
from app.dependencies import (
    get_aoi_geometry_repository,
    get_custom_aoi_catalog,
    get_custom_aoi_repository,
    get_environment,
)
from app.domain.models.environment import Environment
from app.infrastructure.persistence.geoparquet_aoi_geometry_repository import (
    GeoParquetAoiGeometryRepository,
)
from app.infrastructure.persistence.iceberg_custom_aoi_repository import (
    IcebergCustomAoiRepository,
)

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

    def test_x_environment_header_is_hidden_from_docs(self):
        schema = _app.openapi()

        for path, path_item in schema["paths"].items():
            for method, operation in path_item.items():
                for parameter in operation.get("parameters", []):
                    assert parameter.get("name") != "x-environment", (
                        f"x-environment header should be hidden from docs "
                        f"(found in {method.upper()} {path}). "
                        "Add include_in_schema=False to the Header() in "
                        "get_environment."
                    )


def test_each_aoi_type_has_a_geometry_source():
    repository = get_aoi_geometry_repository()

    assert set(repository.repositories) == {
        "admin",
        "key_biodiversity_area",
        "protected_area",
        "indigenous_land",
        "custom_aoi",
    }
    assert isinstance(
        repository.repositories.pop("custom_aoi"), IcebergCustomAoiRepository
    )
    for source in repository.repositories.values():
        assert isinstance(source, GeoParquetAoiGeometryRepository)
        assert re.fullmatch(
            r"s3://lcl-analytics/vectors/aois/\w+/v[\w.]+/aois\.parquet", source.uri
        )


def test_geometry_sources_open_the_custom_aoi_catalog_only_when_used(monkeypatch):
    def unreachable_catalog(name):
        raise ConnectionError("catalog unreachable")

    monkeypatch.setattr(app.dependencies, "load_catalog", unreachable_catalog)
    get_custom_aoi_catalog.cache_clear()
    get_custom_aoi_repository.cache_clear()

    get_aoi_geometry_repository()
