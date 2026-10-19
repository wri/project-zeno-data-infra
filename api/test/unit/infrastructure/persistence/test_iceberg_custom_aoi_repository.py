import pytest
from pyiceberg.catalog.sql import SqlCatalog
from shapely import from_wkb
from shapely.geometry import MultiPolygon, box

from app.domain.models.custom_aoi import CustomAoi
from app.infrastructure.persistence.iceberg_custom_aoi_repository import (
    IcebergCustomAoiRepository,
)


@pytest.fixture
def repository(tmp_path):
    catalog = SqlCatalog(
        "test", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{tmp_path}"
    )
    return IcebergCustomAoiRepository(catalog, "custom.custom_aois")


def aoi(aoi_id, size):
    return CustomAoi(aoi_id, MultiPolygon([box(0, 0, size, size)]), float(size))


def stored_rows(repository):
    rows = repository.table().scan().to_arrow().to_pylist()
    return {row["aoi_id"]: row for row in rows}


@pytest.mark.asyncio
async def test_upsert_creates_the_table_and_replaces_existing_ids(repository):
    await repository.upsert([aoi("a", 1), aoi("b", 1)])
    await repository.upsert([aoi("b", 2)])

    rows = stored_rows(repository)
    assert sorted(rows) == ["a", "b"]
    assert rows["b"]["area_ha"] == 2.0
    assert from_wkb(rows["b"]["geometry"]).area == 4.0


@pytest.mark.asyncio
async def test_delete_is_idempotent(repository):
    await repository.upsert([aoi("a", 1), aoi("b", 1)])

    await repository.delete("a")
    await repository.delete("a")

    assert sorted(stored_rows(repository)) == ["b"]
