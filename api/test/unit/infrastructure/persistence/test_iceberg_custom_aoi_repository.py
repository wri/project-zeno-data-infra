import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchTableError
from shapely import from_wkb
from shapely.geometry import MultiPolygon, box

from app.domain.models.custom_aoi import CustomAoi, MonitorStatus
from app.infrastructure.persistence.iceberg_custom_aoi_repository import (
    IcebergCustomAoiRepository,
    custom_aoi_schema,
)


@pytest.fixture
def repository(tmp_path):
    catalog = SqlCatalog(
        "test", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{tmp_path}"
    )
    catalog.create_namespace("custom")
    # the table is provisioned ahead of time, as Terraform does
    catalog.create_table("custom.custom_aois", custom_aoi_schema())
    return IcebergCustomAoiRepository(catalog, "custom.custom_aois")


def aoi(aoi_id, size, monitor_status=MonitorStatus.not_monitored):
    return CustomAoi(
        aoi_id,
        MultiPolygon([box(0, 0, size, size)]),
        float(size),
        name=f"Area {aoi_id}",
        user_id="user-1",
        monitor_status=monitor_status,
    )


def stored_rows(repository):
    rows = repository.table().scan().to_arrow().to_pylist()
    return {row["aoi_id"]: row for row in rows}


@pytest.mark.asyncio
async def test_upsert_replaces_existing_ids(repository):
    await repository.upsert([aoi("a", 1), aoi("b", 1)])
    await repository.upsert([aoi("b", 2, MonitorStatus.monitored)])

    rows = stored_rows(repository)
    assert sorted(rows) == ["a", "b"]
    assert rows["b"]["area_ha"] == 2.0
    assert (rows["b"]["name"], rows["b"]["user_id"], rows["b"]["monitor_status"]) == (
        "Area b",
        "user-1",
        "monitored",
    )
    assert from_wkb(rows["b"]["geometry"]).area == 4.0


@pytest.mark.asyncio
async def test_delete_is_idempotent(repository):
    await repository.upsert([aoi("a", 1), aoi("b", 1)])

    await repository.delete("a")
    await repository.delete("a")

    assert sorted(stored_rows(repository)) == ["b"]


@pytest.mark.asyncio
async def test_writes_fail_when_the_table_was_never_provisioned(tmp_path):
    catalog = SqlCatalog(
        "test", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{tmp_path}"
    )
    repository = IcebergCustomAoiRepository(catalog, "custom.custom_aois")

    with pytest.raises(NoSuchTableError):
        await repository.upsert([aoi("a", 1)])
