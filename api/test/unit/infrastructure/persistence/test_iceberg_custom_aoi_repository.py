import pytest
import s2sphere
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchTableError
from shapely import from_wkb
from shapely.geometry import MultiPolygon, box

from app.domain.models.custom_aoi import CustomAoi, MonitorStatus
from app.infrastructure.persistence.iceberg_custom_aoi_repository import (
    IcebergCustomAoiRepository,
    custom_aoi_cell_schema,
    custom_aoi_schema,
)

TABLE = "custom.custom_aois"
INDEX = "custom.custom_aoi_cells"


@pytest.fixture
def catalog(tmp_path):
    catalog = SqlCatalog(
        "test", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{tmp_path}"
    )
    catalog.create_namespace("custom")
    # both tables are provisioned ahead of time, unpartitioned, as Terraform does
    catalog.create_table(TABLE, custom_aoi_schema())
    catalog.create_table(INDEX, custom_aoi_cell_schema())
    return catalog


@pytest.fixture
def repository(catalog):
    return IcebergCustomAoiRepository(catalog, TABLE, INDEX)


def aoi(aoi_id, size=1, monitor_status=MonitorStatus.not_monitored, origin=(0, 0)):
    x, y = origin
    return CustomAoi(
        aoi_id,
        MultiPolygon([box(x, y, x + size, y + size)]),
        float(size),
        name=f"Area {aoi_id}",
        user_id="user-1",
        monitor_status=monitor_status,
    )


def rows_by_id(catalog, table_name):
    rows = catalog.load_table(table_name).scan().to_arrow().to_pylist()
    return {row["aoi_id"]: row for row in rows}


def cell_contains(cell, lng, lat):
    point = s2sphere.CellId.from_lat_lng(s2sphere.LatLng.from_degrees(lat, lng))
    return s2sphere.CellId(cell % 2**64).contains(point)


@pytest.mark.asyncio
async def test_upsert_replaces_existing_ids(repository, catalog):
    await repository.upsert([aoi("a"), aoi("b")])
    await repository.upsert([aoi("b", 2, MonitorStatus.monitored)])

    rows = rows_by_id(catalog, TABLE)
    assert sorted(rows) == ["a", "b"]
    assert rows["b"]["area_ha"] == 2.0
    assert (rows["b"]["name"], rows["b"]["user_id"], rows["b"]["monitor_status"]) == (
        "Area b",
        "user-1",
        "monitored",
    )
    assert from_wkb(rows["b"]["geometry"]).area == 4.0


@pytest.mark.asyncio
async def test_first_use_partitions_the_table_by_s2_cell_once(catalog):
    await IcebergCustomAoiRepository(catalog, TABLE, INDEX).upsert([aoi("a")])
    await IcebergCustomAoiRepository(catalog, TABLE, INDEX).upsert([aoi("b")])

    fields = catalog.load_table(TABLE).spec().fields
    assert [field.name for field in fields] == ["s2_cell"]


@pytest.mark.asyncio
async def test_saves_record_the_bbox_and_s2_cell_and_index_the_cell(
    repository, catalog
):
    await repository.upsert([aoi("a", origin=(10, 20))])

    row = rows_by_id(catalog, TABLE)["a"]
    assert (row["xmin"], row["ymin"], row["xmax"], row["ymax"]) == (10, 20, 11, 21)
    assert cell_contains(row["s2_cell"], 10.5, 20.5)
    assert rows_by_id(catalog, INDEX)["a"]["s2_cell"] == row["s2_cell"]


@pytest.mark.asyncio
async def test_an_aoi_moved_to_another_cell_is_stored_once_in_its_new_cell(
    repository, catalog
):
    await repository.upsert([aoi("a", origin=(0, 0))])
    await repository.upsert([aoi("a", origin=(100, 40))])

    rows = catalog.load_table(TABLE).scan().to_arrow().to_pylist()
    assert len(rows) == 1
    assert cell_contains(rows[0]["s2_cell"], 100.5, 40.5)
    assert rows_by_id(catalog, INDEX)["a"]["s2_cell"] == rows[0]["s2_cell"]


@pytest.mark.asyncio
async def test_delete_removes_the_aoi_and_its_index_entry_idempotently(
    repository, catalog
):
    await repository.upsert([aoi("a"), aoi("b", origin=(100, 40))])

    await repository.delete("a")
    await repository.delete("a")

    assert sorted(rows_by_id(catalog, TABLE)) == ["b"]
    assert sorted(rows_by_id(catalog, INDEX)) == ["b"]


@pytest.mark.asyncio
async def test_writes_fail_when_the_tables_were_never_provisioned(tmp_path):
    catalog = SqlCatalog(
        "test", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{tmp_path}"
    )
    repository = IcebergCustomAoiRepository(catalog, TABLE, INDEX)

    with pytest.raises(NoSuchTableError):
        await repository.upsert([aoi("a")])
