import pytest
import s2sphere
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import EqualTo
from shapely import from_wkb
from shapely.geometry import MultiPolygon, Polygon, box

from app.domain.models.custom_aoi import CustomAoi, CustomAoiGeometry, MonitorStatus
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
        email="owner@example.org",
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
    assert (
        rows["b"]["name"],
        rows["b"]["user_id"],
        rows["b"]["email"],
        rows["b"]["monitor_status"],
    ) == ("Area b", "user-1", "owner@example.org", "monitored")
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
async def test_get_returns_only_ids_and_geometries_in_requested_order(repository):
    await repository.upsert([aoi("a"), aoi("b", size=2, origin=(100, 40))])

    found = await repository.get(["b", "missing", "a"])

    assert found == [
        CustomAoiGeometry("b", MultiPolygon([box(100, 40, 102, 42)])),
        CustomAoiGeometry("a", MultiPolygon([box(0, 0, 1, 1)])),
    ]


@pytest.mark.asyncio
async def test_get_finds_aois_whose_index_entry_is_missing(catalog):
    repository = IcebergCustomAoiRepository(
        catalog, TABLE, INDEX, max_table_age_seconds=0
    )
    await repository.upsert([aoi("a")])
    catalog.load_table(INDEX).delete(EqualTo("aoi_id", "a"))

    found = await repository.get(["a"])

    assert [stored.aoi_id for stored in found] == ["a"]


@pytest.mark.asyncio
async def test_intersecting_returns_only_aois_touching_the_geometry(repository):
    await repository.upsert(
        [
            aoi("touched", origin=(10, 20)),
            aoi("inside_bounds_only", origin=(12, 22)),
            aoi("far", origin=(100, 40)),
        ]
    )
    # L shape along the bottom and left of a 10-14, 20-24 square
    l_shape = Polygon(
        [(10, 20), (14, 20), (14, 20.5), (10.5, 20.5), (10.5, 24), (10, 24)]
    )

    found = await repository.intersecting(l_shape)

    assert [stored.aoi_id for stored in found] == ["touched"]


@pytest.mark.asyncio
async def test_cached_tables_see_other_writers_once_they_expire(catalog):
    reader = IcebergCustomAoiRepository(catalog, TABLE, INDEX, max_table_age_seconds=0)
    writer = IcebergCustomAoiRepository(catalog, TABLE, INDEX)
    await writer.upsert([aoi("a")])
    assert [stored.aoi_id for stored in await reader.get(["a"])] == ["a"]

    await writer.upsert([aoi("b", origin=(100, 40))])

    assert [stored.aoi_id for stored in await reader.get(["b"])] == ["b"]


@pytest.mark.asyncio
async def test_own_writes_are_visible_immediately_despite_the_cache(catalog):
    repository = IcebergCustomAoiRepository(
        catalog, TABLE, INDEX, max_table_age_seconds=3600
    )
    await repository.upsert([aoi("a")])
    await repository.get(["a"])

    await repository.upsert([aoi("b", origin=(100, 40))])
    await repository.delete("a")

    assert [stored.aoi_id for stored in await repository.get(["a", "b"])] == ["b"]


@pytest.mark.asyncio
async def test_writes_fail_when_the_tables_were_never_provisioned(tmp_path):
    catalog = SqlCatalog(
        "test", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{tmp_path}"
    )
    repository = IcebergCustomAoiRepository(catalog, TABLE, INDEX)

    with pytest.raises(NoSuchTableError):
        await repository.upsert([aoi("a")])
