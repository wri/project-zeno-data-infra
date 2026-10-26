import asyncio
import threading
import time
from datetime import datetime, timezone
from typing import Callable, Dict, List, Tuple

import pyarrow as pa
import s2sphere
import shapely
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import (
    And,
    BooleanExpression,
    EqualTo,
    GreaterThanOrEqual,
    In,
    LessThanOrEqual,
)
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import (
    BinaryType,
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)
from shapely.geometry.base import BaseGeometry

from app.domain.models.custom_aoi import CustomAoi, MonitorStatus
from app.domain.repositories.custom_aoi_repository import CustomAoiRepository


def custom_aoi_schema() -> Schema:
    return Schema(
        NestedField(1, "aoi_id", StringType(), required=True),
        NestedField(2, "area_ha", DoubleType(), required=True),
        NestedField(3, "geometry", BinaryType(), required=True),
        NestedField(4, "updated_at", TimestamptzType(), required=True),
        NestedField(5, "name", StringType(), required=True),
        NestedField(6, "user_id", StringType(), required=True),
        NestedField(7, "monitor_status", StringType(), required=True),
        NestedField(8, "xmin", DoubleType(), required=True),
        NestedField(9, "ymin", DoubleType(), required=True),
        NestedField(10, "xmax", DoubleType(), required=True),
        NestedField(11, "ymax", DoubleType(), required=True),
        NestedField(12, "s2_cell", LongType(), required=True),
    )


def custom_aoi_cell_schema() -> Schema:
    return Schema(
        NestedField(1, "aoi_id", StringType(), required=True),
        NestedField(2, "s2_cell", LongType(), required=True),
    )


class CachedTables:
    """Loaded Iceberg tables, reused until they are older than max_age_seconds so
    reads don't fetch table metadata from storage every time."""

    def __init__(self, catalog: Catalog, max_age_seconds: float, clock=time.monotonic):
        self.catalog = catalog
        self.max_age_seconds = max_age_seconds
        self.clock = clock
        self.loaded: Dict[str, Tuple[Table, float]] = {}
        self.lock = threading.Lock()

    def get(self, name: str, fresh: bool = False) -> Table:
        with self.lock:
            cached = self.loaded.get(name)
        if not fresh and cached and self.clock() - cached[1] < self.max_age_seconds:
            return cached[0]
        table = self.catalog.load_table(name)
        self.keep(name, table)
        return table

    def keep(self, name: str, table: Table) -> None:
        with self.lock:
            self.loaded[name] = (table, self.clock())


class IcebergCustomAoiRepository(CustomAoiRepository):
    """Stores custom AOIs in an Iceberg table with WKB geometries, partitioned by
    the S2 cell of each geometry's centroid so spatial reads skip most files.

    A second table indexes each AOI's cell, so reads and writes by id only touch
    that AOI's partitions. An id without an index entry is treated as new. The
    data table is always written first, so if an index write fails, the next save
    of that AOI repairs the entry. Both tables are provisioned ahead of time; the
    partition is added on first use if it's missing.

    Reads reuse loaded tables for up to max_table_age_seconds, so they may miss
    another instance's writes for that long; this instance's own writes are
    visible immediately."""

    def __init__(
        self,
        catalog: Catalog,
        table_name: str,
        index_table_name: str,
        cell_level: int = 5,
        commit_attempts: int = 3,
        max_table_age_seconds: float = 30,
    ):
        self.catalog = catalog
        self.table_name = table_name
        self.index_table_name = index_table_name
        self.cell_level = cell_level
        self.commit_attempts = commit_attempts
        self.tables = CachedTables(catalog, max_table_age_seconds)

    async def upsert(self, aois: List[CustomAoi]) -> None:
        await asyncio.to_thread(self._upsert, aois)

    async def delete(self, aoi_id: str) -> None:
        await asyncio.to_thread(self._delete, aoi_id)

    async def get(self, aoi_ids: List[str]) -> List[CustomAoi]:
        return await asyncio.to_thread(self._get, aoi_ids)

    async def intersecting(self, geometry: BaseGeometry) -> List[CustomAoi]:
        return await asyncio.to_thread(self._intersecting, geometry)

    def _get(self, aoi_ids: List[str]) -> List[CustomAoi]:
        cells = self.cells(aoi_ids)
        if not cells:
            return []
        rows = self.table().scan(row_filter=self.rows_for(list(cells), cells.values()))
        by_id = {aoi.aoi_id: aoi for aoi in from_arrow(rows.to_arrow())}
        return [by_id[aoi_id] for aoi_id in aoi_ids if aoi_id in by_id]

    def _intersecting(self, geometry: BaseGeometry) -> List[CustomAoi]:
        """Prunes files and rows by bounding box, then keeps the AOIs that
        actually intersect the geometry."""
        xmin, ymin, xmax, ymax = geometry.bounds
        overlapping = And(
            And(LessThanOrEqual("xmin", xmax), GreaterThanOrEqual("xmax", xmin)),
            And(LessThanOrEqual("ymin", ymax), GreaterThanOrEqual("ymax", ymin)),
        )
        candidates = from_arrow(self.table().scan(row_filter=overlapping).to_arrow())
        shapely.prepare(geometry)
        return [aoi for aoi in candidates if geometry.intersects(aoi.geometry)]

    def _upsert(self, aois: List[CustomAoi]) -> None:
        rows = to_arrow(aois, self.cell_level)
        ids = [aoi.aoi_id for aoi in aois]
        previous_cells = self.cells(ids)
        new_cells = rows.column("s2_cell").to_pylist()
        replaced = self.rows_for(ids, [*previous_cells.values(), *new_cells])

        def replace(table: Table) -> None:
            with table.transaction() as transaction:
                transaction.delete(replaced)
                transaction.append(rows)

        self.commit(self.table, replace)
        index_rows = rows.select(["aoi_id", "s2_cell"]).cast(
            custom_aoi_cell_schema().as_arrow()
        )
        self.commit(
            self.index_table,
            lambda index: index.upsert(index_rows, join_cols=["aoi_id"]),
        )

    def _delete(self, aoi_id: str) -> None:
        cells = self.cells([aoi_id])
        if cells:
            self.commit(
                self.table,
                lambda table: table.delete(self.rows_for([aoi_id], cells.values())),
            )
        else:
            self.commit(
                self.table, lambda table: table.delete(EqualTo("aoi_id", aoi_id))
            )
        self.commit(
            self.index_table, lambda index: index.delete(EqualTo("aoi_id", aoi_id))
        )

    def cells(self, aoi_ids: List[str]) -> Dict[str, int]:
        """The indexed S2 cell of each id that has an index entry."""
        entries = (
            self.index_table()
            .scan(
                row_filter=In("aoi_id", aoi_ids), selected_fields=("aoi_id", "s2_cell")
            )
            .to_arrow()
        )
        return dict(
            zip(
                entries.column("aoi_id").to_pylist(),
                entries.column("s2_cell").to_pylist(),
            )
        )

    @staticmethod
    def rows_for(aoi_ids: List[str], cells) -> BooleanExpression:
        """The rows of these ids, limited to the partitions of these cells."""
        return And(In("aoi_id", aoi_ids), In("s2_cell", set(cells)))

    def table(self, fresh: bool = False) -> Table:
        table = self.tables.get(self.table_name, fresh)
        if self.is_partitioned(table):
            return table
        table = self.partition(table)
        self.tables.keep(self.table_name, table)
        return table

    def index_table(self, fresh: bool = False) -> Table:
        return self.tables.get(self.index_table_name, fresh)

    @staticmethod
    def is_partitioned(table: Table) -> bool:
        return any(field.name == "s2_cell" for field in table.spec().fields)

    def partition(self, table: Table) -> Table:
        """Adds the S2 cell partition. Another writer may add it first, so a
        failed commit is checked against the latest table before retrying."""
        for attempt in range(1, self.commit_attempts + 1):
            try:
                with table.update_spec() as update:
                    update.add_identity("s2_cell")
                return self.catalog.load_table(self.table_name)
            except CommitFailedException:
                table = self.catalog.load_table(self.table_name)
                if self.is_partitioned(table):
                    return table
                if attempt == self.commit_attempts:
                    raise
        return table

    def commit(
        self, load: Callable[..., Table], change: Callable[[Table], object]
    ) -> None:
        """Applies the change to the latest table, retrying when another writer
        committed first. The committed table replaces the cached one, so this
        instance's reads see its own writes."""
        for attempt in range(1, self.commit_attempts + 1):
            try:
                table = load(fresh=True)
                change(table)
                self.tables.keep(".".join(table.name()), table)
                return
            except CommitFailedException:
                if attempt == self.commit_attempts:
                    raise


def to_arrow(aois: List[CustomAoi], cell_level: int) -> pa.Table:
    """Rows for the AOIs, all stamped with the same save time."""
    bounds = shapely.bounds([aoi.geometry for aoi in aois])
    return pa.table(
        {
            "aoi_id": [aoi.aoi_id for aoi in aois],
            "area_ha": [aoi.area_ha for aoi in aois],
            "geometry": [shapely.to_wkb(aoi.geometry) for aoi in aois],
            "updated_at": [datetime.now(timezone.utc)] * len(aois),
            "name": [aoi.name for aoi in aois],
            "user_id": [aoi.user_id for aoi in aois],
            "monitor_status": [aoi.monitor_status.value for aoi in aois],
            "xmin": bounds[:, 0],
            "ymin": bounds[:, 1],
            "xmax": bounds[:, 2],
            "ymax": bounds[:, 3],
            "s2_cell": [s2_cell(aoi.geometry, cell_level) for aoi in aois],
        },
        schema=custom_aoi_schema().as_arrow(),
    )


def from_arrow(rows: pa.Table) -> List[CustomAoi]:
    return [
        CustomAoi(
            aoi_id=row["aoi_id"],
            geometry=shapely.from_wkb(row["geometry"]),
            area_ha=row["area_ha"],
            name=row["name"],
            user_id=row["user_id"],
            monitor_status=MonitorStatus(row["monitor_status"]),
        )
        for row in rows.to_pylist()
    ]


def s2_cell(geometry, level: int) -> int:
    """The S2 cell at this level containing the geometry's centroid, as a signed
    64-bit integer to fit Iceberg's long type."""
    centroid = geometry.centroid
    cell = s2sphere.CellId.from_lat_lng(
        s2sphere.LatLng.from_degrees(centroid.y, centroid.x)
    ).parent(level)
    return cell.id() - 2**64 if cell.id() >= 2**63 else cell.id()
