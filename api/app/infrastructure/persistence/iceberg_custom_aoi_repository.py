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
    Or,
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

from app.domain.models.custom_aoi import CustomAoi, CustomAoiGeometry
from app.domain.repositories.custom_aoi_repository import CustomAoiRepository


def custom_aoi_schema() -> Schema:
    return Schema(
        NestedField(1, "aoi_id", StringType(), required=True),
        NestedField(2, "name", StringType(), required=True),
        NestedField(3, "user_id", StringType(), required=True),
        NestedField(4, "geometry", BinaryType(), required=True),
        NestedField(5, "area_ha", DoubleType(), required=True),
        NestedField(6, "xmin", DoubleType(), required=True),
        NestedField(7, "ymin", DoubleType(), required=True),
        NestedField(8, "xmax", DoubleType(), required=True),
        NestedField(9, "ymax", DoubleType(), required=True),
        NestedField(10, "s2_cell", LongType(), required=True),
        NestedField(11, "updated_at", TimestamptzType(), required=True),
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
        with self.lock:
            self.loaded[name] = (table, self.clock())
        return table


class IcebergCustomAoiRepository(CustomAoiRepository):
    """Stores custom AOIs in an Iceberg table with WKB geometries. Utilizes
    Iceberg partitions by Google S2 cell ID for fast spatial reads, and a
    separate aoi_id -> partition table for fast lookup by ID."""

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

    async def get(self, aoi_ids: List[str]) -> List[CustomAoiGeometry]:
        return await asyncio.to_thread(self._get, aoi_ids)

    async def intersecting(self, geometry: BaseGeometry) -> List[CustomAoiGeometry]:
        return await asyncio.to_thread(self._intersecting, geometry)

    def _upsert(self, aois: List[CustomAoi]) -> None:
        rows = to_arrow(aois, self.cell_level)
        ids = [aoi.aoi_id for aoi in aois]
        new_cells = rows.column("s2_cell").to_pylist()

        def replace(table: Table) -> None:
            # read on every attempt, in case another writer just moved an AOI
            cells = [*self.cells(ids, fresh=True).values(), *new_cells]
            with table.transaction() as transaction:
                transaction.delete(self.rows_for(ids, cells))
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
        self.commit(
            self.table,
            lambda table: table.delete(self.rows_with_ids([aoi_id], fresh=True)),
        )
        self.commit(
            self.index_table, lambda index: index.delete(EqualTo("aoi_id", aoi_id))
        )

    def _get(self, aoi_ids: List[str]) -> List[CustomAoiGeometry]:
        rows = self.table().scan(
            row_filter=self.rows_with_ids(aoi_ids),
            selected_fields=("aoi_id", "geometry"),
        )
        by_id = {aoi.aoi_id: aoi for aoi in geometries_from_arrow(rows.to_arrow())}
        return [by_id[aoi_id] for aoi_id in aoi_ids if aoi_id in by_id]

    def _intersecting(self, geometry: BaseGeometry) -> List[CustomAoiGeometry]:
        """Prunes files and rows by bounding box, then keeps the AOIs that
        actually intersect the geometry."""
        xmin, ymin, xmax, ymax = geometry.bounds
        overlapping = And(
            And(LessThanOrEqual("xmin", xmax), GreaterThanOrEqual("xmax", xmin)),
            And(LessThanOrEqual("ymin", ymax), GreaterThanOrEqual("ymax", ymin)),
        )
        rows = self.table().scan(
            row_filter=overlapping, selected_fields=("aoi_id", "geometry")
        )
        shapely.prepare(geometry)
        return [
            aoi
            for aoi in geometries_from_arrow(rows.to_arrow())
            if geometry.intersects(aoi.geometry)
        ]

    def rows_with_ids(
        self, aoi_ids: List[str], fresh: bool = False
    ) -> BooleanExpression:
        """The rows of these ids: indexed ids within their partitions, and ids
        without an index entry across all partitions."""
        cells = self.cells(aoi_ids, fresh)
        unindexed = [aoi_id for aoi_id in aoi_ids if aoi_id not in cells]
        if not cells:
            return In("aoi_id", unindexed)
        indexed = self.rows_for(list(cells), cells.values())
        return Or(indexed, In("aoi_id", unindexed)) if unindexed else indexed

    def cells(self, aoi_ids: List[str], fresh: bool = False) -> Dict[str, int]:
        """The indexed S2 cell of each id that has an index entry."""
        entries = (
            self.index_table(fresh)
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
        if not is_partitioned(table):
            self.partition(table)
            table = self.tables.get(self.table_name, fresh=True)
        return table

    def index_table(self, fresh: bool = False) -> Table:
        return self.tables.get(self.index_table_name, fresh)

    def partition(self, table: Table) -> None:
        """Adds the S2 cell partition. Another writer may add it first, so a
        failed commit is checked against the latest table before retrying."""
        for attempt in range(1, self.commit_attempts + 1):
            try:
                with table.update_spec() as update:
                    update.add_identity("s2_cell")
                return
            except CommitFailedException:
                table = self.catalog.load_table(self.table_name)
                if is_partitioned(table):
                    return
                if attempt == self.commit_attempts:
                    raise

    def commit(
        self, load: Callable[..., Table], change: Callable[[Table], object]
    ) -> None:
        """Applies the change to the latest table, retrying when another writer
        committed first. The committed table stays cached, so this instance's
        reads see its own writes."""
        for attempt in range(1, self.commit_attempts + 1):
            try:
                change(load(fresh=True))
                return
            except CommitFailedException:
                if attempt == self.commit_attempts:
                    raise


def is_partitioned(table: Table) -> bool:
    return any(field.name == "s2_cell" for field in table.spec().fields)


def to_arrow(aois: List[CustomAoi], cell_level: int) -> pa.Table:
    """Rows for the AOIs, all stamped with the same save time."""
    bounds = shapely.bounds([aoi.geometry for aoi in aois])
    return pa.table(
        {
            "aoi_id": [aoi.aoi_id for aoi in aois],
            "name": [aoi.name for aoi in aois],
            "user_id": [aoi.user_id for aoi in aois],
            "geometry": [shapely.to_wkb(aoi.geometry) for aoi in aois],
            "area_ha": [aoi.area_ha for aoi in aois],
            "xmin": bounds[:, 0],
            "ymin": bounds[:, 1],
            "xmax": bounds[:, 2],
            "ymax": bounds[:, 3],
            "s2_cell": [s2_cell(aoi.geometry, cell_level) for aoi in aois],
            "updated_at": [datetime.now(timezone.utc)] * len(aois),
        },
        schema=custom_aoi_schema().as_arrow(),
    )


def geometries_from_arrow(rows: pa.Table) -> List[CustomAoiGeometry]:
    return [
        CustomAoiGeometry(row["aoi_id"], shapely.from_wkb(row["geometry"]))
        for row in rows.to_pylist()
    ]


def s2_cell(geometry: BaseGeometry, level: int) -> int:
    """The S2 cell at this level containing the geometry's centroid, as a signed
    64-bit integer to fit Iceberg's long type."""
    centroid = geometry.centroid
    cell = s2sphere.CellId.from_lat_lng(
        s2sphere.LatLng.from_degrees(centroid.y, centroid.x)
    ).parent(level)
    return cell.id() - 2**64 if cell.id() >= 2**63 else cell.id()
