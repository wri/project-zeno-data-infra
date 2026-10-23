import asyncio
from datetime import datetime, timezone
from typing import Callable, List

import pyarrow as pa
import shapely
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import EqualTo
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import (
    BinaryType,
    DoubleType,
    NestedField,
    StringType,
    TimestamptzType,
)

from app.domain.models.custom_aoi import CustomAoi
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
        identifier_field_ids=[1],
    )


class IcebergCustomAoiRepository(CustomAoiRepository):
    """Stores custom AOIs in an Iceberg table with WKB geometries. The table is
    provisioned ahead of time with this schema."""

    def __init__(self, catalog: Catalog, table_name: str, commit_attempts: int = 3):
        self.catalog = catalog
        self.table_name = table_name
        self.commit_attempts = commit_attempts

    async def upsert(self, aois: List[CustomAoi]) -> None:
        rows = to_arrow(aois)
        await asyncio.to_thread(
            self.commit, lambda table: table.upsert(rows, join_cols=["aoi_id"])
        )

    async def delete(self, aoi_id: str) -> None:
        await asyncio.to_thread(
            self.commit, lambda table: table.delete(EqualTo("aoi_id", aoi_id))
        )

    def table(self) -> Table:
        return self.catalog.load_table(self.table_name)

    def commit(self, change: Callable[[Table], object]) -> None:
        """Retries when another writer committed first, against the latest table."""
        for attempt in range(1, self.commit_attempts + 1):
            try:
                change(self.table())
                return
            except CommitFailedException:
                if attempt == self.commit_attempts:
                    raise


def to_arrow(aois: List[CustomAoi]) -> pa.Table:
    """Rows for the AOIs, all stamped with the same save time."""
    return pa.table(
        {
            "aoi_id": [aoi.aoi_id for aoi in aois],
            "area_ha": [aoi.area_ha for aoi in aois],
            "geometry": [shapely.to_wkb(aoi.geometry) for aoi in aois],
            "updated_at": [datetime.now(timezone.utc)] * len(aois),
            "name": [aoi.name for aoi in aois],
            "user_id": [aoi.user_id for aoi in aois],
            "monitor_status": [aoi.monitor_status.value for aoi in aois],
        },
        schema=custom_aoi_schema().as_arrow(),
    )
