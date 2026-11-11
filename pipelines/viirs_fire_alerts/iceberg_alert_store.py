"""An Iceberg table of fire alert points.

The table is partitioned by month and coarse Google S2 cell, and each write is
sorted by fine S2 cell so file statistics prune spatial reads.
"""

from datetime import date
from typing import Optional

import pyarrow as pa
import pyarrow.compute as pc
import s2sphere
from pyiceberg.catalog import Catalog
from pyiceberg.expressions import GreaterThanOrEqual
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.transforms import IdentityTransform, MonthTransform
from pyiceberg.types import DateType, DoubleType, LongType, NestedField, StringType

from pipelines.viirs_fire_alerts.alerts import alert_key_columns


def alert_schema() -> Schema:
    return Schema(
        NestedField(1, "latitude", DoubleType(), required=True),
        NestedField(2, "longitude", DoubleType(), required=True),
        NestedField(3, "acq_date", DateType(), required=True),
        NestedField(4, "acq_time", StringType(), required=True),
        NestedField(5, "confidence", StringType(), required=False),
        NestedField(6, "bright_ti4", DoubleType(), required=False),
        NestedField(7, "bright_ti5", DoubleType(), required=False),
        NestedField(8, "frp", DoubleType(), required=False),
        NestedField(9, "s2_cell", LongType(), required=True),
        NestedField(10, "s2_cell_fine", LongType(), required=True),
    )


def alert_partition_spec() -> PartitionSpec:
    return PartitionSpec(
        PartitionField(3, 1000, MonthTransform(), "acq_date_month"),
        PartitionField(9, 1001, IdentityTransform(), "s2_cell"),
    )


def alert_sort_order() -> SortOrder:
    return SortOrder(
        SortField(10, IdentityTransform()),
        SortField(3, IdentityTransform()),
    )


class IcebergAlertStore:
    """Stores alert points in an Iceberg table, created on first use, adding
    each alert's S2 cells at these levels for partitioning and ordering."""

    def __init__(
        self,
        catalog: Catalog,
        table_name: str,
        coarse_cell_level: int,
        fine_cell_level: int,
    ):
        self.catalog = catalog
        self.table_name = table_name
        self.coarse_cell_level = coarse_cell_level
        self.fine_cell_level = fine_cell_level

    def table(self) -> Table:
        return self.catalog.create_table_if_not_exists(
            self.table_name,
            alert_schema(),
            partition_spec=alert_partition_spec(),
            sort_order=alert_sort_order(),
            # A retried append would be rebased onto the other writer's commit
            # without re-reading stored keys, duplicating alerts.
            properties={"commit.retry.num-retries": "0"},
        )

    def latest_date(self) -> Optional[date]:
        """The newest stored alert date, from file statistics (no data is read)."""
        files = self.table().inspect.files()
        if files.num_rows == 0:
            return None
        upper_bounds = pc.struct_field(
            files.column("readable_metrics"), ["acq_date", "upper_bound"]
        )
        return pc.max(upper_bounds).as_py()

    def append_new(self, alerts: pa.Table) -> int:
        """Append, in one commit, the alerts whose keys aren't stored yet. Stored
        keys are read from the same table version the commit builds on, so the
        commit fails rather than duplicating alerts if another writer commits in
        between. Returns the number of alerts appended."""
        if alerts.num_rows == 0:
            return 0
        table = self.table()
        new_alerts = alerts.join(
            stored_keys(table, alerts),
            keys=alert_key_columns(),
            join_type="left anti",
        )
        if new_alerts.num_rows == 0:
            return 0
        rows = add_s2_cells(new_alerts, self.coarse_cell_level, self.fine_cell_level)
        ordered = rows.sort_by(
            [("s2_cell_fine", "ascending"), ("acq_date", "ascending")]
        )
        table.append(ordered.cast(alert_schema().as_arrow()))
        return new_alerts.num_rows


def stored_keys(table: Table, alerts: pa.Table) -> pa.Table:
    """Keys of stored alerts from the alerts' earliest date on."""
    earliest = pc.min(alerts.column("acq_date")).as_py()
    return (
        table.scan(
            row_filter=GreaterThanOrEqual("acq_date", earliest),
            selected_fields=tuple(alert_key_columns()),
        )
        .to_arrow()
        .cast(alerts.select(alert_key_columns()).schema)
    )


def add_s2_cells(alerts: pa.Table, coarse_level: int, fine_level: int) -> pa.Table:
    """Add s2_cell and s2_cell_fine columns: the S2 cells at these levels
    containing each alert's point, as signed 64-bit integers to fit Iceberg's
    long type."""
    fine_cells = [
        s2sphere.CellId.from_lat_lng(s2sphere.LatLng.from_degrees(lat, lon)).parent(
            fine_level
        )
        for lat, lon in zip(
            alerts.column("latitude").to_pylist(),
            alerts.column("longitude").to_pylist(),
        )
    ]
    coarse_cells = [cell.parent(coarse_level) for cell in fine_cells]
    return alerts.append_column(
        "s2_cell", pa.array(signed_ids(coarse_cells), pa.int64())
    ).append_column("s2_cell_fine", pa.array(signed_ids(fine_cells), pa.int64()))


def signed_ids(cells) -> list:
    ids = [cell.id() for cell in cells]
    return [cell_id - 2**64 if cell_id >= 2**63 else cell_id for cell_id in ids]
