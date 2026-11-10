"""An Iceberg table of fire alert points.

Alerts have no ids, so an alert is identified by its point and acquisition time.
The table is partitioned by month and coarse S2 cell, and each write is sorted
by fine S2 cell so file statistics prune spatial reads.
"""

from datetime import date
from typing import List, Optional

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.catalog import Catalog
from pyiceberg.expressions import GreaterThanOrEqual
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.transforms import IdentityTransform, MonthTransform
from pyiceberg.types import DateType, DoubleType, LongType, NestedField, StringType


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
    """Stores alert points in an Iceberg table, created on first use."""

    def __init__(self, catalog: Catalog, table_name: str):
        self.catalog = catalog
        self.table_name = table_name
        self.key_columns = ["latitude", "longitude", "acq_date", "acq_time"]

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

    def append_unseen(self, alerts: pa.Table) -> int:
        """Append, in one commit, the alerts not already stored, each once.
        Stored keys are read from the same table version the commit builds on,
        so the commit fails rather than duplicating alerts if another writer
        commits in between. Returns the number of alerts appended."""
        table = self.table()
        unseen = first_of_each_key(alerts, self.key_columns)
        if unseen.num_rows > 0:
            unseen = unseen.join(
                self.stored_keys(table, unseen),
                keys=self.key_columns,
                join_type="left anti",
            )
        if unseen.num_rows == 0:
            return 0
        ordered = unseen.sort_by(
            [("s2_cell_fine", "ascending"), ("acq_date", "ascending")]
        )
        table.append(ordered.cast(alert_schema().as_arrow()))
        return unseen.num_rows

    def stored_keys(self, table: Table, alerts: pa.Table) -> pa.Table:
        """Keys of stored alerts from the alerts' earliest date on."""
        earliest = pc.min(alerts.column("acq_date")).as_py()
        return (
            table.scan(
                row_filter=GreaterThanOrEqual("acq_date", earliest),
                selected_fields=tuple(self.key_columns),
            )
            .to_arrow()
            .cast(alerts.select(self.key_columns).schema)
        )


def first_of_each_key(alerts: pa.Table, key_columns: List[str]) -> pa.Table:
    """The alerts with only the first row of each key kept."""
    numbered = alerts.append_column("row", pa.array(range(alerts.num_rows)))
    firsts = numbered.group_by(key_columns, use_threads=False).aggregate(
        [("row", "min")]
    )
    return alerts.take(firsts.column("row_min"))
