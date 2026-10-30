from datetime import datetime, timezone
from typing import Dict, Iterable

import pyarrow as pa
import pyiceberg.catalog
import pytest
import shapely
from pyiceberg.catalog import load_catalog
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
from pyiceberg.utils.config import Config
from shapely.geometry.base import BaseGeometry

# The custom AOI table's schema, as the API's custom AOI store writes it.
CUSTOM_AOI_SCHEMA = Schema(
    NestedField(1, "aoi_id", StringType(), required=True),
    NestedField(2, "name", StringType(), required=True),
    NestedField(3, "user_id", StringType(), required=True),
    NestedField(4, "email", StringType(), required=True),
    NestedField(5, "monitor_status", StringType(), required=True),
    NestedField(6, "geometry", BinaryType(), required=True),
    NestedField(7, "area_ha", DoubleType(), required=True),
    NestedField(8, "xmin", DoubleType(), required=True),
    NestedField(9, "ymin", DoubleType(), required=True),
    NestedField(10, "xmax", DoubleType(), required=True),
    NestedField(11, "ymax", DoubleType(), required=True),
    NestedField(12, "s2_cell", LongType(), required=True),
    NestedField(13, "updated_at", TimestamptzType(), required=True),
)


@pytest.fixture
def custom_aoi_table(tmp_path, monkeypatch) -> Table:
    """Create an empty custom AOI table in a local SQL catalog, configured
    through environment variables as on the cluster."""
    monkeypatch.setenv("PYICEBERG_CATALOG__AOIS__TYPE", "sql")
    monkeypatch.setenv(
        "PYICEBERG_CATALOG__AOIS__URI", f"sqlite:///{tmp_path}/catalog.db"
    )
    monkeypatch.setenv(
        "PYICEBERG_CATALOG__AOIS__WAREHOUSE", f"file://{tmp_path}/warehouse"
    )
    # pyiceberg reads its environment once, at import
    monkeypatch.setattr(pyiceberg.catalog, "_ENV_CONFIG", Config())
    catalog = load_catalog("aois")
    catalog.create_namespace("custom")
    return catalog.create_table("custom.custom_aois", CUSTOM_AOI_SCHEMA)


def custom_aoi_rows(
    aois: Dict[str, BaseGeometry], not_monitored: Iterable[str] = ()
) -> pa.Table:
    """Build rows for the custom AOI table, monitored unless listed in
    not_monitored."""
    ids, geometries = list(aois), list(aois.values())
    bounds = shapely.bounds(geometries)
    return pa.table(
        {
            "aoi_id": ids,
            "name": ids,
            "user_id": ["user-1"] * len(ids),
            "email": ["user@example.org"] * len(ids),
            "monitor_status": [
                "not_monitored" if aoi_id in not_monitored else "monitored"
                for aoi_id in ids
            ],
            "geometry": shapely.to_wkb(geometries).tolist(),
            "area_ha": [0.0] * len(ids),
            "xmin": bounds[:, 0],
            "ymin": bounds[:, 1],
            "xmax": bounds[:, 2],
            "ymax": bounds[:, 3],
            "s2_cell": [0] * len(ids),
            "updated_at": [datetime.now(timezone.utc)] * len(ids),
        },
        schema=CUSTOM_AOI_SCHEMA.as_arrow(),
    )
