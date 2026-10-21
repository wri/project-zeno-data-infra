"""A small synthetic alert points table (by-date layout) and a brute-force query
to check table reads against."""

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import shapely

# 10 m grid whose tile corner (row 10000, col 10000) is at (11, 0) degrees, so
# the points below straddle four alerts tiles.
GRID = {"x0": 10.00005, "y0": 0.99995, "dx": 0.0001, "dy": -0.0001}
LATEST_DAY = 4307  # 2026-10-16
DAYS_STORED = 5
ROW_GROUP = 10

TABLE_SCHEMA = pa.schema(
    [
        ("row", pa.int32()),
        ("col", pa.int32()),
        ("alert_date", pa.int16()),
        ("confidence", pa.int8()),
        ("natural_lands_class", pa.uint8()),
    ]
)


def make_points(n=4000, seed=0) -> pd.DataFrame:
    """Alert pixels (one alert per pixel) around the tile corner, over the stored
    days."""
    rng = np.random.default_rng(seed)
    cells = rng.choice(200 * 200, size=n, replace=False)
    return pd.DataFrame(
        {
            "row": (9900 + cells // 200).astype(np.int32),
            "col": (9900 + cells % 200).astype(np.int32),
            "alert_date": rng.integers(
                LATEST_DAY - DAYS_STORED + 1, LATEST_DAY + 1, n
            ).astype(np.int16),
            "confidence": rng.integers(2, 5, n).astype(np.int8),
            "natural_lands_class": rng.integers(0, 13, n).astype(np.uint8),
        }
    )


def file_order(points: pd.DataFrame) -> pd.DataFrame:
    """points in the fixture table's file order: newest date first, then compact
    8 x 8 pixel blocks (standing in for the Hilbert order of a real table)."""
    return (
        points.assign(block_row=points.row // 8, block_col=points.col // 8)
        .sort_values(
            ["alert_date", "block_row", "block_col", "row", "col"],
            ascending=[False, True, True, True, True],
        )
        .drop(columns=["block_row", "block_col"])
    )


def write_points_table(path, points: pd.DataFrame, version="v20261018") -> str:
    """Write points in file_order, so no row group spans two dates and some lie
    entirely inside an AOI."""
    metadata = {
        "alerts_version": version,
        "latest_alert_date": "2026-10-16",
        "days_stored": str(DAYS_STORED),
        "grid_x0": repr(GRID["x0"]),
        "grid_y0": repr(GRID["y0"]),
        "grid_dx": repr(GRID["dx"]),
        "grid_dy": repr(GRID["dy"]),
    }
    schema = TABLE_SCHEMA.with_metadata(metadata)
    ordered = file_order(points)
    with pq.ParquetWriter(str(path), schema, write_statistics=True) as writer:
        for _, day_points in ordered.groupby("alert_date", sort=False):
            writer.write_table(
                pa.Table.from_pandas(
                    day_points[TABLE_SCHEMA.names], schema=schema, preserve_index=False
                ),
                row_group_size=ROW_GROUP,
            )
    return str(path)


def lon(col):
    return GRID["x0"] + np.asarray(col) * GRID["dx"]


def lat(row):
    return GRID["y0"] + np.asarray(row) * GRID["dy"]


def brute_force(points, geom, start_day, end_day, confidences, classes):
    """Every matching point, in the order the table serves them (file_order)."""
    keep = (
        points.alert_date.between(start_day, end_day)
        & points.confidence.isin(confidences)
        & (True if classes is None else points.natural_lands_class.isin(classes))
    ).to_numpy()
    keep &= shapely.contains_xy(geom, lon(points.col), lat(points.row))
    return file_order(points[keep]).reset_index(drop=True)


def aoi_polygon():
    """A polygon across the tile corner with one slanted edge, covering whole
    8 x 8 blocks inside and cutting others."""
    return shapely.Polygon(
        [
            (lon(9920), lat(9930)),
            (lon(10080), lat(9930)),
            (lon(10060), lat(10075)),
            (lon(9935), lat(10060)),
        ]
    )
