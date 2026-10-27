import asyncio
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from functools import lru_cache
from typing import Dict, Iterable, Optional, Tuple

import numpy as np
import pandas as pd
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import shapely
from shapely.geometry.base import BaseGeometry

from app.domain.repositories.alert_points_repository import (
    AlertPointsPage,
    AlertPointsQuery,
    AlertPointsRepository,
    AlertPointsSource,
)

# alert_date in the table counts days since this date
ALERT_EPOCH = date(2014, 12, 31)
# Pixel centers of the alerts zarr's first column and row (global 10 m grid), for
# table files written without grid metadata.
ALERTS_GRID = {"x0": -179.99995, "y0": 89.99995, "dx": 0.0001, "dy": -0.0001}
POINT_COLUMNS = ["row", "col", "alert_date", "confidence", "natural_lands_class"]
# Row groups read at a time within a date. A busy date can hold millions of points
# under a large AOI; reading it in batches stops near the page size instead.
READ_BATCH_GROUPS = 16


def build_points_input_uris(version: str) -> Dict[str, str]:
    """The alerts version's points table, written by the integrated alerts
    pipeline before the version is marked latest."""
    return {
        "alert_points_table_uri": (
            f"s3://lcl-analytics/zonal-statistics/integrated-alerts/{version}/"
            "alert-points/by-date.parquet"
        ),
    }


def day_to_date(day: int) -> date:
    return ALERT_EPOCH + timedelta(days=int(day))


def date_to_day(value: str) -> int:
    """Days since ALERT_EPOCH of a YYYY-MM-DD date (month and day may be one digit)."""
    return (datetime.strptime(value, "%Y-%m-%d").date() - ALERT_EPOCH).days


@dataclass(frozen=True)
class AlertPointsTable:
    """Footer of an alert points table parquet: per row group row counts,
    alert_date statistics and extent (a lat/lon box from its row/col statistics),
    with a spatial index over the extents, plus the alerts grid from the
    key-value metadata. A query picks its row groups from these without reading
    any data."""

    path: str
    filesystem: pafs.FileSystem
    metadata: pq.FileMetaData
    num_rows: np.ndarray
    date_min: np.ndarray
    date_max: np.ndarray
    extents: np.ndarray
    extent_tree: shapely.STRtree
    version: str
    latest_day: int
    days_stored: int
    x0: float
    y0: float
    dx: float
    dy: float

    @property
    def first_day(self) -> int:
        return self.latest_day - self.days_stored + 1

    def lon(self, col):
        """Pixel-center longitude of global column index col."""
        return self.x0 + np.asarray(col) * self.dx

    def lat(self, row):
        """Pixel-center latitude of global row index row."""
        return self.y0 + np.asarray(row) * self.dy


def _filesystem(uri: str) -> Tuple[pafs.FileSystem, str]:
    if uri.startswith("s3://"):
        filesystem = pafs.S3FileSystem(
            region="us-east-1",
            connect_timeout=5,
            request_timeout=30,
            retry_strategy=pafs.AwsStandardS3RetryStrategy(max_attempts=5),
        )
        return filesystem, uri.removeprefix("s3://")
    return pafs.LocalFileSystem(), uri


# The footer of a 30-day table is tens of MB, so it's read once per process and
# version. Two entries keep the previous version while a new one takes over.
@lru_cache(maxsize=2)
def load_alert_points_table(uri: str) -> AlertPointsTable:
    filesystem, path = _filesystem(uri)
    with filesystem.open_input_file(path) as f:
        metadata = pq.ParquetFile(f).metadata

    names = [metadata.schema.column(j).name for j in range(metadata.num_columns)]
    row_groups = [metadata.row_group(g) for g in range(metadata.num_row_groups)]

    def statistics(column: str) -> Tuple[np.ndarray, np.ndarray]:
        j = names.index(column)
        stats = [rg.column(j).statistics for rg in row_groups]
        return (
            np.array([s.min for s in stats], dtype=np.int64),
            np.array([s.max for s in stats], dtype=np.int64),
        )

    key_value = {k.decode(): v.decode() for k, v in (metadata.metadata or {}).items()}
    row_min, row_max = statistics("row")
    col_min, col_max = statistics("col")
    date_min, date_max = statistics("alert_date")
    # Table files written without the key-value metadata (the first ones) get the
    # version from their path, the stored window from the date statistics and the
    # grid from ALERTS_GRID.
    if "alerts_version" not in key_value:
        version = re.search(r"/(v\d{8})/", uri)
        if version is None:
            raise ValueError(f"No alerts version in the metadata or path of {uri}.")
        latest_day = int(date_max.max())
        key_value = {
            "alerts_version": version.group(1),
            "latest_alert_date": day_to_date(latest_day).isoformat(),
            "days_stored": str(latest_day - int(date_min.min()) + 1),
            **{f"grid_{k}": repr(v) for k, v in ALERTS_GRID.items()},
        }
    x0, y0 = float(key_value["grid_x0"]), float(key_value["grid_y0"])
    dx, dy = float(key_value["grid_dx"]), float(key_value["grid_dy"])
    # Each row group's extent: the pixel edges around its row/col range. The table
    # is Hilbert-sorted, so a row group covers a compact patch and its extent is
    # tight.
    extents = shapely.box(
        x0 + col_min * dx - abs(dx) / 2,
        y0 + row_max * dy - abs(dy) / 2,
        x0 + col_max * dx + abs(dx) / 2,
        y0 + row_min * dy + abs(dy) / 2,
    )
    return AlertPointsTable(
        path=path,
        filesystem=filesystem,
        metadata=metadata,
        num_rows=np.array([rg.num_rows for rg in row_groups], dtype=np.int64),
        date_min=date_min,
        date_max=date_max,
        extents=extents,
        extent_tree=shapely.STRtree(extents),
        version=key_value["alerts_version"],
        latest_day=date_to_day(key_value["latest_alert_date"]),
        days_stored=int(key_value["days_stored"]),
        x0=x0,
        y0=y0,
        dx=dx,
        dy=dy,
    )


def read_most_recent_rows(
    table: AlertPointsTable,
    geom: BaseGeometry,
    start_day: int,
    end_day: int,
    confidences: Iterable[int],
    classes: Optional[Iterable[int]],
    limit: int,
    after: Optional[Tuple[int, int]] = None,
) -> Tuple[pd.DataFrame, bool, Optional[Tuple[int, int]]]:
    """The newest `limit` alert points whose pixel centers are inside geom, with
    alert_date in [start_day, end_day], confidence in confidences and, unless
    classes is None, natural_lands_class in classes. Sorted newest date first,
    then in the table's order within a date (Hilbert order on the alerts grid),
    which is the same for every request. Also returns whether more points matched
    and the (row group, row) position of the last point returned.

    after, a (row group, row) position from an earlier page, starts the page just
    past that point: the file is in serving order, so everything up to it has
    been served.

    Row groups are those whose extent intersects geom (a query on the extents'
    spatial index) and whose dates overlap the request, read a date at a time
    from the newest and, within a date, in batches of
    READ_BATCH_GROUPS in file order, stopping as soon as the page is over full, so
    memory stays near the page size however many points a date holds. Points from
    row groups entirely inside geom skip the point-in-polygon test. Relies on the
    table never mixing two dates in one row group, so every point on or after a
    date has been read once that date's row groups have.
    """
    shapely.prepare(geom)
    # Sorted, since the tree returns row groups in its own order and pages are
    # read in file order.
    candidates = np.sort(table.extent_tree.query(geom, predicate="intersects"))
    candidates = candidates[
        (table.date_max[candidates] >= start_day)
        & (table.date_min[candidates] <= end_day)
    ]
    if after is not None:
        candidates = candidates[candidates >= after[0]]
    interior = shapely.contains_properly(geom, table.extents[candidates])

    confidences = list(confidences)
    classes = None if classes is None else list(classes)
    found, n_found = [], 0
    read = np.zeros(len(candidates), dtype=bool)
    with table.filesystem.open_input_file(table.path) as f:
        parquet = pq.ParquetFile(f, metadata=table.metadata, pre_buffer=True)
        for day in range(end_day, start_day - 1, -1):
            # Positions in candidates, ascending, so batches follow file order.
            day_groups = np.flatnonzero(
                ~read
                & (table.date_max[candidates] >= day)
                & (table.date_min[candidates] <= day)
            )
            read[day_groups] = True
            for start in range(0, len(day_groups), READ_BATCH_GROUPS):
                stop = start + READ_BATCH_GROUPS
                step = day_groups[start:stop]
                groups = candidates[step]
                points = parquet.read_row_groups(
                    groups.tolist(), columns=POINT_COLUMNS
                ).to_pandas()
                # Each point's position in the file, for the next page's cursor.
                sizes = table.num_rows[groups]
                points["_group"] = np.repeat(groups, sizes)
                points["_offset"] = np.concatenate([np.arange(n) for n in sizes])
                point_interior = np.repeat(interior[step], sizes)
                if after is not None:
                    unserved = (
                        (points["_group"] > after[0]) | (points["_offset"] > after[1])
                    ).to_numpy()
                    points, point_interior = points[unserved], point_interior[unserved]
                points = _keep_points(
                    table,
                    geom,
                    points,
                    point_interior,
                    (start_day, end_day),
                    confidences,
                    classes,
                )
                found.append(points)
                n_found += len(points)
                if n_found > limit:
                    break
            if n_found > limit:
                break

    if not found:
        return _empty_points(), False, None
    # Already newest date first and in file order within a date; the sort only
    # guards that order should a row group ever mix two dates.
    result = pd.concat(found, ignore_index=True).sort_values(
        ["alert_date", "_group", "_offset"],
        ascending=[False, True, True],
        ignore_index=True,
    )
    page = result.head(limit)
    last = (
        (int(page["_group"].iloc[-1]), int(page["_offset"].iloc[-1]))
        if len(page)
        else None
    )
    return page[POINT_COLUMNS], len(result) > limit, last


def _keep_points(
    table, geom, points, interior, days, confidences, classes
) -> pd.DataFrame:
    keep = (
        points["alert_date"].between(*days) & points["confidence"].isin(confidences)
    ).to_numpy()
    if classes is not None:
        keep &= points["natural_lands_class"].isin(classes).to_numpy()
    points, inside = points[keep], interior[keep].copy()

    # Points from row groups entirely inside the AOI are inside it; the others get
    # the exact test, against the AOI prepared in read_most_recent_rows.
    edge = np.flatnonzero(~inside)
    if len(edge) == 0:
        return points
    inside[edge] = shapely.contains_xy(
        geom,
        table.lon(points["col"].to_numpy()[edge]),
        table.lat(points["row"].to_numpy()[edge]),
    )
    return points[inside]


def _empty_points() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "row": np.empty(0, np.int32),
            "col": np.empty(0, np.int32),
            "alert_date": np.empty(0, np.int16),
            "confidence": np.empty(0, np.int8),
            "natural_lands_class": np.empty(0, np.uint8),
        }
    )


class ParquetAlertPointsRepository(AlertPointsRepository):
    """Alert points from an alert points table parquet (see read_most_recent_rows). A
    page's position is the (row group, row) of its last point in the file."""

    def __init__(self, uri: str):
        self.uri = uri

    async def source(self) -> AlertPointsSource:
        table = await asyncio.to_thread(load_alert_points_table, self.uri)
        return AlertPointsSource(
            version=table.version,
            first_date=day_to_date(table.first_day),
            latest_date=day_to_date(table.latest_day),
        )

    async def read_most_recent_points(self, query: AlertPointsQuery) -> AlertPointsPage:
        # Loading the table (cached after the first call) and reading row groups
        # block, so they run in a thread; the rest is cheap.
        table = await asyncio.to_thread(load_alert_points_table, self.uri)
        after = None if query.after is None else _decode_position(query.after)
        rows, more, last = await asyncio.to_thread(
            read_most_recent_rows,
            table,
            query.geometry,
            (query.start_date - ALERT_EPOCH).days,
            (query.end_date - ALERT_EPOCH).days,
            query.confidences,
            query.natural_lands_classes,
            query.limit,
            after,
        )
        days = rows["alert_date"].to_numpy()
        dates = {d: day_to_date(d) for d in np.unique(days)}
        return AlertPointsPage(
            points=pd.DataFrame(
                {
                    "lat": table.lat(rows["row"].to_numpy()),
                    "lon": table.lon(rows["col"].to_numpy()),
                    "alert_date": [dates[d] for d in days],
                    "confidence": rows["confidence"].to_numpy(),
                    "natural_lands_class": rows["natural_lands_class"].to_numpy(),
                }
            ),
            more=more,
            last_position=None if last is None else f"{last[0]}:{last[1]}",
        )


def _decode_position(position: str) -> Tuple[int, int]:
    """The (row group, row) in a position string; ValueError if it isn't one."""
    try:
        group, offset = (int(part) for part in position.split(":"))
    except ValueError:
        raise ValueError("Not a valid points cursor.") from None
    if group < 0 or offset < 0:
        raise ValueError("Not a valid points cursor.")
    return group, offset
