"""The alert points table: one row per integrated alert pixel in the most recent
days of a release, for listing the newest alerts in an area of interest.

Layout (relied on by the API's reader): newest alert_date first, Hilbert order
on the alerts grid within a date, no row group spanning two dates, row groups
of at most ROW_GROUP_SIZE rows, statistics on, and key-value metadata giving the
alerts version, stored dates and grid.

Built in three steps so no process holds every point:
1. one Dask task per alerts tile writes a part file, one row group per date;
2. one Dask task per date merges that date's rows from every part, in Hilbert
   order, into a date file;
3. the client streams the date files, newest first, into the table.
"""

from datetime import datetime, timedelta
from functools import partial
from typing import Dict, List, Optional, Tuple

import fsspec
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import xarray as xr
from shapely.geometry import Polygon

from pipelines.globals import sbtn_natural_lands_zarr_uri
from pipelines.integrated_alerts.alert_pixels import (
    ALERT_EPOCH,
    alert_tiles,
    open_zarrs,
    read_tile_alert_pixels,
)
from pipelines.prefect_flows.tile_stages import (
    hilbert_index,
    map_tiles,
    worker_cached,
)

DAYS_STORED = 30  # days of alerts kept, ending at the newest alert date
# layers sampled at each alert pixel, by column name
LAYER_URIS = {"natural_lands_class": sbtn_natural_lands_zarr_uri}
ROW_GROUP_SIZE = 10_000
HILBERT_ORDER = 22  # 2**22 cells covers the 3.6M-column global 10 m grid
TABLE_SCHEMA = pa.schema(
    [
        ("row", pa.int32()),
        ("col", pa.int32()),
        ("alert_date", pa.int16()),
        ("confidence", pa.int8()),
        ("natural_lands_class", pa.uint8()),
    ]
)
PART_SCHEMA = TABLE_SCHEMA.append(pa.field("hilbert", pa.int64()))


def latest_alert_day(admin_result_uri: str) -> int:
    """The newest alert date in a release, as days since ALERT_EPOCH, from the
    admin results parquet's alert_date statistics (no data is read)."""
    with fsspec.open(admin_result_uri, "rb") as f:
        metadata = pq.ParquetFile(f).metadata
    names = [metadata.schema.column(j).name for j in range(metadata.num_columns)]
    j = names.index("alert_date")
    latest = max(
        metadata.row_group(g).column(j).statistics.max
        for g in range(metadata.num_row_groups)
    )
    if isinstance(latest, datetime):
        latest = latest.date()
    return (latest - ALERT_EPOCH).days


def write_tile_part(task) -> Tuple[Optional[str], Dict[int, int]]:
    """Dask task: one tile's alert pixels in the window, sorted newest date then
    Hilbert, written to a part file with one row group per date. Returns the
    part's path and its point count per date (no file if the tile is empty)."""
    ys, xs, day_lo, day_hi, alerts_zarr_uri, parts_uri = task
    zarrs = worker_cached(
        ("alert_points_zarrs", alerts_zarr_uri),
        partial(open_zarrs, alerts_zarr_uri, LAYER_URIS),
    )
    pixels = read_tile_alert_pixels(zarrs, ys, xs, day_lo, day_hi)
    if pixels.empty:
        return None, {}
    pixels["hilbert"] = hilbert_index(pixels["col"], pixels["row"], HILBERT_ORDER)
    pixels = pixels.sort_values(
        ["alert_date", "hilbert"], ascending=[False, True], ignore_index=True
    )

    part_uri = f"{parts_uri}/tiles/{ys.start}_{xs.start}.parquet"
    counts = {}
    with fsspec.open(part_uri, "wb", auto_mkdir=True) as f, pq.ParquetWriter(
        f, PART_SCHEMA, write_statistics=True
    ) as writer:
        for day, day_pixels in pixels.groupby("alert_date", sort=False):
            writer.write_table(
                pa.Table.from_pandas(
                    day_pixels, schema=PART_SCHEMA, preserve_index=False
                )
            )
            counts[int(day)] = len(day_pixels)
    return part_uri, counts


def write_date_part(task) -> Tuple[int, str, int]:
    """Dask task: one date's points from every tile part that has it, in Hilbert
    order, written to a date file. Returns the date, the file and its point
    count."""
    day, tile_part_uris, parts_uri = task
    filesystem, _ = fsspec.core.url_to_fs(parts_uri)
    paths = [fsspec.core.url_to_fs(uri)[1] for uri in tile_part_uris]
    points = (
        ds.dataset(paths, schema=PART_SCHEMA, filesystem=filesystem, format="parquet")
        .to_table(filter=pc.field("alert_date") == day)
        .sort_by("hilbert")
        .select(TABLE_SCHEMA.names)
    )
    date_part_uri = f"{parts_uri}/dates/{day}.parquet"
    with fsspec.open(date_part_uri, "wb", auto_mkdir=True) as f:
        pq.write_table(points, f)
    return day, date_part_uri, points.num_rows


def write_table(
    date_part_uris: List[str], result_uri: str, metadata: Dict[str, str]
) -> None:
    """Stream the date files, in the order given (newest first), into the
    table. Each date is written on its own, so no row group spans two dates."""
    schema = TABLE_SCHEMA.with_metadata(metadata)
    with fsspec.open(result_uri, "wb", auto_mkdir=True) as f, pq.ParquetWriter(
        f, schema, write_statistics=True
    ) as writer:
        for uri in date_part_uris:
            with fsspec.open(uri, "rb") as part:
                points = pq.read_table(part)
            writer.write_table(
                points.replace_schema_metadata(metadata),
                row_group_size=ROW_GROUP_SIZE,
            )


def table_metadata(
    alerts_zarr_uri: str, version: str, latest_day: int, days_stored: int
) -> Dict[str, str]:
    """Key-value metadata for the table: the alerts version, the stored dates,
    and the alerts grid (pixel centers of the first row and column, and signed
    pixel steps), so readers get lat/lon from row/col without the zarr."""
    grid = xr.open_zarr(alerts_zarr_uri)
    x, y = grid.x.values, grid.y.values
    return {
        "alerts_version": version,
        "latest_alert_date": (ALERT_EPOCH + timedelta(days=latest_day)).isoformat(),
        "days_stored": str(days_stored),
        "grid_x0": repr(float(x[0])),
        "grid_y0": repr(float(y[0])),
        "grid_dx": repr(float((x[-1] - x[0]) / (len(x) - 1))),
        "grid_dy": repr(float((y[-1] - y[0]) / (len(y) - 1))),
    }


def build_alert_points_table(
    alerts_zarr_uri: str,
    admin_result_uri: str,
    version: str,
    result_uri: str,
    days_stored: int,
    bbox: Optional[Polygon] = None,
) -> Dict:
    """Build the alert points table for the days_stored days ending at the
    release's newest alert date, write it to result_uri, and return a summary
    (points per date, total points, file and footer sizes)."""
    latest_day = latest_alert_day(admin_result_uri)
    day_lo = latest_day - days_stored + 1
    parts_uri = result_uri.removesuffix(".parquet") + "_parts"
    tiles = alert_tiles(alerts_zarr_uri, bbox)
    print(f"alert points table: {len(tiles):,} tiles, days {day_lo}..{latest_day}")

    tile_parts: Dict[int, List[str]] = {}
    tile_tasks = [
        (ys, xs, day_lo, latest_day, alerts_zarr_uri, parts_uri) for ys, xs in tiles
    ]
    for part_uri, counts in map_tiles(write_tile_part, tile_tasks, "tiles"):
        for day in counts:
            tile_parts.setdefault(day, []).append(part_uri)

    date_tasks = [(day, uris, parts_uri) for day, uris in tile_parts.items()]
    date_parts = {
        day: (uri, n) for day, uri, n in map_tiles(write_date_part, date_tasks, "dates")
    }
    days = sorted(date_parts, reverse=True)

    metadata = table_metadata(alerts_zarr_uri, version, latest_day, days_stored)
    write_table([date_parts[day][0] for day in days], result_uri, metadata)
    _remove(parts_uri)
    return _summary(result_uri, {day: date_parts[day][1] for day in days})


def _summary(result_uri: str, points_per_day: Dict[int, int]) -> Dict:
    filesystem, path = fsspec.core.url_to_fs(result_uri)
    size = filesystem.size(path)
    with filesystem.open(path, "rb") as f:
        metadata = pq.ParquetFile(f).metadata
    summary = {
        "points_per_date": {
            (ALERT_EPOCH + timedelta(days=day)).isoformat(): n
            for day, n in points_per_day.items()
        },
        "points": metadata.num_rows,
        "row_groups": metadata.num_row_groups,
        "file_bytes": size,
        "footer_bytes": metadata.serialized_size,
    }
    print(
        f"alert points table: {summary['points']:,} points in "
        f"{summary['row_groups']:,} row groups, {size / 1e6:,.1f} MB "
        f"(footer {summary['footer_bytes'] / 1e6:,.1f} MB) -> {result_uri}"
    )
    for day, n in summary["points_per_date"].items():
        print(f"  {day}: {n:,}")
    return summary


def _remove(uri: str) -> None:
    filesystem, path = fsspec.core.url_to_fs(uri)
    if filesystem.exists(path):
        filesystem.rm(path, recursive=True)
