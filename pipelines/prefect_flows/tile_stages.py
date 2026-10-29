"""Per-tile computation on a Dask cluster: tile windows on a zarr's chunk grid,
objects cached per worker, sampling coarser layers at points, and running one
task per tile."""

from contextlib import contextmanager
from math import ceil
from typing import Callable, Hashable, Iterable, Iterator, List, Set, Tuple

import fsspec
import numpy as np
import xarray as xr
import zarr
from dask.distributed import Client, LocalCluster, as_completed, get_client

Bounds = Tuple[float, float, float, float]


def chunk_edges(chunks: Iterable[int], target: int) -> List[int]:
    """Chunk boundaries coalesced into tile edges of at least `target` pixels."""
    bounds = np.concatenate([[0], np.cumsum(list(chunks))])
    edges = [0]
    for b in bounds[1:]:
        if b - edges[-1] >= target or b == bounds[-1]:
            edges.append(int(b))
    return edges


def window_bounds(grid: xr.DataArray, ys: slice, xs: slice) -> Bounds:
    """(west, south, east, north) of the pixels at rows ys and columns xs of
    grid, at the pixel edges, from the pixel-center coordinates."""
    x, y = grid.x.values, grid.y.values
    dx, dy = abs(float(x[1] - x[0])), abs(float(y[1] - y[0]))
    top, bottom = y[ys.start], y[ys.stop - 1]
    return (
        float(x[xs.start]) - dx / 2,
        float(min(top, bottom)) - dy / 2,
        float(x[xs.stop - 1]) + dx / 2,
        float(max(top, bottom)) + dy / 2,
    )


def tile_windows(
    grid: xr.DataArray, tile_px: int
) -> Iterator[Tuple[slice, slice, Bounds]]:
    """(y slice, x slice, bounds) of each tile of grid, snapped to its dask chunk
    boundaries. Bounds are as window_bounds gives them."""
    y_edges = chunk_edges(grid.chunks[grid.get_axis_num("y")], tile_px)
    x_edges = chunk_edges(grid.chunks[grid.get_axis_num("x")], tile_px)
    for y0, y1 in zip(y_edges[:-1], y_edges[1:]):
        for x0, x1 in zip(x_edges[:-1], x_edges[1:]):
            ys, xs = slice(y0, y1), slice(x0, x1)
            yield ys, xs, window_bounds(grid, ys, xs)


def written_chunks(zarr_uri: str, variable: str) -> Set[Tuple[int, int]]:
    """(y, x) chunk indices of a (..., y, x) zarr variable's chunks that were
    written, where any leading dimensions have a single chunk. zarr does not
    write chunks holding only the fill value, so these are the chunks with
    data. Each chunk's key comes from zarr's own chunk key encoding, so any key
    layout works."""
    array = zarr.open_array(zarr_uri, path=variable, mode="r")
    if array.shards is not None:
        raise ValueError(f"{zarr_uri}/{variable} is sharded; keys would name shards")
    fs, root = fsspec.core.url_to_fs(zarr_uri)
    array_root = f"{root.rstrip('/')}/{variable}/"
    stored = {key.removeprefix(array_root) for key in fs.find(array_root)}
    encode = array.metadata.chunk_key_encoding.encode_chunk_key
    leading = (0,) * (array.ndim - 2)
    rows, cols = (ceil(n / c) for n, c in zip(array.shape[-2:], array.chunks[-2:]))
    return {
        (i, j)
        for i in range(rows)
        for j in range(cols)
        if encode(leading + (i, j)) in stored
    }


def worker_cached(key: Hashable, opener: Callable):
    """opener() cached on the Dask worker object, so expensive objects such as
    opened zarrs are created once per worker. The cache lives on the worker,
    never in a module global: a global would be shipped with every task.
    Off a worker (client, tests), opener() runs uncached."""
    try:
        from distributed import get_worker

        cache = get_worker().__dict__.setdefault("_tile_stages_cache", {})
    except ValueError:
        return opener()
    if key not in cache:
        cache[key] = opener()
    return cache[key]


def sample_nearest(
    layer: xr.DataArray,
    x: np.ndarray,
    y: np.ndarray,
    tolerance: float,
    fill_value=0,
) -> np.ndarray:
    """Values of a 2D (y, x) layer at points: each point takes the layer pixel
    whose center is nearest, within tolerance, else fill_value, as
    reindex_like(method="nearest") does. Pointwise indexing reads only the
    layer's chunks that hold points; open the layer without dask (chunks=None)
    inside a task, so this is direct I/O rather than a nested dask graph."""
    xi = layer.get_index("x").get_indexer(x, method="nearest", tolerance=tolerance)
    yi = layer.get_index("y").get_indexer(y, method="nearest", tolerance=tolerance)
    found = (xi >= 0) & (yi >= 0)
    values = np.full(len(x), fill_value, dtype=layer.dtype)
    if found.any():
        values[found] = layer.isel(
            x=xr.DataArray(xi[found], dims="point"),
            y=xr.DataArray(yi[found], dims="point"),
        ).values
    return values


@contextmanager
def _dask_client() -> Iterator[Client]:
    """The current Dask client (e.g. the Coiled cluster's), or an in-process
    local cluster for local runs and tests."""
    try:
        client = get_client()
    except ValueError:
        client = None
    if client is not None:
        yield client
        return
    with LocalCluster(processes=False, dashboard_address=None) as cluster:
        with Client(cluster) as local_client:
            yield local_client


def map_tiles(fn: Callable, tasks: list, label: str = "tiles") -> Iterator:
    """fn(task) for every task, one Dask task each, yielding results as they
    finish, in batches. Tasks should carry only small descriptions (windows,
    paths), not data. Raises once all tasks are done if any failed, so callers
    never complete a partial result."""
    if not tasks:
        return
    with _dask_client() as client:
        futures = client.map(fn, tasks, pure=False)
        failed, done = [], 0
        for batch in as_completed(
            futures, with_results=True, raise_errors=False
        ).batches():
            for future, result in batch:
                if future.status == "error":
                    failed.append((future.key, repr(future.exception())))
                else:
                    yield result
            done += len(batch)
            if done % 500 < len(batch) or done == len(futures):
                print(f"{label}: {done:,}/{len(futures):,} done, {len(failed)} failed")
        if failed:
            raise RuntimeError(f"{len(failed)} {label} failed, e.g. {failed[:3]}")


def hilbert_index(x: np.ndarray, y: np.ndarray, order: int) -> np.ndarray:
    """Distance along a Hilbert curve over a 2**order x 2**order grid, for
    integer cell coordinates (x, y)."""
    x, y = np.asarray(x, dtype=np.int64), np.asarray(y, dtype=np.int64)
    n, d, s = 1 << order, np.zeros(len(x), dtype=np.int64), 1 << (order - 1)
    while s > 0:
        rx, ry = (x & s) > 0, (y & s) > 0
        d += s * s * ((3 * rx) ^ ry)
        flip = ~ry & rx
        x, y = np.where(flip, n - 1 - x, x), np.where(flip, n - 1 - y, y)
        x, y = np.where(~ry, y, x), np.where(~ry, x, y)  # swap when ry == 0
        s >>= 1
    return d
