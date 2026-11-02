import dask.array as da
import numpy as np
import pytest
import xarray as xr
import zarr

from pipelines.integrated_alerts.stages import resample_to_alerts_grid
from pipelines.prefect_flows.tile_stages import (
    chunk_edges,
    hilbert_index,
    map_tiles,
    sample_nearest,
    tile_windows,
    worker_cached,
    written_chunks,
)

# Real grid spacings: SBTN natural lands is 30m, integrated alerts is 10m.
NATURAL_LANDS_RESOLUTION = 0.00025
ALERTS_RESOLUTION = 0.0001


@pytest.mark.parametrize("order", [1, 3, 4])
def test_hilbert_index_visits_every_cell_once_in_adjacent_steps(order):
    n = 1 << order
    x, y = (a.ravel() for a in np.meshgrid(np.arange(n), np.arange(n)))
    d = hilbert_index(x, y, order)

    assert sorted(d) == list(range(n * n))
    path = np.argsort(d)
    steps = np.abs(np.diff(x[path])) + np.abs(np.diff(y[path]))
    assert (steps == 1).all()


def test_chunk_edges_coalesce_chunks_up_to_the_target():
    assert chunk_edges([4, 4, 4, 2], 4) == [0, 4, 8, 12, 14]
    assert chunk_edges([4, 4, 4, 2], 8) == [0, 8, 14]


def test_tile_windows_follow_chunks_with_pixel_edge_bounds():
    x = 10 + (np.arange(6) + 0.5) * ALERTS_RESOLUTION
    y = 1 - (np.arange(4) + 0.5) * ALERTS_RESOLUTION
    grid = xr.DataArray(
        da.zeros((4, 6), chunks=(2, 3)), dims=("y", "x"), coords={"y": y, "x": x}
    )

    windows = list(tile_windows(grid, tile_px=2))

    assert [(ys, xs) for ys, xs, _ in windows] == [
        (slice(0, 2), slice(0, 3)),
        (slice(0, 2), slice(3, 6)),
        (slice(2, 4), slice(0, 3)),
        (slice(2, 4), slice(3, 6)),
    ]
    west, south, east, north = windows[0][2]
    assert (west, east) == pytest.approx((10.0, 10.0003))
    assert (south, north) == pytest.approx((0.9998, 1.0))


def test_written_chunks_skips_chunks_never_written(tmp_path):
    values = np.zeros((1, 40, 40), np.uint16)
    values[0, 5, 5] = 4300
    values[0, 25, 30] = 4301
    xr.Dataset(
        {"alert_date": (("band", "y", "x"), values)},
        coords={
            "band": [1],
            "y": 1 - (np.arange(40) + 0.5) * ALERTS_RESOLUTION,
            "x": 10 + (np.arange(40) + 0.5) * ALERTS_RESOLUTION,
        },
    ).chunk({"band": 1, "y": 20, "x": 20}).to_zarr(tmp_path / "alerts.zarr")

    assert written_chunks(str(tmp_path / "alerts.zarr"), "alert_date") == {
        (0, 0),
        (1, 1),
    }


@pytest.mark.parametrize(
    "chunk_key_encoding",
    [{"name": "default", "separator": "/"}, {"name": "v2", "separator": "."}],
)
def test_written_chunks_with_any_chunk_key_layout(tmp_path, chunk_key_encoding):
    array = zarr.create_array(
        str(tmp_path / "alerts.zarr"),
        name="alert_date",
        shape=(1, 4, 4),
        chunks=(1, 2, 2),
        dtype="uint16",
        fill_value=0,
        chunk_key_encoding=chunk_key_encoding,
    )
    array[0, 0, 0] = 4300
    array[0, 3, 1] = 4301

    assert written_chunks(str(tmp_path / "alerts.zarr"), "alert_date") == {
        (0, 0),
        (1, 0),
    }


def test_written_chunks_refuses_sharded_arrays(tmp_path):
    zarr.create_array(
        str(tmp_path / "alerts.zarr"),
        name="alert_date",
        shape=(1, 4, 4),
        chunks=(1, 2, 2),
        shards=(1, 4, 4),
        dtype="uint16",
        fill_value=0,
    )

    with pytest.raises(ValueError, match="sharded"):
        written_chunks(str(tmp_path / "alerts.zarr"), "alert_date")


def _grid(n_pixels, resolution):
    """Pixel centers, starting at a whole degree like the real tiled grids."""
    return np.arange(n_pixels) * resolution + resolution / 2


def test_sample_nearest_matches_resample_to_alerts_grid():
    """Sampling at points agrees with resampling the whole layer to the alerts
    grid, including points beyond the layer's extent (0)."""
    x30 = _grid(4, NATURAL_LANDS_RESOLUTION)
    layer = xr.DataArray(
        np.arange(1, 17, dtype=np.uint8).reshape(4, 4),
        dims=("y", "x"),
        coords={"y": -x30, "x": x30},
    )
    x10 = _grid(12, ALERTS_RESOLUTION)
    alerts = xr.DataArray(
        np.zeros((12, 12), dtype=np.uint8),
        dims=("y", "x"),
        coords={"y": -x10, "x": x10},
    )
    expected = resample_to_alerts_grid(layer, alerts).values
    rows, cols = (a.ravel() for a in np.meshgrid(np.arange(12), np.arange(12)))

    sampled = sample_nearest(
        layer, x10[cols], -x10[rows], tolerance=NATURAL_LANDS_RESOLUTION / 2
    )

    assert sampled.tolist() == expected[rows, cols].tolist()
    assert sampled.dtype == np.uint8
    assert (expected == 0).any()


def test_sample_nearest_with_no_points_or_outside_the_layer():
    x = _grid(2, NATURAL_LANDS_RESOLUTION)
    layer = xr.DataArray(
        np.ones((2, 2), dtype=np.uint8), dims=("y", "x"), coords={"y": -x, "x": x}
    )
    empty = np.array([], dtype=float)
    assert sample_nearest(layer, empty, empty, 1e-4).tolist() == []
    far = sample_nearest(layer, np.array([5.0]), np.array([5.0]), 1e-4)
    assert far.tolist() == [0]


def test_worker_cached_off_a_worker_opens_every_time():
    opened = []
    assert worker_cached("key", lambda: opened.append(1) or "value") == "value"
    assert worker_cached("key", lambda: opened.append(1) or "value") == "value"
    assert len(opened) == 2


def _square(n):
    return n * n


def _fail_on_three(n):
    if n == 3:
        raise ValueError("three")
    return n


def test_map_tiles_yields_every_result():
    assert sorted(map_tiles(_square, [1, 2, 3, 4])) == [1, 4, 9, 16]
    assert list(map_tiles(_square, [])) == []


def test_map_tiles_fails_after_the_rest_finish():
    results = []
    with pytest.raises(RuntimeError, match="1 tiles failed"):
        for result in map_tiles(_fail_on_three, [1, 2, 3, 4]):
            results.append(result)
    assert sorted(results) == [1, 2, 4]
