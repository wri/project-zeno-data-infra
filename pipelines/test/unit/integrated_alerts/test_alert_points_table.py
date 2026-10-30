from datetime import date, datetime

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import xarray as xr
from shapely.geometry import box

from pipelines.integrated_alerts import alert_points_table
from pipelines.integrated_alerts.alert_pixels import alert_tiles
from pipelines.integrated_alerts.alert_points_table import (
    HILBERT_ORDER,
    TABLE_SCHEMA,
    build_alert_points_table,
    latest_alert_day,
)
from pipelines.integrated_alerts.stages import resample_to_alerts_grid
from pipelines.prefect_flows.common_stages import clip_ds_to_bbox
from pipelines.prefect_flows.tile_stages import hilbert_index

ALERTS_RESOLUTION = 0.0001
NATURAL_LANDS_RESOLUTION = 0.00025
LATEST_DAY = 4307  # 2026-10-16
SIZE, CHUNK = 60, 20  # 3 x 3 chunks of the alerts grid
DAYS_STORED = 4


@pytest.fixture
def inputs(tmp_path):
    """A synthetic alerts zarr (band, y, x) with one chunk left empty, a coarser
    natural lands zarr and an admin results parquet, on disk."""
    rng = np.random.default_rng(0)
    alert_date = rng.integers(LATEST_DAY - 8, LATEST_DAY + 1, (1, SIZE, SIZE))
    confidence = rng.integers(0, 5, (1, SIZE, SIZE))  # 0 = no alert, 1 = unused
    empty_chunk = (0, slice(0, CHUNK), slice(CHUNK, 2 * CHUNK))  # chunk (0, 1)
    alert_date[empty_chunk] = 0
    confidence[empty_chunk] = 0
    alert_date[confidence == 0] = 0
    alerts = xr.Dataset(
        {
            "alert_date": (("band", "y", "x"), alert_date.astype(np.uint16)),
            "confidence": (("band", "y", "x"), confidence.astype(np.uint8)),
        },
        coords={
            "band": [1],
            "y": 1 - (np.arange(SIZE) + 0.5) * ALERTS_RESOLUTION,
            "x": 10 + (np.arange(SIZE) + 0.5) * ALERTS_RESOLUTION,
        },
    )
    alerts_uri = str(tmp_path / "date_conf.zarr")
    alerts.chunk({"band": 1, "y": CHUNK, "x": CHUNK}).to_zarr(alerts_uri)

    # 30 m natural lands covering most, not all, of the alerts extent.
    n = int(SIZE * ALERTS_RESOLUTION / NATURAL_LANDS_RESOLUTION) - 2
    natural_lands = xr.Dataset(
        {
            "band_data": (
                ("band", "y", "x"),
                rng.integers(1, 22, (1, n, n)).astype(np.uint8),
            )
        },
        coords={
            "band": [1],
            "y": 1 - (np.arange(n) + 0.5) * NATURAL_LANDS_RESOLUTION,
            "x": 10 + (np.arange(n) + 0.5) * NATURAL_LANDS_RESOLUTION,
        },
    )
    natural_lands_uri = str(tmp_path / "natural_lands.zarr")
    natural_lands.chunk({"band": 1, "y": 8, "x": 8}).to_zarr(natural_lands_uri)

    admin_uri = str(tmp_path / "admin-integrated-alerts.parquet")
    pq.write_table(
        pa.table(
            {
                "aoi_id": ["BRA", "BRA", "IDN"],
                "alert_date": [date(2026, 10, 1), date(2026, 10, 16), date(2026, 9, 3)],
            }
        ),
        admin_uri,
        row_group_size=1,
    )
    return {
        "alerts": alerts_uri,
        "natural_lands": natural_lands_uri,
        "admin": admin_uri,
        "tmp_path": tmp_path,
    }


@pytest.fixture(autouse=True)
def small_row_groups(monkeypatch):
    # Small row groups so every date spans several of them.
    monkeypatch.setattr(alert_points_table, "ROW_GROUP_SIZE", 7)


@pytest.fixture
def natural_lands_uri(inputs, monkeypatch):
    monkeypatch.setattr(
        alert_points_table,
        "LAYER_URIS",
        {"natural_lands_class": inputs["natural_lands"]},
    )
    return inputs["natural_lands"]


def raster_scan(inputs, bbox=None):
    """The expected points, from the zarrs with xarray: the window and
    confidence filter on the alerts grid, and natural lands resampled to it."""
    alerts = xr.open_zarr(inputs["alerts"]).squeeze("band", drop=True)
    natural_lands = xr.open_zarr(inputs["natural_lands"])["band_data"].squeeze(
        "band", drop=True
    )
    classes = resample_to_alerts_grid(natural_lands, alerts).values
    alert_date, confidence = alerts.alert_date.values, alerts.confidence.values
    keep = (
        (alert_date > LATEST_DAY - DAYS_STORED)
        & (alert_date <= LATEST_DAY)
        & np.isin(confidence, [2, 3, 4])
    )
    if bbox is not None:
        clipped = clip_ds_to_bbox(alerts, bbox)
        inside = np.zeros_like(keep)
        rows = np.flatnonzero(np.isin(alerts.y.values, clipped.y.values))
        cols = np.flatnonzero(np.isin(alerts.x.values, clipped.x.values))
        inside[np.ix_(rows, cols)] = True
        keep &= inside
    rows, cols = np.nonzero(keep)
    return pd.DataFrame(
        {
            "row": rows,
            "col": cols,
            "alert_date": alert_date[rows, cols],
            "confidence": confidence[rows, cols],
            "natural_lands_class": classes[rows, cols],
        }
    )


def _same_points(table: pd.DataFrame, expected: pd.DataFrame) -> bool:
    columns = TABLE_SCHEMA.names
    a = table[columns].sort_values(columns, ignore_index=True)
    b = expected[columns].sort_values(columns, ignore_index=True)
    return a.astype("int64").equals(b.astype("int64"))


def test_latest_alert_day_reads_alert_date_statistics(inputs):
    assert latest_alert_day(inputs["admin"]) == LATEST_DAY


def test_latest_alert_day_accepts_timestamps(tmp_path):
    path = str(tmp_path / "admin.parquet")
    pq.write_table(
        pa.table({"alert_date": [datetime(2026, 10, 16), datetime(2026, 10, 2)]}),
        path,
    )
    assert latest_alert_day(path) == LATEST_DAY


def test_alert_tiles_are_the_chunks_with_data(inputs):
    tiles = alert_tiles(inputs["alerts"])
    assert len(tiles) == 8
    assert (slice(0, CHUNK), slice(CHUNK, 2 * CHUNK)) not in tiles


def test_alert_tiles_narrow_to_a_bbox(inputs):
    bbox = box(10.0015, 0.9955, 10.0045, 0.9985)  # pixel centers 15..44 of each axis
    tiles = alert_tiles(inputs["alerts"], bbox)
    rows = sorted({(ys.start, ys.stop) for ys, _ in tiles})
    cols = sorted({(xs.start, xs.stop) for _, xs in tiles})
    assert rows == [(15, 20), (20, 40), (40, 45)]
    assert cols == [(15, 20), (20, 40), (40, 45)]
    assert (slice(15, 20), slice(20, 40)) not in tiles  # the empty chunk


def test_table_has_the_raster_points_in_the_readers_layout(inputs, natural_lands_uri):
    result_uri = str(inputs["tmp_path"] / "by-date.parquet")

    summary = build_alert_points_table(
        inputs["alerts"], inputs["admin"], "v20261018", result_uri, DAYS_STORED
    )

    parquet = pq.ParquetFile(result_uri)
    table = parquet.read().to_pandas()
    expected = raster_scan(inputs)
    assert len(expected) > 0
    assert _same_points(table, expected)

    # schema and key-value metadata
    assert parquet.schema_arrow.remove_metadata().equals(TABLE_SCHEMA)
    metadata = {k.decode(): v.decode() for k, v in parquet.metadata.metadata.items()}
    assert {k: metadata[k] for k in ("alerts_version", "latest_alert_date")} == {
        "alerts_version": "v20261018",
        "latest_alert_date": "2026-10-16",
    }
    assert metadata["days_stored"] == str(DAYS_STORED)
    assert float(metadata["grid_x0"]) == pytest.approx(10.00005)
    assert float(metadata["grid_y0"]) == pytest.approx(0.99995)
    assert float(metadata["grid_dx"]) == pytest.approx(ALERTS_RESOLUTION)
    assert float(metadata["grid_dy"]) == pytest.approx(-ALERTS_RESOLUTION)

    # newest date first, Hilbert order within a date
    dates = table["alert_date"].to_numpy()
    assert (np.diff(dates) <= 0).all()
    hilbert = hilbert_index(table["col"], table["row"], HILBERT_ORDER)
    for day in np.unique(dates):
        assert (np.diff(hilbert[dates == day]) > 0).all()

    # no row group spans two dates; row groups no bigger than ROW_GROUP_SIZE;
    # statistics for the reader's row group selection
    meta = parquet.metadata
    names = [meta.schema.column(j).name for j in range(meta.num_columns)]
    for g in range(meta.num_row_groups):
        row_group = meta.row_group(g)
        assert row_group.num_rows <= 7
        stats = {name: row_group.column(j).statistics for j, name in enumerate(names)}
        assert stats["alert_date"].min == stats["alert_date"].max
        assert stats["row"].has_min_max and stats["col"].has_min_max

    assert summary["points"] == len(expected)
    assert sum(summary["points_per_date"].values()) == len(expected)
    assert not (inputs["tmp_path"] / "by-date_parts").exists()


def test_bbox_table_has_the_raster_points_inside_it(inputs, natural_lands_uri):
    bbox = box(10.0015, 0.9955, 10.0045, 0.9985)
    result_uri = str(inputs["tmp_path"] / "bbox.parquet")

    build_alert_points_table(
        inputs["alerts"], inputs["admin"], "v20261018", result_uri, DAYS_STORED, bbox
    )

    table = pq.read_table(result_uri).to_pandas()
    expected = raster_scan(inputs, bbox)
    assert 0 < len(expected) < len(raster_scan(inputs))
    assert _same_points(table, expected)


def test_window_with_no_alerts_writes_an_empty_table(inputs, natural_lands_uri):
    far_bbox = box(20, 20, 21, 21)
    result_uri = str(inputs["tmp_path"] / "empty.parquet")

    summary = build_alert_points_table(
        inputs["alerts"],
        inputs["admin"],
        "v20261018",
        result_uri,
        DAYS_STORED,
        far_bbox,
    )

    parquet = pq.ParquetFile(result_uri)
    assert parquet.metadata.num_rows == 0
    assert parquet.schema_arrow.remove_metadata().equals(TABLE_SCHEMA)
    assert summary["points"] == 0
