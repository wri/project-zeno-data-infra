from dataclasses import dataclass
from typing import Dict

import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import shapely
from shapely.geometry import Polygon, box

from pipelines.zonal_stats import pixels_in_polygons
from pipelines.zonal_stats.pixels_in_polygons import (
    TileTask,
    sum_pixels_in_polygons,
    tile_sums,
    zonal_stats,
)

# A 12 x 12 grid of unit pixels from (0, 0), in 4 x 4 pixel tiles. Pixel (row,
# col) has its center at (col + 0.5, 11.5 - row), group g = (row + col) % 3 and
# value row * 12 + col.
SIZE, TILE = 12, 4
SCHEMA = pa.schema([("aoi_id", pa.string()), ("g", pa.int64()), ("v", pa.float64())])


def grid_pixels(ys: slice, xs: slice) -> pd.DataFrame:
    rows, cols = np.meshgrid(
        np.arange(ys.start, ys.stop), np.arange(xs.start, xs.stop), indexing="ij"
    )
    rows, cols = rows.ravel(), cols.ravel()
    return pd.DataFrame(
        {
            "x": cols + 0.5,
            "y": SIZE - rows - 0.5,
            "g": (rows + cols) % 3,
            "v": (rows * SIZE + cols).astype(np.float64),
        }
    )


def grid_tiles() -> list:
    return [
        (
            slice(r, r + TILE),
            slice(c, c + TILE),
            (c, SIZE - r - TILE, c + TILE, SIZE - r),
        )
        for r in range(0, SIZE, TILE)
        for c in range(0, SIZE, TILE)
    ]


@dataclass(frozen=True)
class FakeSource:
    """An AOI source over AOI polygons held in memory."""

    aois: Dict[str, Polygon]
    aoi_type: str = "test"

    def bounds(self) -> pd.DataFrame:
        bounds = shapely.bounds(list(self.aois.values()))
        return pd.DataFrame(
            {
                "aoi_id": list(self.aois),
                **dict(zip(["xmin", "ymin", "xmax", "ymax"], bounds.T)),
            }
        )

    def read(self, bounds) -> gpd.GeoDataFrame:
        aois = gpd.GeoDataFrame(
            {"aoi_id": list(self.aois)},
            geometry=list(self.aois.values()),
            crs="EPSG:4326",
        )
        return aois[aois.intersects(box(*bounds))].reset_index(drop=True)

    def provenance(self) -> Dict[str, str]:
        return {"aoi_source": "memory"}


def first_tile_task(aoi_source: FakeSource, part_uri: str) -> TileTask:
    """The task of the top-left tile: rows 0-3, cols 0-3, x 0-4, y 8-12."""
    ys, xs, bounds = grid_tiles()[0]
    return TileTask(ys, xs, bounds, aoi_source, grid_pixels, ["g"], "v", part_uri)


def brute_force(aois: Dict[str, Polygon]) -> pd.DataFrame:
    """Every pixel center of the whole grid tested against every polygon."""
    pixels = grid_pixels(slice(0, SIZE), slice(0, SIZE))
    covered = [
        pixels[shapely.contains_xy(polygon, pixels["x"], pixels["y"])].assign(
            aoi_id=aoi_id
        )
        for aoi_id, polygon in aois.items()
    ]
    return (
        pd.concat(covered)
        .groupby(["aoi_id", "g"], as_index=False)["v"]
        .sum()
        .sort_values(["aoi_id", "g"], ignore_index=True)
    )


def test_sum_pixels_in_polygons_matches_brute_force():
    rng = np.random.default_rng(0)
    pixels = pd.DataFrame(
        {
            "x": rng.uniform(0, 10, 2_000),
            "y": rng.uniform(0, 10, 2_000),
            "g": rng.integers(0, 4, 2_000),
            "v": rng.uniform(0, 1, 2_000),
        }
    )
    centers = rng.uniform(1, 9, (20, 2))
    polygons = [
        shapely.Point(x, y).buffer(r)
        for (x, y), r in zip(centers, rng.uniform(0.2, 3, 20))
    ] + [
        box(20, 20, 21, 21)
    ]  # covers no pixel

    sums = sum_pixels_in_polygons(pixels, polygons, ["g"], "v")

    expected = (
        pd.concat(
            pixels[shapely.contains_xy(polygon, pixels["x"], pixels["y"])].assign(
                polygon=position
            )
            for position, polygon in enumerate(polygons)
        )
        .groupby(["polygon", "g"], as_index=False)["v"]
        .sum()
    )
    pd.testing.assert_frame_equal(sums, expected)


def test_sum_pixels_in_polygons_with_no_pixels_covered():
    pixels = grid_pixels(slice(0, 2), slice(0, 2))

    sums = sum_pixels_in_polygons(pixels, [box(20, 20, 21, 21)], ["g"], "v")

    assert sums.empty
    assert list(sums.columns) == ["polygon", "g", "v"]


def test_tile_sums_reports_identical_polygons_for_each_aoi(tmp_path):
    aoi_source = FakeSource(
        {
            "a": box(0, 8, 3, 12),
            "b": box(0, 8, 3, 12),  # same polygon, listed twice
            "c": box(2, 6, 9, 10),  # beyond the tile: clipped to it
        }
    )
    tile = first_tile_task(aoi_source, str(tmp_path / "part.parquet"))

    assert tile_sums(tile) == tile.part_uri

    part = pd.read_parquet(tile.part_uri)
    expected = brute_force(
        {
            aoi_id: polygon & box(*tile.bounds)
            for aoi_id, polygon in aoi_source.aois.items()
        }
    )
    pd.testing.assert_frame_equal(part, expected, check_dtype=False)
    assert part["aoi_id"].is_monotonic_increasing


def test_tile_sums_without_aois_or_pixels_writes_nothing(tmp_path):
    part_uri = str(tmp_path / "part.parquet")
    elsewhere = FakeSource({"a": box(8, 0, 12, 4)})
    touching = FakeSource({"a": box(4, 8, 6, 12)})  # shares only the tile's edge

    assert tile_sums(first_tile_task(elsewhere, part_uri)) is None
    assert tile_sums(first_tile_task(touching, part_uri)) is None
    assert not (tmp_path / "part.parquet").exists()


@pytest.mark.parametrize("aois_per_bucket", [1, 2, 20_000])
def test_zonal_stats_adds_up_aois_across_tiles(tmp_path, monkeypatch, aois_per_bucket):
    monkeypatch.setattr(pixels_in_polygons, "AOIS_PER_BUCKET", aois_per_bucket)
    monkeypatch.setattr(pixels_in_polygons, "ROW_GROUP_SIZE", 5)
    aois = {
        "across": Polygon([(1, 1.2), (11, 2.3), (10.1, 11), (3, 9.2)]),  # 9 tiles
        "corner": box(0, 0, 2.2, 2.2),
        "dup": box(5, 5, 7.6, 7),
        "dup-2": box(5, 5, 7.6, 7),
        "no-pixels": box(6.1, 6.1, 6.4, 6.4),  # between pixel centers
        "outside": box(30, 30, 31, 31),
    }
    result_uri = str(tmp_path / "result.parquet")

    summary = zonal_stats(
        FakeSource(aois),
        grid_tiles(),
        read_pixels=grid_pixels,
        by=["g"],
        sum_column="v",
        finish=lambda sums: sums.sort_values(["aoi_id", "g"]),
        schema=SCHEMA,
        result_uri=result_uri,
        metadata={"dataset_version": "v1"},
    )

    result = pq.ParquetFile(result_uri)
    pd.testing.assert_frame_equal(
        result.read().to_pandas(), brute_force(aois), check_dtype=False
    )
    assert result.schema_arrow.remove_metadata().equals(SCHEMA)
    assert result.schema_arrow.metadata == {
        b"dataset_version": b"v1",
        b"aoi_source": b"memory",
    }
    assert summary["aois"] == len(aois)
    assert summary["tiles"] == 9  # "outside" touches no tile
    assert summary["rows"] == len(brute_force(aois))
    assert not (tmp_path / "result_parts").exists()


def test_zonal_stats_without_aois_writes_an_empty_result(tmp_path):
    result_uri = str(tmp_path / "result.parquet")

    summary = zonal_stats(
        FakeSource({"outside": box(30, 30, 31, 31)}),
        grid_tiles(),
        read_pixels=grid_pixels,
        by=["g"],
        sum_column="v",
        finish=lambda sums: sums,
        schema=SCHEMA,
        result_uri=result_uri,
        metadata={},
    )

    result = pq.read_table(result_uri)
    assert result.num_rows == 0
    assert result.schema.remove_metadata().equals(SCHEMA)
    assert summary["tiles"] == 0
