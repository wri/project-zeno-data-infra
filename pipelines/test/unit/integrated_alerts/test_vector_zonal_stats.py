from datetime import timedelta

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest
import shapely
import xarray as xr
from shapely.geometry import Polygon, box

from pipelines.integrated_alerts import vector_zonal_stats
from pipelines.integrated_alerts.alert_pixels import ALERT_EPOCH
from pipelines.integrated_alerts.stages import resample_to_alerts_grid
from pipelines.integrated_alerts.vector_zonal_stats import (
    FIRST_DAY,
    RESULT_SCHEMA,
    build_vector_zonal_stats,
)
from pipelines.test.unit.conftest import custom_aoi_rows
from pipelines.zonal_stats.aoi_sources import custom_aoi_source

ALERTS_RESOLUTION = 0.0001
NATURAL_LANDS_RESOLUTION = 0.00025
SIZE, CHUNK = 60, 20  # 3 x 3 chunks of the alerts grid

AOIS = {
    "across": Polygon(
        [
            (10.00031, 0.99413),
            (10.00573, 0.99617),
            (10.00488, 0.99982),
            (10.0011, 0.9989),
        ]
    ),
    "dup-a": box(10.00213, 0.99512, 10.00337, 0.99671),
    "dup-b": box(10.00213, 0.99512, 10.00337, 0.99671),
    "empty-chunk": box(10.00223, 0.99822, 10.00377, 0.99978),  # chunk with no alerts
    "outside": box(20, 20, 21, 21),
    "unmonitored": box(10.0001, 0.9941, 10.0059, 0.9999),
}


@pytest.fixture
def inputs(tmp_path, monkeypatch):
    """Write synthetic alerts, natural lands and pixel area zarrs, and make the
    adapter read them."""
    rng = np.random.default_rng(1)
    y = 1 - (np.arange(SIZE) + 0.5) * ALERTS_RESOLUTION
    x = 10 + (np.arange(SIZE) + 0.5) * ALERTS_RESOLUTION
    alert_date = rng.integers(FIRST_DAY - 30, FIRST_DAY + 30, (1, SIZE, SIZE))
    confidence = rng.integers(0, 5, (1, SIZE, SIZE))
    confidence[0, :CHUNK, slice(CHUNK, 2 * CHUNK)] = 0  # chunk (0, 1)
    alert_date[confidence == 0] = 0
    alerts = xr.Dataset(
        {
            "alert_date": (("band", "y", "x"), alert_date.astype(np.uint16)),
            "confidence": (("band", "y", "x"), confidence.astype(np.uint8)),
        },
        coords={"band": [1], "y": y, "x": x},
    )
    alerts_uri = str(tmp_path / "date_conf.zarr")
    alerts.chunk({"band": 1, "y": CHUNK, "x": CHUNK}).to_zarr(alerts_uri)

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

    pixel_area = xr.Dataset(
        {
            "band_data": (
                ("band", "y", "x"),
                rng.uniform(90, 110, (1, SIZE, SIZE)).astype(np.float32),
            )
        },
        coords={"band": [1], "y": y + 1e-12, "x": x - 1e-12},
    )
    pixel_area_path = str(tmp_path / "pixel_area.zarr")
    pixel_area.chunk({"band": 1, "y": CHUNK, "x": CHUNK}).to_zarr(pixel_area_path)

    monkeypatch.setattr(
        vector_zonal_stats,
        "LAYER_URIS",
        {"natural_lands_class": natural_lands_uri, "area_m2": pixel_area_path},
    )
    return {
        "alerts": alerts_uri,
        "natural_lands": natural_lands_uri,
        "pixel_area": pixel_area_path,
        "tmp_path": tmp_path,
    }


def sum_raster_by_aoi(inputs, aois) -> pd.DataFrame:
    """Compute the expected results from the whole grid with xarray."""
    alerts = xr.open_zarr(inputs["alerts"]).squeeze("band", drop=True)
    natural_lands = xr.open_zarr(inputs["natural_lands"])["band_data"].squeeze(
        "band", drop=True
    )
    pixel_area = xr.open_zarr(inputs["pixel_area"])["band_data"].squeeze(
        "band", drop=True
    )
    x, y = np.meshgrid(alerts.x.values, alerts.y.values)
    pixels = pd.DataFrame(
        {
            "x": x.ravel(),
            "y": y.ravel(),
            "alert_date": alerts.alert_date.values.ravel(),
            "confidence": alerts.confidence.values.ravel(),
            "natural_lands_class": resample_to_alerts_grid(natural_lands, alerts)
            .values.ravel()
            .astype(np.uint8),
            "area_ha": pixel_area.values.ravel().astype(np.float64) / 10_000,
        }
    )
    pixels = pixels[
        (pixels["alert_date"] >= FIRST_DAY) & pixels["confidence"].isin([2, 3, 4])
    ]
    sums = (
        pd.concat(
            pixels[shapely.contains_xy(polygon, pixels["x"], pixels["y"])].assign(
                aoi_id=aoi_id
            )
            for aoi_id, polygon in aois.items()
        )
        .groupby(
            ["aoi_id", "natural_lands_class", "alert_date", "confidence"],
            as_index=False,
        )["area_ha"]
        .sum()
    )
    return pd.DataFrame(
        {
            "aoi_id": sums["aoi_id"],
            "aoi_type": "custom",
            "natural_lands_class": sums["natural_lands_class"].astype(np.uint8),
            "alert_date": [
                ALERT_EPOCH + timedelta(days=int(day)) for day in sums["alert_date"]
            ],
            "alert_confidence": sums["confidence"].map(
                {2: "low", 3: "high", 4: "highest"}
            ),
            "area_ha": sums["area_ha"],
        }
    ).sort_values(
        ["aoi_id", "natural_lands_class", "alert_confidence", "alert_date"],
        ignore_index=True,
    )


def test_custom_aoi_results_match_the_raster(inputs, custom_aoi_table):
    custom_aoi_table.append(custom_aoi_rows(AOIS, not_monitored=["unmonitored"]))
    result_uri = str(inputs["tmp_path"] / "custom-integrated-alerts.parquet")

    summary = build_vector_zonal_stats(
        custom_aoi_source(), inputs["alerts"], "v20261028", result_uri
    )

    result = pq.ParquetFile(result_uri)
    table = result.read().to_pandas()
    monitored = {k: v for k, v in AOIS.items() if k != "unmonitored"}
    expected = sum_raster_by_aoi(inputs, monitored)
    assert set(expected["aoi_id"]) == {"across", "dup-a", "dup-b"}
    pd.testing.assert_frame_equal(table, expected)

    assert result.schema_arrow.remove_metadata().equals(RESULT_SCHEMA)
    metadata = {k.decode(): v.decode() for k, v in result.metadata.metadata.items()}
    assert {k: metadata[k] for k in ("alerts_version", "aoi_source")} == {
        "alerts_version": "v20261028",
        "aoi_source": "iceberg:aois/custom.custom_aois",
    }
    assert summary["aois"] == len(monitored)
    assert summary["rows"] == len(expected)


def test_bbox_run_counts_only_pixels_inside_it(inputs, custom_aoi_table):
    custom_aoi_table.append(custom_aoi_rows({"across": AOIS["across"]}))
    bbox = box(10.0015, 0.9955, 10.0045, 0.9985)  # pixel centers 15..44
    result_uri = str(inputs["tmp_path"] / "custom-bbox.parquet")

    build_vector_zonal_stats(
        custom_aoi_source(), inputs["alerts"], "v20261028", result_uri, bbox
    )

    table = pq.read_table(result_uri).to_pandas()
    expected = sum_raster_by_aoi(inputs, {"across": AOIS["across"] & bbox})
    pd.testing.assert_frame_equal(table, expected)
