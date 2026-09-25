from unittest.mock import patch

import dask.array as da
import numpy as np
import pytest
import xarray as xr

from pipelines import globals as zarr_uris
from pipelines.disturbance import stages
from pipelines.prefect_flows import common_stages

DIST_ZARR_URI = "s3://dummy_zarr_uri"
SIZE, CHUNK = 40, 10
COORDS = {
    "y": ("y", np.linspace(10.0, 9.99025, SIZE)),
    "x": ("x", np.linspace(-60.0, -59.99025, SIZE)),
}
CONTEXTUAL_GROUPS = {
    None: (),
    "natural_land_class": (np.arange(22),),
    "driver": (np.arange(5),),
    "grasslands": ([0, 1],),
    "land_cover": (np.arange(9),),
}


def _grid(rng, low, high, dims=("band", "y", "x"), years=None, dtype=np.uint16):
    shape = {"band": 1, "year": len(years or []), "y": SIZE, "x": SIZE}
    data = rng.integers(low, high, [shape[d] for d in dims]).astype(dtype)
    chunks = [CHUNK if d in ("y", "x") else -1 for d in dims]
    coords = dict(COORDS, **({"year": ("year", years)} if years else {}))
    return xr.DataArray(da.from_array(data, chunks=chunks), dims=dims, coords=coords)


@pytest.fixture
def synthetic_zarrs():
    rng = np.random.default_rng(0)
    alert_date = _grid(rng, 731, 760)
    # about half the pixels have no alert
    alert_date = alert_date.where(_grid(rng, 0, 2) == 1, 0).astype(np.uint16)
    dist = xr.Dataset(
        {"alert_date": alert_date, "confidence": _grid(rng, 2, 4, dtype=np.uint8)}
    )
    area = xr.DataArray(
        da.from_array(rng.random((1, SIZE, SIZE)) * 800, chunks=(-1, CHUNK, CHUNK)),
        dims=("band", "y", "x"),
        coords=COORDS,
    )
    band = lambda array: xr.Dataset({"band_data": array})  # noqa: E731
    return {
        DIST_ZARR_URI: dist,
        zarr_uris.country_zarr_uri: band(_grid(rng, 76, 78)),
        zarr_uris.region_zarr_uri: band(_grid(rng, 1, 5, dtype=np.uint8)),
        zarr_uris.subregion_zarr_uri: band(_grid(rng, 1, 9)),
        zarr_uris.pixel_area_zarr_uri: band(area),
        zarr_uris.sbtn_natural_lands_zarr_uri: band(_grid(rng, 2, 22, dtype=np.uint8)),
        zarr_uris.dist_driver_zarr_uri: band(
            _grid(rng, 0, 5, dims=("y", "x"), dtype=np.uint8)
        ),
        zarr_uris.grasslands_zarr_uri: band(
            _grid(rng, 0, 2, ("year", "y", "x"), [2021, 2022], np.uint8)
        ),
        zarr_uris.land_cover_zarr_uri: band(
            _grid(rng, 0, 9, ("year", "y", "x"), [2023, 2024], np.uint8)
        ),
    }


def _full_grid_inputs(contextual_name):
    datasets = stages.load_data(DIST_ZARR_URI)
    if contextual_name is None:
        return datasets + (None,)
    uri, year = stages.CONTEXTUAL_LAYERS[contextual_name]
    layer = stages._align_to(datasets[0], uri)
    return datasets + (layer if year is None else layer.sel(year=year),)


def _reduce(datasets, contextual_name):
    expected_groups = (
        (np.arange(999), np.arange(86), np.arange(854))
        + CONTEXTUAL_GROUPS[contextual_name]
        + (np.arange(731, 3288), [1, 2, 3])
    )
    compute_input = stages.setup_compute(datasets, expected_groups, contextual_name)
    df = stages.create_result_dataframe(
        common_stages.compute(*compute_input, funcname="sum")
    )
    return df.sort_values(list(df.columns.drop("area_ha"))).reset_index(drop=True)


@pytest.mark.integration
@pytest.mark.parametrize("contextual_name", list(CONTEXTUAL_GROUPS))
def test_alert_pixel_reduction_matches_full_grid(synthetic_zarrs, contextual_name):
    with patch.object(stages, "_load_zarr", side_effect=synthetic_zarrs.__getitem__):
        expected = _reduce(_full_grid_inputs(contextual_name), contextual_name)
        actual = _reduce(
            stages.alert_pixel_inputs(DIST_ZARR_URI, contextual_name), contextual_name
        )

    assert len(expected) > 0
    assert actual.drop(columns="area_ha").equals(expected.drop(columns="area_ha"))
    np.testing.assert_allclose(actual.area_ha, expected.area_ha, rtol=1e-12)


@pytest.mark.integration
def test_alert_pixels_exclude_pixels_without_alerts(synthetic_zarrs):
    with patch.object(stages, "_load_zarr", side_effect=synthetic_zarrs.__getitem__):
        pixels = stages.load_alert_pixels(DIST_ZARR_URI)

    n_alerts = int((synthetic_zarrs[DIST_ZARR_URI].alert_date > 0).sum())
    assert 0 < n_alerts < SIZE * SIZE
    assert {arr.shape for arr in pixels.values()} == {(n_alerts,)}
    assert set(pixels) == {
        "alert_date",
        "confidence",
        "country",
        "region",
        "subregion",
        "pixel_area",
        *stages.CONTEXTUAL_LAYERS,
    }
