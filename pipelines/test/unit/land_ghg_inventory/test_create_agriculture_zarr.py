from unittest.mock import patch

import dask.array as da
import numpy as np
import pytest
import xarray as xr

from pipelines.globals import land_ghg_inventory_agriculture_zarr_uri
from pipelines.land_ghg_inventory import create_agriculture_zarr as mod
from pipelines.land_ghg_inventory.agriculture_stages import AGRICULTURE_SOURCE_VARS


@pytest.fixture
def reference_veg_dataset():
    """4x4 vegetation grid, 2 years -- only used for its geobox."""
    ref = xr.DataArray(
        da.from_array(np.zeros((2, 4, 4), dtype="float32"), chunks=(1, 4, 4)),
        dims=["year", "y", "x"],
        coords={
            "year": [0, 1],
            "y": [1.0, 0.5, 0.0, -0.5],
            "x": [0.0, 0.5, 1.0, 1.5],
        },
    )
    return xr.Dataset({mod.REFERENCE_GRID_VAR: ref})


def _fake_cropland_cog(value):
    """A coarser source raster (absolute kg total per pixel), matching the
    reference grid 2:1 per axis so each source pixel has exactly 4 children."""
    arr = xr.DataArray(
        da.from_array(np.full((1, 2, 2), value, dtype="float32"), chunks=(1, 2, 2)),
        dims=["band", "y", "x"],
        coords={"band": [1], "y": [1.0, 0.0], "x": [0.0, 1.0]},
    )
    arr.rio.write_crs("EPSG:4326", inplace=True)
    return arr


def _fake_livestock_cog(value):
    """A coarser source raster (kg/ha rate), band dim included like a real GeoTIFF read."""
    arr = xr.DataArray(
        da.from_array(np.full((1, 2, 2), value, dtype="float32"), chunks=(1, 2, 2)),
        dims=["band", "y", "x"],
        coords={"band": [1], "y": [1.0, 0.0], "x": [0.0, 1.0]},
    )
    arr.rio.write_crs("EPSG:4326", inplace=True)
    return arr


@pytest.fixture
def pixel_area_layer():
    """4x4 pixel-area layer (hectares), matching the reference grid 1:1 --
    the shape ``common.align_to`` returns after loading + snapping."""
    return xr.DataArray(
        da.from_array(np.full((4, 4), 5.0, dtype="float64"), chunks=(4, 4)),
        dims=["y", "x"],
        coords={
            "y": [1.0, 0.5, 0.0, -0.5],
            "x": [0.0, 0.5, 1.0, 1.5],
        },
    )


def test_create_agriculture_zarr_writes_expected_shape(
    reference_veg_dataset, pixel_area_layer
):
    captured = {}

    def fake_to_zarr(self, uri, group=None, mode=None):
        captured["ds"] = self
        captured["uri"] = uri
        captured["group"] = group
        captured["mode"] = mode

    with (
        patch.object(mod, "s3_uri_exists", return_value=False),
        patch.object(mod.xr, "open_zarr", return_value=reference_veg_dataset),
        patch.object(
            mod.rio,
            "open_rasterio",
            side_effect=[_fake_cropland_cog(400.0), _fake_livestock_cog(2_000.0)],
        ),
        patch.object(mod, "align_to", return_value=pixel_area_layer) as mock_align_to,
        patch.object(xr.Dataset, "to_zarr", fake_to_zarr),
    ):
        result_uri = mod.create_agriculture_zarr(overwrite=False)

    assert result_uri == land_ghg_inventory_agriculture_zarr_uri
    assert captured["uri"] == land_ghg_inventory_agriculture_zarr_uri
    assert captured["group"] == "pipeline"
    assert captured["mode"] == "w"

    # pixel area is only needed for livestock now (cropland uses the
    # mass-conserving uniform-split path instead of an area multiply).
    mock_align_to.assert_called_once()

    ds = captured["ds"].compute()
    # matches what agriculture_stages.load_agriculture expects: cropland +
    # livestock variables, dims (y, x) with no leftover band dim.
    assert set(ds.data_vars) == set(AGRICULTURE_SOURCE_VARS.values())
    assert ds.sizes.keys() == {"y", "x"}
    assert "band" not in ds.dims

    # resampled onto the reference (vegetation) grid, not the source grid
    assert list(ds.y.values) == [1.0, 0.5, 0.0, -0.5]
    assert list(ds.x.values) == [0.0, 0.5, 1.0, 1.5]

    # cropland: absolute per-pixel total (400 kg), split across however many
    # 30m reference pixels a nearest-neighbor reprojection maps each of the
    # 4 source pixels onto (uneven here due to grid misalignment: child
    # counts of 1, 2, 2, 4 for the 4 source pixels), then kg -> Mg. Summing
    # over each source pixel's own children reproduces that source pixel's
    # 400 kg / 1000 = 0.4 Mg; the grand total is 4 source pixels x 0.4 Mg.
    cropland = ds[AGRICULTURE_SOURCE_VARS["cropland"]].values
    assert cropland[0, 0] == pytest.approx(400.0 / mod.KG_PER_MG)
    assert np.nansum(cropland) == pytest.approx(4 * 400.0 / mod.KG_PER_MG)

    # livestock: kg/ha -> ha-multiplied -> Mg conversion applied:
    # 2_000 kg/ha * 5 ha / 1000 = 10
    livestock = ds[AGRICULTURE_SOURCE_VARS["livestock"]].values
    assert livestock[0, 0] == pytest.approx(10.0)


def test_create_agriculture_zarr_skips_when_present_and_not_overwrite():
    with (
        patch.object(mod, "s3_uri_exists", return_value=True) as mock_exists,
        patch.object(mod, "_reference_geobox") as mock_geobox,
    ):
        result_uri = mod.create_agriculture_zarr(overwrite=False)

    assert result_uri == land_ghg_inventory_agriculture_zarr_uri
    mock_exists.assert_called_once_with(
        f"{land_ghg_inventory_agriculture_zarr_uri}/pipeline/zarr.json"
    )
    mock_geobox.assert_not_called()


def test_create_agriculture_zarr_overwrite_skips_exists_check(
    reference_veg_dataset, pixel_area_layer
):
    with (
        patch.object(mod, "s3_uri_exists") as mock_exists,
        patch.object(mod.xr, "open_zarr", return_value=reference_veg_dataset),
        patch.object(
            mod.rio,
            "open_rasterio",
            side_effect=[_fake_cropland_cog(100.0), _fake_livestock_cog(500.0)],
        ),
        patch.object(mod, "align_to", return_value=pixel_area_layer),
        patch.object(xr.Dataset, "to_zarr"),
    ):
        mod.create_agriculture_zarr(overwrite=True)

    mock_exists.assert_not_called()


def test_resample_total_uniformly_conserves_mass_per_source_pixel(reference_veg_dataset):
    """Each source pixel keeps a distinct value, so that summing the
    reprojected output over just the children of one source pixel (not the
    whole grid) is a meaningful check -- verifies conservation isn't masked
    by every source pixel happening to carry the same value. Grid
    misalignment gives the source pixels uneven child counts (1, 2, 2, 4 out
    of 16 destination pixels; the rest fall outside the source and are 0),
    so an unweighted nearest-neighbor replication (no division) would
    over-count the pixels with more children -- this checks each source
    pixel's own total is reproduced regardless of its child count."""
    distinct_cog = xr.DataArray(
        da.from_array(
            np.array([[100.0, 200.0], [300.0, 400.0]], dtype="float32").reshape(1, 2, 2),
            chunks=(1, 2, 2),
        ),
        dims=["band", "y", "x"],
        coords={"band": [1], "y": [1.0, 0.0], "x": [0.0, 1.0]},
    )
    distinct_cog.rio.write_crs("EPSG:4326", inplace=True)

    with patch.object(mod.xr, "open_zarr", return_value=reference_veg_dataset):
        geobox = mod._reference_geobox()

    with patch.object(mod.rio, "open_rasterio", return_value=distinct_cog):
        result = mod._resample_total_uniformly("fake_uri", geobox).compute()

    # source pixel (0,0)=100 -> destination (0,0) only (1 child)
    assert result.values[0, 0] == pytest.approx(100.0)
    # source pixel (0,1)=200 -> destination (0,1),(0,2) (2 children)
    assert np.nansum(result.values[0, 1:3]) == pytest.approx(200.0)
    # source pixel (1,0)=300 -> destination (1,0),(2,0) (2 children)
    assert np.nansum(result.values[1:3, 0]) == pytest.approx(300.0)
    # source pixel (1,1)=400 -> destination (1,1),(1,2),(2,1),(2,2) (4 children)
    assert np.nansum(result.values[1:3, 1:3]) == pytest.approx(400.0)
    # row/col 3 (y=-0.5, x=1.5) fall outside the source raster entirely
    assert np.all(result.values[3, :] == 0) or np.all(np.isnan(result.values[3, :]))
